package ceui.pixiv.download.copy

import android.content.Context
import android.graphics.Bitmap
import android.net.Uri
import androidx.annotation.Keep
import ceui.lisa.download.IllustDownload
import ceui.lisa.utils.Params
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.model.Bucket
import ceui.pixiv.download.model.RelativePath
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.imageloader.*
import ceui.pixiv.ui.filter.*
import ceui.pixiv.ui.share.ArtworkPosterExporter
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

@Keep data class ImageDerivativeRequest(val version: Int = 1, val type: String, val illustJson: String,
    val page: Int, val sourceUrl: String, val sourcePath: String? = null, val preset: String = "original",
    val intensity: Int = 100, val adjust: List<Int> = emptyList(), val jpg: CopyDestination,
    val png: CopyDestination, val shareRoot: String? = null)
@Keep data class ImageDerivativeState(val version: Int = 1, val sourceHash: String = "", val sourceBytes: Long = 0,
    val avatarHash: String? = null, val avatarAttempted: Boolean = false, val mime: String? = null)
data class ImageDerivativeResult(val uri: Uri, val mime: String, val skipped: Boolean)

/** Existing poster rendering reads only these owned inputs while it holds ENCODE. */
internal class DerivativeRenderSession(val source: File, val avatar: File?, val target: File) {
    fun capture(bitmap: Bitmap, format: Bitmap.CompressFormat, quality: Int): Uri {
        FileOutputStream(target).use { output ->
            if (!bitmap.compress(format, quality, output)) throw IOException("Image encode failed")
            output.fd.sync()
        }
        return Uri.fromFile(target)
    }
    companion object { val current = ThreadLocal<DerivativeRenderSession?>() }
}

/** Durable recipes for user-triggered rendering. Ordinary editing previews remain interactive. */
class QueuedImageDerivatives internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    private val output: CopyOutput = AndroidCopyOutput(app) { DownloadsRegistry.downloads },
    private val local: suspend (ImageDerivativeRequest, Illust) -> File? = { request, illust ->
        request.sourcePath?.let(::File)?.takeIf { it.isFile && it.length() > 0 }
            ?: PageImageSourceResolver.resolve(app, illust, request.page, request.sourceUrl, allowNetwork = false).awaitFile(app)
    },
    private val render: suspend (ImageDerivativeRequest, Illust, File, File?, File) -> String = { request, illust, source, avatar, target ->
        if (request.type == POSTER) {
            withContext(DerivativeRenderSession.current.asContextElement(DerivativeRenderSession(source, avatar, target))) {
                ArtworkPosterExporter.exportRaw(app, illust, request.page)
            }
            "image/jpeg"
        } else {
            val decoded = ceui.pixiv.ui.upscale.decodeBoundedArgb(source, "filter", mutable = true) ?: throw IOException("Image decode failed")
            val bitmap = if (decoded.config == Bitmap.Config.ARGB_8888) decoded else
                decoded.copy(Bitmap.Config.ARGB_8888, true).also { decoded.recycle() } ?: throw IOException("ARGB conversion failed")
            try {
                val state = FilterState(FilterPreset.fromKey(request.preset), request.intensity, request.adjust)
                NativeImageFilter.render(bitmap, bitmap, state)
                val png = bitmap.hasAlpha()
                DerivativeRenderSession(source, null, target).capture(bitmap,
                    if (png) Bitmap.CompressFormat.PNG else Bitmap.CompressFormat.JPEG, 95)
                if (png) "image/png" else "image/jpeg"
            } finally { bitmap.recycle() }
        }
    },
) : ContentPlanHandler {
    override val kind = "image-derivative"
    private val gson = Gson()
    private val publisher = ArtifactPublisher(phases, output)
    init { queue.register(this) }
    private fun directory(id: String) = File(app.filesDir, "content_image_derivatives/${hash(id)}")
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, ImageDerivativeRequest::class.java).also {
        require(it.version == 1 && it.type in TYPES && it.page >= 0 && it.sourceUrl.isNotBlank())
        it.jpg.storageChoice(); it.png.storageChoice()
        if (it.type.startsWith("filter")) require(it.adjust.size == FilterAdjust.entries.size && it.intensity in 0..100)
        it.shareRoot?.let { root -> require(root == app.cacheDir.absolutePath || root == app.externalCacheDir?.absolutePath) }
    }
    fun enqueue(spec: ImageDerivativeRequest): ContentPlanTicket = queue.submit("image-derivative:${UUID.randomUUID()}", kind) { gson.toJson(spec) }
    suspend fun await(ticket: ContentPlanTicket): ImageDerivativeResult {
        val end = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
        if (end.state in setOf(State.PAUSED, State.CANCELLED)) throw CancellationException("Image export stopped")
        if (end.state != State.SUCCEEDED) throw IOException(end.error ?: "Image export failed")
        val outer = gson.fromJson(queue.store.get(ticket.id).checkpoint, ArtifactCheckpoint::class.java)
        val state = gson.fromJson(outer.domain, ImageDerivativeState::class.java)
        return ImageDerivativeResult(outer.receipt?.let(Uri::parse) ?: Uri.EMPTY, state.mime ?: "image/jpeg", outer.skipped)
    }
    suspend fun poster(illust: Illust, page: Int): ImageDerivativeResult = await(enqueue(freeze(illust, page, POSTER)))
    suspend fun filter(illust: Illust, page: Int, source: File, state: FilterState, share: Boolean): ImageDerivativeResult =
        await(enqueue(freeze(illust, page, if (share) FILTER_SHARE else FILTER_SAVE, source, state)))
    suspend fun wallpaper(illust: Illust, page: Int): ImageDerivativeResult = await(enqueue(freeze(illust, page, WALLPAPER)))
    suspend fun shareOriginal(illust: Illust, page: Int): ImageDerivativeResult = await(enqueue(freeze(illust, page, ORIGINAL_SHARE)))
    private fun freeze(illust: Illust, page: Int, type: String, source: File? = null, state: FilterState? = null): ImageDerivativeRequest {
        val safePage = page.coerceIn(0, (illust.page_count - 1).coerceAtLeast(0))
        val url = IllustDownload.getUrl(illust, safePage,
            if (type == POSTER && illust.isGif()) Params.IMAGE_RESOLUTION_LARGE else Params.IMAGE_RESOLUTION_ORIGINAL)
            ?: throw IOException("Missing source image")
        val share = type in setOf(FILTER_SHARE, WALLPAPER, ORIGINAL_SHARE)
        val shareId = UUID.randomUUID().toString()
        val config = DownloadsRegistry.store.loadOrFallback()
        fun destination(png: Boolean): CopyDestination {
            val ext = if (type in setOf(WALLPAPER, ORIGINAL_SHARE)) url.substringBefore('?').substringAfterLast('.', "jpg").lowercase()
                .takeIf { it in setOf("jpg", "jpeg", "png", "webp", "gif") } ?: "jpg" else if (png) "png" else "jpg"
            val mime = if (ext == "jpg" || ext == "jpeg") "image/jpeg" else "image/$ext"
            if (share) return CopyDestination("cache", segments = listOf("images", shareId,
                "${illust.id}_p${safePage}_${state?.fileTag() ?: "wallpaper"}.$ext"), mime = mime, policy = "Replace")
            val item = if (type == POSTER) DownloadItems.illustPoster(illust, safePage) else DownloadItems.illustFiltered(illust, safePage, png)
            val resolved = config.resolve(Bucket.Illust)
            val path = ceui.pixiv.download.Downloads(config) { error("Render only") }.resolveDerivedPath(item,
                if (type == POSTER) "_share" else "_filter_${state!!.fileTag()}")
            return CopyDestination.capture(resolved.storage, path, mime, resolved.overwrite)
        }
        return ImageDerivativeRequest(type = type, illustJson = gson.toJson(illust), page = safePage, sourceUrl = url,
            sourcePath = source?.absolutePath, preset = state?.preset?.key ?: "original", intensity = state?.intensity ?: 100,
            adjust = state?.adjust?.toList() ?: emptyList(), jpg = destination(false), png = destination(true),
            shareRoot = if (share) (app.externalCacheDir ?: app.cacheDir).absolutePath else null)
    }
    private fun state(outer: ArtifactCheckpoint) = runCatching { gson.fromJson(outer.domain, ImageDerivativeState::class.java) }
        .getOrNull()?.takeIf { it.version == 1 } ?: ImageDerivativeState()
    private fun destination(spec: ImageDerivativeRequest, state: ImageDerivativeState) = if (state.mime == "image/png") spec.png else spec.jpg
    private fun publisher(spec: ImageDerivativeRequest, destination: CopyDestination): ArtifactPublisher {
        val base = spec.shareRoot ?: return publisher
        val file = destination.segments.fold(File(base)) { parent, segment -> File(parent, segment) }
        require(file.canonicalPath.startsWith(File(base, "images").canonicalPath + File.separator))
        return ArtifactPublisher(phases, DocumentCopyOutput(app, Uri.fromFile(file).toString(), null, file.name, destination.mime))
    }
    override suspend fun execute(context: ContentPlanContext) {
        val spec = request(context.plan); val illust = gson.fromJson(spec.illustJson, Illust::class.java)
        val dir = directory(context.plan.id); val source = File(dir, "source"); val artifact = File(dir, "artifact")
        var outer = publisher.checkpoint(context); var state = state(outer)
        suspend fun save(value: ImageDerivativeState) {
            state = value; outer = publisher.checkpoint(context).copy(domain = gson.toJson(value)); context.checkpoint(gson.toJson(outer))
        }
        val ready = phases.run(context.plan.id, "derivative-check", "derivative:${context.plan.id}", Pool.COPY) {
            artifact.isFile && outer.bytes > 0 && artifact.length() == outer.bytes && ArtifactPublisher.digest(artifact) == outer.sha256
        }
        if (!ready) {
            if (outer.receipt != null) throw IOException("Generated image changed after publication")
            val sourceReady = phases.run(context.plan.id, "derivative-source-check", "derivative:${context.plan.id}", Pool.COPY) {
                source.isFile && source.length() == state.sourceBytes && state.sourceBytes > 0 && ArtifactPublisher.digest(source) == state.sourceHash
            }
            suspend fun capture(file: File) = phases.run(context.plan.id, "derivative-source", "derivative:${context.plan.id}", Pool.COPY) {
                if (!file.isFile || file.length() <= 0) throw IOException("Missing image input")
                dir.mkdirs(); val tmp = File(dir, "source.copying")
                try {
                    FileOutputStream(tmp).use { to -> file.inputStream().use { it.copyTo(to) }; to.fd.sync() }
                    context.checkStop(); if (!tmp.renameTo(source)) throw IOException("Cannot preserve image input")
                } finally { tmp.delete() }
                save(state.copy(sourceHash = ArtifactPublisher.digest(source), sourceBytes = source.length()))
            }
            if (!sourceReady) {
                val file = local(spec, illust)
                if (file != null) CopyInputReferences.reserve(file).use { capture(file) }
                else ResumableOkHttpFetcher.acquire(spec.sourceUrl, {}, saving = true).use { capture(it.file) }
            }
            val avatar = File(dir, "avatar")
            if (spec.type == POSTER && !state.avatarAttempted) {
                val url = illust.user?.profile_image_urls?.findMaxSizeUrl()
                if (!url.isNullOrBlank()) try {
                    ResumableOkHttpFetcher.acquire(url, {}, saving = true).use { lease ->
                        phases.run(context.plan.id, "poster-avatar", "derivative:${context.plan.id}", Pool.COPY) {
                            FileOutputStream(avatar).use { out -> lease.file.inputStream().use { it.copyTo(out) }; out.fd.sync() }
                        }
                    }
                    save(state.copy(avatarHash = ArtifactPublisher.digest(avatar), avatarAttempted = true))
                } catch (cancel: CancellationException) { throw cancel }
                catch (_: Exception) { save(state.copy(avatarAttempted = true)) }
                else save(state.copy(avatarAttempted = true))
            }
            if (spec.type in setOf(WALLPAPER, ORIGINAL_SHARE)) {
                phases.run(context.plan.id, "wallpaper-input", "derivative:${context.plan.id}", Pool.COPY) {
                    source.copyTo(artifact, overwrite = true)
                }
                save(state.copy(mime = spec.jpg.mime))
            } else {
                context.progress("image-render")
                val mime = phases.run(context.plan.id, "image-render", "derivative:${context.plan.id}", Pool.ENCODE) {
                    val temp = File(dir, "artifact.building")
                    try {
                        val mime = render(spec, illust, source, avatar.takeIf { it.isFile && state.avatarHash != null }, temp)
                        currentCoroutineContext().ensureActive(); context.checkStop()
                        if (!temp.isFile || temp.length() == 0L || !temp.renameTo(artifact)) throw IOException("Image encode produced no committed file")
                        mime
                    } finally { temp.delete() }
                }
                save(state.copy(mime = mime))
            }
        }
        val destination = destination(spec, state)
        spec.shareRoot?.let { root -> phases.run(context.plan.id, "share-directory", "derivative:${context.plan.id}", Pool.COPY) {
            destination.segments.dropLast(1).fold(File(root)) { p, s -> File(p, s) }.mkdirs()
        } }
        publisher(spec, destination).publish(context, artifact, destination)
    }
    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val spec = request(plan); val state = state(gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java))
        val destination = destination(spec, state)
        return publisher(spec, destination).reconcile(plan, destination)
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        phases.run(plan.id, "derivative-cleanup", "derivative:${plan.id}", Pool.COPY) {
            val dir = directory(plan.id)
            if (dir.exists() && !dir.deleteRecursively()) throw IOException("Cannot remove owned derivative input")
        }
    }
    companion object {
        const val POSTER = "poster"; const val FILTER_SAVE = "filter-save"; const val FILTER_SHARE = "filter-share"; const val WALLPAPER = "wallpaper"; const val ORIGINAL_SHARE = "original-share"
        private val TYPES = setOf(POSTER, FILTER_SAVE, FILTER_SHARE, WALLPAPER, ORIGINAL_SHARE)
        private fun hash(value: String) = MessageDigest.getInstance("SHA-256").digest(value.toByteArray()).joinToString("") { "%02x".format(it) }
        @Volatile private var instance: Pair<Context, QueuedImageDerivatives>? = null
        @JvmStatic fun get(context: Context): QueuedImageDerivatives {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedImageDerivatives(app).also { instance = app to it }
            }
        }
    }
}
