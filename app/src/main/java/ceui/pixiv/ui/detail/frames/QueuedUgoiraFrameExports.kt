package ceui.pixiv.ui.detail.frames

import android.content.Context
import android.net.Uri
import androidx.annotation.Keep
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.Downloads
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.copy.*
import ceui.pixiv.download.model.RelativePath
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.bulk.*
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.security.MessageDigest
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

@Keep data class FrameExportRequest(val version: Int = 1, val illustJson: String, val indices: List<Int>,
    val paths: List<String>, val delays: List<Int>, val destinations: List<CopyDestination>,
    val video: CopyDestination? = null, val preferVideo: Boolean = false, val shareRoot: String? = null,
    val offlineDir: String? = null)
@Keep data class FrameInputReceipt(val name: String, val bytes: Long, val sha256: String)
@Keep data class FrameExportReceipt(val uri: String?, val mime: String, val skipped: Boolean = false, val child: String? = null)
@Keep data class FrameExportState(val version: Int = 1, val inputs: List<FrameInputReceipt> = emptyList(),
    val outputs: List<FrameExportReceipt> = emptyList(), val video: Boolean = false, val complete: Boolean = false)

/** Selected original frames and trim clips have durable, frozen intentions of their own. */
class QueuedUgoiraFrameExports internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    private val locals: QueuedLocalOutputs = QueuedLocalOutputs.get(app),
    private val output: CopyOutput = AndroidCopyOutput(app) { DownloadsRegistry.downloads },
    private val reload: suspend (Illust) -> UgoiraFrames = UgoiraEngine::loadOriginalFrames,
    private val videoEncoder: suspend (File, List<File>, List<Int>, (Int) -> Unit) -> File? = { target, files, delays, progress ->
        UgoiraEngine.withVideoEncoder { UgoiraVideoEncoder.encode(target, files, delays, progress) }
    },
    private val gifEncoder: (List<File>, List<Int>, OutputStream, (Int) -> Unit) -> Unit = ::encodeFramesToGif,
) {
    private val gson = Gson()
    private val clip = object : ContentPlanHandler {
        override val kind = CLIP
        override suspend fun execute(context: ContentPlanContext) = executeClip(context)
        override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
            val request = request(plan); val outer = gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java)
            val state = gson.fromJson(outer.domain, FrameExportState::class.java) ?: return false
            val destination = if (state.video) requireNotNull(request.video) else request.destinations.single()
            return publisher(request, destination).reconcile(plan, destination)
        }
        override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) = clean(plan, state)
    }
    private val group = object : ContentPlanHandler {
        override val kind = GROUP
        override val isGroup = true
        override suspend fun execute(context: ContentPlanContext) = executeFrames(context)
        override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
            val state = gson.fromJson(plan.checkpoint, FrameExportState::class.java)
            return state.complete && state.outputs.size == request(plan).indices.size &&
                queue.store.children(plan.id).let { children -> children.isNotEmpty() && children.all { it.state == State.SUCCEEDED } }
        }
        override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
            if (state == State.CANCELLED) queue.store.children(plan.id).forEach { queue.ticket(it.id)?.cancel() }
            clean(plan, state)
        }
    }
    init { queue.register(clip); queue.register(group) }
    private fun root(id: String) = File(app.filesDir, "content_frame_exports/${hash(id)}")
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, FrameExportRequest::class.java).also {
        require(it.version == 1 && it.indices.isNotEmpty() && it.indices.distinct().size == it.indices.size && it.indices.all { i -> i >= 0 })
        require(it.paths.size == it.indices.size && it.delays.size == it.indices.size && it.delays.all { d -> d > 0 })
        require(it.destinations.size == if (plan.kind == GROUP) it.indices.size else 1)
        it.destinations.forEach { d -> d.relativePath(); d.storageChoice(); d.overwrite() }
        it.shareRoot?.let { r -> require(r == app.cacheDir.absolutePath || r == app.externalCacheDir?.absolutePath) }
    }
    internal fun enqueue(request: FrameExportRequest, frames: Boolean): ContentPlanTicket = queue.submit(
        "ugoira-frame-export:${UUID.randomUUID()}", if (frames) GROUP else CLIP) { gson.toJson(request) }
    suspend fun frames(illust: Illust, frames: UgoiraFrames, indices: List<Int>, share: Boolean,
        offline: File?): List<FrameExportReceipt> = await(enqueue(freeze(illust, frames, indices, share, false, false, offline), true))
    suspend fun clip(illust: Illust, frames: UgoiraFrames, range: IntRange, share: Boolean, video: Boolean,
        offline: File?, progress: (Int) -> Unit): FrameExportReceipt {
        val ticket = enqueue(freeze(illust, frames, range.toList(), share, true, video, offline), false)
        return await(ticket) { progress(it.bytes.coerceIn(0, 100).toInt()) }.single()
    }
    private suspend fun await(ticket: ContentPlanTicket, progress: (ContentPlanStatus) -> Unit = {}): List<FrameExportReceipt> {
        val end = ticket.status.first { progress(it); it.state in ContentPlanTicket.OBSERVABLE_END }
        if (end.state in setOf(State.PAUSED, State.CANCELLED)) throw CancellationException("Frame export stopped")
        if (end.state != State.SUCCEEDED) throw IOException(end.error ?: "Frame export failed")
        val plan = queue.store.get(ticket.id)
        if (plan.kind == GROUP) return gson.fromJson(plan.checkpoint, FrameExportState::class.java).outputs
        val outer = gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java)
        val state = gson.fromJson(outer.domain, FrameExportState::class.java)
        val request = request(plan)
        return listOf(FrameExportReceipt(outer.receipt,
            (if (state.video) requireNotNull(request.video) else request.destinations.single()).mime, outer.skipped))
    }
    private fun freeze(illust: Illust, frames: UgoiraFrames, indices: List<Int>, share: Boolean, clip: Boolean,
        preferVideo: Boolean, offline: File?): FrameExportRequest {
        require(indices.isNotEmpty() && indices.all { it in frames.files.indices })
        val config = DownloadsRegistry.store.loadOrFallback(); val rendered = Downloads(config) { error("Render only") }
        val id = UUID.randomUUID().toString()
        fun destination(index: Int, video: Boolean = false): CopyDestination {
            val item = if (clip) DownloadItems.ugoira(illust, video) else DownloadItems.ugoiraFrame(illust)
            val suffix = if (clip) ClipRanges.clipSuffix(indices.first()..indices.last(), frames.files.size)
                else ClipRanges.frameSuffix(index, frames.files.size)
            if (share) return CopyDestination("cache", segments = listOf("images", id, "${illust.id}$suffix.${item.ext}"),
                mime = item.mime, policy = "Replace")
            val resolved = config.resolve(item.bucket)
            return CopyDestination.capture(resolved.storage, rendered.resolveDerivedPath(item, suffix), item.mime, resolved.overwrite)
        }
        return FrameExportRequest(illustJson = gson.toJson(illust), indices = indices.toList(), paths = indices.map { frames.files[it].absolutePath },
            delays = indices.map { frames.delaysMs[it] }, destinations = if (clip) listOf(destination(indices.first())) else indices.map { destination(it) },
            video = if (clip) destination(indices.first(), true) else null, preferVideo = preferVideo,
            shareRoot = if (share) (app.externalCacheDir ?: app.cacheDir).absolutePath else null, offlineDir = offline?.absolutePath)
    }
    private suspend fun capture(context: ContentPlanContext, request: FrameExportRequest, saved: FrameExportState,
        save: suspend (FrameExportState) -> Unit): FrameExportState {
        val directory = File(root(context.plan.id), "frames")
        val valid = saved.inputs.size == request.indices.size && phases.run(context.plan.id, "frame-input-check", "frame-input:${context.plan.id}", Pool.COPY) {
            saved.inputs.all { receipt ->
                require(receipt.name.matches(Regex("[0-9]+[.]jpg")) && receipt.bytes > 0)
                val file = File(directory, receipt.name)
                file.isFile && file.length() == receipt.bytes && ArtifactPublisher.digest(file) == receipt.sha256
            }
        }
        if (valid) return saved
        var files = request.paths.map(::File)
        if (files.any { !it.isFile || it.length() <= 0 }) {
            if (request.offlineDir != null) throw IOException("离线快照原帧已不可用")
            val refreshed = reload(gson.fromJson(request.illustJson, Illust::class.java))
            if (request.indices.any { it !in refreshed.files.indices }) throw IOException("原帧数量已改变")
            files = request.indices.map { refreshed.files[it] }
        }
        // Never wait for the cache lease while owning COPY or an encoder.
        val receipts = UgoiraCacheAccess.read {
            phases.run(context.plan.id, "frame-input-capture", "frame-input:${context.plan.id}", Pool.COPY) {
                directory.mkdirs()
                files.mapIndexed { i, file ->
                    context.checkStop(); if (!file.isFile || file.length() <= 0) throw IOException("Original frame unavailable")
                    val target = File(directory, "$i.jpg"); val temp = File(directory, "$i.copying")
                    try {
                        FileOutputStream(temp).use { out -> file.inputStream().use { it.copyTo(out) }; out.fd.sync() }
                        context.checkStop(); if (!temp.renameTo(target)) throw IOException("Cannot preserve original frame")
                    } finally { temp.delete() }
                    FrameInputReceipt(target.name, target.length(), ArtifactPublisher.digest(target))
                }
            }
        }
        return saved.copy(inputs = receipts).also { save(it) }
    }
    private fun publisher(request: FrameExportRequest, destination: CopyDestination): ArtifactPublisher {
        val base = request.shareRoot ?: return ArtifactPublisher(phases, output)
        val file = destination.segments.fold(File(base)) { parent, segment -> File(parent, segment) }
        require(file.canonicalPath.startsWith(File(base, "images").canonicalPath + File.separator))
        return ArtifactPublisher(phases, DocumentCopyOutput(app, Uri.fromFile(file).toString(), null, file.name, destination.mime))
    }
    private suspend fun shareDirectory(context: ContentPlanContext, request: FrameExportRequest, destination: CopyDestination) {
        request.shareRoot?.let { base -> phases.run(context.plan.id, "frame-share-directory", "frame-input:${context.plan.id}", Pool.COPY) {
            destination.segments.dropLast(1).fold(File(base)) { parent, segment -> File(parent, segment) }.mkdirs()
        } }
    }
    private suspend fun executeFrames(context: ContentPlanContext) {
        val request = request(context.plan)
        var saved = gson.fromJson(context.checkpoint, FrameExportState::class.java)
        suspend fun save(value: FrameExportState) { saved = value; context.checkpoint(gson.toJson(value)) }
        saved = capture(context, request, saved, ::save)
        for (i in saved.outputs.size until request.indices.size) {
            val destination = request.destinations[i]; shareDirectory(context, request, destination)
            val family = "${context.plan.id}:output:$i"; queue.store.ownChild(context.plan.id, family)
            val source = File(root(context.plan.id), "frames/${saved.inputs[i].name}")
            // Share outputs are kept at an explicit, unique FileProvider destination.
            if (request.shareRoot != null) {
                val file = destination.segments.fold(File(request.shareRoot)) { p, s -> File(p, s) }
                val childSpec = FrameExportRequest(illustJson = request.illustJson, indices = listOf(request.indices[i]),
                    paths = listOf(source.absolutePath), delays = listOf(request.delays[i]), destinations = listOf(destination),
                    shareRoot = request.shareRoot, offlineDir = source.parent)
                // A leaf copy owner is used rather than recursively submitting another group.
                val child = queue.submit(family, CLIP, reuseSucceeded = true) { gson.toJson(childSpec) }
                awaitChild(context, child)
                val outer = gson.fromJson(queue.store.get(child.id).checkpoint, ArtifactCheckpoint::class.java)
                save(saved.copy(outputs = saved.outputs + FrameExportReceipt(outer.receipt, destination.mime, outer.skipped, child.id)))
            } else {
                val child = locals.enqueue(family, source = source, reuseSucceeded = true) {
                    LocalOutputRequest(type = QueuedLocalOutputs.FILE, subjectId = 0, sourcePath = source.absolutePath,
                        sourceBytes = source.length(), sourceModified = source.lastModified(), destination = destination)
                }
                awaitChild(context, child)
                val outer = gson.fromJson(queue.store.get(child.id).checkpoint, ArtifactCheckpoint::class.java)
                save(saved.copy(outputs = saved.outputs + FrameExportReceipt(outer.receipt, destination.mime, outer.skipped, child.id)))
            }
            context.progress("frame-output", saved.outputs.size.toLong(), request.indices.size.toLong())
        }
        save(saved.copy(complete = true)); context.commit {}
    }
    private suspend fun awaitChild(context: ContentPlanContext, ticket: ContentPlanTicket) {
        try {
            val result = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
            if (result.state == State.SUCCEEDED) return
            if (result.state == State.PAUSED) { queue.ticket(context.plan.id)!!.pause(); throw CancellationException("Frame output paused") }
            if (result.state == State.CANCELLED) { queue.ticket(context.plan.id)!!.cancel(); throw CancellationException("Frame output cancelled") }
            throw IOException(result.error ?: "Frame output failed")
        } catch (cancel: CancellationException) {
            withContext(NonCancellable) {
                if (queue.ticket(context.plan.id)?.status?.value?.state == State.CANCEL_REQUESTED) ticket.cancel() else ticket.pause()
                ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
            }
            throw cancel
        }
    }
    private suspend fun executeClip(context: ContentPlanContext) {
        val request = request(context.plan)
        var outer = runCatching { gson.fromJson(context.checkpoint, ArtifactCheckpoint::class.java) }.getOrNull() ?: ArtifactCheckpoint()
        var saved = runCatching { gson.fromJson(outer.domain, FrameExportState::class.java) }.getOrNull() ?: FrameExportState()
        suspend fun save(value: FrameExportState) { saved = value; outer = outer.copy(domain = gson.toJson(value)); context.checkpoint(gson.toJson(outer)) }
        val artifact = File(root(context.plan.id), "artifact")
        val ready = phases.run(context.plan.id, "clip-check", "frame-input:${context.plan.id}", Pool.COPY) {
            artifact.isFile && outer.bytes > 0 && artifact.length() == outer.bytes && ArtifactPublisher.digest(artifact) == outer.sha256
        }
        if (!ready) {
            if (outer.receipt != null) throw IOException("Clip input changed after publication")
            saved = capture(context, request, saved, ::save)
            val files = saved.inputs.map { File(root(context.plan.id), "frames/${it.name}") }
            var video = false
            // A one-frame share child is a byte copy, preserving the exact JPEG frame.
            if (request.video == null) phases.run(context.plan.id, "frame-share-input", "frame-input:${context.plan.id}", Pool.COPY) {
                files.single().copyTo(artifact, overwrite = true)
            } else {
                val progress: (Int) -> Unit = { context.progress("clip-encode", it.toLong(), 100) }
                if (request.preferVideo) {
                    val encoded = videoEncoder(File(root(context.plan.id), "clip.mp4"), files, request.delays, progress)
                    if (encoded != null) {
                        phases.run(context.plan.id, "clip-video-input", "frame-input:${context.plan.id}", Pool.COPY) { encoded.copyTo(artifact, overwrite = true) }
                        video = true
                    }
                }
                if (!video) phases.run(context.plan.id, "clip-gif", "frame-input:${context.plan.id}", Pool.ENCODE) {
                    val temp = File(root(context.plan.id), "clip.building")
                    try {
                        val coroutine = currentCoroutineContext()
                        FileOutputStream(temp).use { out -> gifEncoder(files, request.delays, out) { coroutine.ensureActive(); context.checkStop(); progress(it) }; out.fd.sync() }
                        context.checkStop(); if (!temp.renameTo(artifact)) throw IOException("Cannot preserve encoded clip")
                    } finally { temp.delete() }
                }
            }
            save(saved.copy(video = video))
        }
        val destination = if (saved.video) requireNotNull(request.video) else request.destinations.single()
        shareDirectory(context, request, destination)
        // The final output receipt is checkpointed before the group/clip receives completion.
        publisher(request, destination).publish(context, artifact, destination)
    }
    private suspend fun clean(plan: ContentPlanStore.Plan, state: State) {
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        phases.run(plan.id, "frame-export-cleanup", "frame-input:${plan.id}", Pool.COPY) {
            val directory = root(plan.id)
            if (directory.exists() && !directory.deleteRecursively()) throw IOException("Cannot remove owned frame inputs")
        }
    }
    companion object {
        private const val GROUP = "ugoira-frames-group"; private const val CLIP = "ugoira-clip"
        private fun hash(id: String) = MessageDigest.getInstance("SHA-256").digest(id.toByteArray()).joinToString("") { "%02x".format(it) }
        @Volatile private var instance: Pair<Context, QueuedUgoiraFrameExports>? = null
        @JvmStatic fun get(context: Context): QueuedUgoiraFrameExports {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedUgoiraFrameExports(app).also { instance = app to it }
            }
        }
    }
}
