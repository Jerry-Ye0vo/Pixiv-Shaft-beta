package ceui.pixiv.ui.novel.reader.settings

import android.content.Context
import androidx.annotation.Keep
import ceui.lisa.http.GithubProxy
import ceui.pixiv.download.network.ResumableFileTransfer
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.novel.reader.paginate.TypefaceProvider
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import okhttp3.OkHttpClient

@Keep data class ReaderFontRequest(val version: Int = 1, val fontId: String, val url: String,
    val bytes: Long, val sha256: String, val delete: Boolean = false)
@Keep data class ReaderFontCheckpoint(val version: Int = 1, val prepared: Boolean = false)

/** Same noBackupFilesDir font identity and checksums; a durable owner replaces panel-owned jobs. */
class QueuedReaderFonts internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    private val allowed: () -> Boolean = { ceui.lisa.utils.DownloadLimitTypeUtil.autoStartAllowed() },
    private val client: OkHttpClient = OkHttpClient.Builder().followRedirects(true)
        .connectTimeout(30, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS).build(),
    private val changed: (ReaderWebFont, Boolean) -> Unit = { font, installed ->
        TypefaceProvider.evict(font.id); if (installed) ReaderSettings.onFontInstalled(font.id)
    },
) : ContentPlanHandler {
    override val kind = "reader-font"
    private val gson = Gson()
    private val manual = ConcurrentHashMap.newKeySet<String>()
    init { queue.register(this) }
    fun enqueue(font: ReaderWebFont, delete: Boolean = false): ContentPlanTicket = enqueue(ReaderFontRequest(
        fontId = font.id, url = GithubProxy.wrap(font.downloadUrl), bytes = font.byteSize, sha256 = font.checksumSha256, delete = delete))
    internal fun enqueue(request: ReaderFontRequest): ContentPlanTicket {
        val family = "reader-font:${request.fontId}"; manual.add(family)
        return queue.submit(family, kind, replaceActive = request.delete) { gson.toJson(request) }
    }
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, ReaderFontRequest::class.java).also {
        require(it.version == 1 && ReaderWebFont.byId(it.fontId) != null && it.bytes in 1..(64L * 1024 * 1024))
        require(it.sha256.matches(Regex("[0-9a-f]{64}")) && (it.url.startsWith("https://") || it.url.startsWith("http://")))
    }
    private fun directory(id: String) = File(app.noBackupFilesDir, "content_reader_fonts/" +
        java.security.MessageDigest.getInstance("SHA-256").digest(id.toByteArray()).joinToString("") { "%02x".format(it) })
    override fun onUserResume(plan: ContentPlanStore.Plan) { manual.add(plan.family) }
    override fun onNetworkPolicyRestricted() { manual.clear() }
    override fun canRun(plan: ContentPlanStore.Plan): Boolean = manual.contains(plan.family) || allowed() ||
        runCatching { request(plan).delete || gson.fromJson(plan.checkpoint, ReaderFontCheckpoint::class.java).prepared }.getOrDefault(true)
    private suspend fun matches(file: File, request: ReaderFontRequest) = file.isFile && file.length() == request.bytes &&
        ArtifactPublisher.digest(file) == request.sha256
    override suspend fun execute(context: ContentPlanContext) {
        val request = request(context.plan); val font = requireNotNull(ReaderWebFont.byId(request.fontId))
        val target = font.file(app); val input = File(directory(context.plan.id), "font")
        if (request.delete) {
            context.checkpoint(gson.toJson(ReaderFontCheckpoint(prepared = true)))
            phases.run(context.plan.id, "font-delete", "reader-font:${font.id}", Pool.COPY) {
                context.commit {
                    if (target.exists() && !target.delete()) throw IOException("Cannot delete reader font")
                    changed(font, false)
                }
            }
            return
        }
        val installed = phases.run(context.plan.id, "font-check", "reader-font:${font.id}", Pool.COPY) { matches(target, request) }
        if (installed) { context.commit { changed(font, true) }; return }
        val ready = phases.run(context.plan.id, "font-input-check", "reader-font:${font.id}", Pool.COPY) { matches(input, request) }
        if (!ready) {
            if (!manual.contains(context.plan.family) && !allowed()) context.yieldForPolicy()
            context.progress("font-download")
            phases.run(context.plan.id, "font-download", "font-source:${request.url}", Pool.NETWORK) {
                ResumableFileTransfer.transfer(request.url, input, client, maximumBytes = request.bytes,
                    onProgress = { bytes, _ -> context.progress("font-download", bytes, request.bytes) })
            }
        }
        phases.run(context.plan.id, "font-install", "reader-font:${font.id}", Pool.COPY) {
            if (!matches(input, request)) throw IOException("Reader font checksum mismatch")
            context.checkpoint(gson.toJson(ReaderFontCheckpoint(prepared = true)))
            target.parentFile!!.mkdirs()
            val staged = File(target.parentFile, ".${font.id}.${context.plan.id.substringAfterLast(':')}.install")
            try {
                FileOutputStream(staged).use { out -> input.inputStream().use { it.copyTo(out) }; out.fd.sync() }
                context.commit {
                    if (!staged.renameTo(target)) throw IOException("Cannot install verified reader font")
                    changed(font, true)
                }
            } finally { staged.delete() }
        }
    }
    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val request = request(plan); val font = requireNotNull(ReaderWebFont.byId(request.fontId))
        val prepared = runCatching { gson.fromJson(plan.checkpoint, ReaderFontCheckpoint::class.java).prepared }.getOrDefault(false)
        if (!prepared) return false
        return phases.run(plan.id, "font-reconcile", "reader-font:${font.id}", Pool.COPY) {
            val complete = if (request.delete) !font.file(app).exists() else matches(font.file(app), request)
            if (complete) changed(font, !request.delete)
            complete
        }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        phases.run(plan.id, "font-cleanup", "reader-font-input:${plan.id}", Pool.COPY) {
            val dir = directory(plan.id)
            if (dir.exists() && !dir.deleteRecursively()) throw IOException("Cannot remove font transfer inputs")
        }
    }
    companion object {
        @Volatile private var instance: Pair<Context, QueuedReaderFonts>? = null
        @JvmStatic fun get(context: Context): QueuedReaderFonts {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedReaderFonts(app).also { instance = app to it }
            }
        }
        @JvmStatic fun refreshEligibility() { instance?.second?.queue?.refreshEligibility() }
        @JvmStatic fun onNetworkGateClosed() { instance?.second?.let { it.manual.clear(); it.queue.refreshEligibility() } }
    }
}
