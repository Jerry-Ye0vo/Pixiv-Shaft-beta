package ceui.pixiv.ui.novel.reader.export

import android.content.Context
import android.net.Uri
import ceui.lisa.activities.Shaft
import ceui.lisa.fragments.WebNovelParser
import ceui.loxia.Novel
import ceui.pixiv.api.Client
import ceui.pixiv.api.model.WebNovel
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.copy.AndroidCopyOutput
import ceui.pixiv.download.copy.CopyDestination
import ceui.pixiv.download.header.HeaderConfigRepo
import ceui.pixiv.download.header.NovelHeaderRenderer
import ceui.pixiv.download.model.Bucket
import ceui.pixiv.download.model.RelativePath
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.novel.reader.model.ContentToken
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.FilterOutputStream
import java.io.OutputStream
import java.io.IOException
import java.security.MessageDigest
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

/** A render session redirects existing format writers to a parent-owned immutable artifact. */
internal class NovelOutputSession(val context: ContentPlanContext, val source: File, val header: String?,
    private val phases: ContentPhaseRunner, val heldEncoding: Boolean) {
    suspend fun capture(mime: String, writer: (OutputStream) -> Unit): Uri {
        suspend fun write(): Uri {
            val temporary = File(source.parentFile, "artifact.building")
            source.parentFile?.mkdirs()
            try {
                FileOutputStream(temporary).use { output ->
                    writer(object : FilterOutputStream(output) { override fun close() { flush() } })
                    output.fd.sync()
                }
                currentCoroutineContext().ensureActive(); context.checkStop()
                if (!temporary.renameTo(source)) throw IOException("Cannot commit generated novel input")
            } finally { temporary.delete() }
            return Uri.fromFile(source)
        }
        return if (heldEncoding) write() else phases.run(context.plan.id, "novel-render", "novel-input:${context.plan.id}",
            if (mime == "application/epub+zip" || mime == "application/pdf") Pool.ENCODE else Pool.COPY) { write() }
    }
    companion object { val current = ThreadLocal<NovelOutputSession?>() }
}

/** Every novel output is durable before byte work; current format writers and old history keys stay. */
class QueuedNovelExports internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    output: ceui.pixiv.download.copy.CopyOutput = AndroidCopyOutput(app) { DownloadsRegistry.downloads },
    private val record: (Novel, Uri) -> Unit = NovelExportManager::recordDownload,
) : ContentPlanHandler {
    override val kind = "novel-export"
    private val gson = Gson()
    private val publisher = ArtifactPublisher(phases, output)
    init { queue.register(this) }
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, NovelExportRequest::class.java).also { it.validate() }
    private fun source(plan: ContentPlanStore.Plan): File {
        val hash = MessageDigest.getInstance("SHA-256").digest(plan.id.toByteArray()).joinToString("") { "%02x".format(it) }
        return File(app.filesDir, "content_novel_inputs/$hash/artifact")
    }
    private fun freeze(bucket: Bucket, path: RelativePath, format: ExportFormat): CopyDestination {
        val resolved = DownloadsRegistry.store.loadOrFallback().resolve(bucket)
        return CopyDestination.capture(resolved.storage, path, format.mimeType, resolved.overwrite)
    }
    suspend fun single(format: ExportFormat, novel: Novel?, webNovel: WebNovel?, tokens: List<ContentToken>?,
        destination: RelativePath, retry: EpubImageRetryPolicy, legacy: Boolean = false): ExportResult {
        val id = novel?.id ?: webNovel?.id?.toLongOrNull() ?: 0L
        val family = "novel-export:$id:${format.name}:${destination.joinTo()}"
        val ticket = queue.submit(family, kind) {
            gson.toJson(NovelExportRequest(format = format.name, novelJson = novel?.let(gson::toJson), novelId = id,
                webJson = webNovel?.let(gson::toJson), tokens = tokens?.map(ExportToken::capture), merged = null,
                destination = freeze(Bucket.Novel, destination, format), header = novel?.let {
                    NovelHeaderRenderer.render(it, HeaderConfigRepo.activePreset(), it.series != null)
                }, legacyTxt = legacy, retryMode = mode(retry)))
        }
        return await(ticket, destination, format)
    }
    suspend fun merged(format: ExportFormat, content: MergedNovelContent, destination: RelativePath,
        retry: EpubImageRetryPolicy, image: suspend (String, Int) -> Unit): Boolean {
        val ticket = queue.submit("novel-merge:${content.documentId}:${format.name}:${destination.joinTo()}", kind) {
            gson.toJson(NovelExportRequest(format = format.name, novelJson = null, novelId = 0, webJson = null,
                tokens = null, merged = ExportMerged.capture(content, gson), destination = freeze(Bucket.NovelSeries, destination, format),
                header = null, retryMode = mode(retry)))
        }
        val result = await(ticket, destination, format) { phase ->
            if (phase.startsWith("novel-image:")) phase.removePrefix("novel-image:").split(':', limit = 2).let {
                if (it.size == 2) image(it[0], it[1].toIntOrNull() ?: 0)
            }
        }
        if (result is ExportResult.Failure) throw IOException(result.message, result.cause)
        return result is ExportResult.Success
    }
    /** Stable group ownership prevents re-export after a child committed before its group acknowledged it. */
    internal fun enqueueMerged(format: ExportFormat, content: MergedNovelContent, destination: CopyDestination,
        family: String, reuseSucceeded: Boolean): ContentPlanTicket = queue.submit(family, kind,
            reuseSucceeded = reuseSucceeded) {
        gson.toJson(NovelExportRequest(format = format.name, novelJson = null, novelId = 0, webJson = null,
            tokens = null, merged = ExportMerged.capture(content, gson), destination = destination,
            header = null, retryMode = "batch"))
    }
    private suspend fun await(ticket: ContentPlanTicket, path: RelativePath, format: ExportFormat,
        progress: suspend (String) -> Unit = {}): ExportResult {
        val state = ticket.status.first { progress(it.phase); it.state in ContentPlanTicket.OBSERVABLE_END }
        if (state.state == State.PAUSED || state.state == State.CANCELLED) throw CancellationException("Novel export stopped")
        if (state.state != State.SUCCEEDED) return ExportResult.Failure(state.error ?: "小说导出失败")
        val checkpoint = gson.fromJson(requireNotNull(queue.store.get(ticket.id)).checkpoint, ArtifactCheckpoint::class.java)
        return if (checkpoint.skipped) ExportResult.Skipped(path.joinTo())
            else ExportResult.Success(Uri.parse(requireNotNull(checkpoint.receipt)), path.joinTo(), format)
    }
    override suspend fun execute(context: ContentPlanContext) {
        val spec = request(context.plan)
        val source = source(context.plan)
        val saved = publisher.checkpoint(context)
        val ready = phases.run(context.plan.id, "novel-input-check", "novel-input:${context.plan.id}", Pool.COPY) {
            source.isFile && source.length() > 0 && (saved.bytes == 0L ||
                (saved.bytes == source.length() && saved.sha256 == ArtifactPublisher.digest(source)))
        }
        val novel = spec.novelJson?.let { gson.fromJson(it, Novel::class.java) }
        if (!ready) {
            if (saved.receipt != null) throw IOException("Generated input no longer matches its output receipt")
            val format = ExportFormat.valueOf(spec.format)
            val session = NovelOutputSession(context, source, spec.header, phases, format == ExportFormat.Pdf)
            suspend fun render() = withContext(NovelOutputSession.current.asContextElement(session)) {
                val retry = when (spec.retryMode) { "foreground" -> EpubImageRetryPolicy.Foreground()
                    "batch" -> EpubImageRetryPolicy.Batch(); else -> EpubImageRetryPolicy.Auto }
                if (spec.merged != null) {
                    MergedNovelWriters.raw(format).write(app, spec.merged.content(gson), spec.destination.relativePath(), retry) { key, bytes ->
                        context.progress("novel-image:$key:$bytes")
                    }
                } else {
                    val web = spec.webJson?.let { gson.fromJson(it, WebNovel::class.java) } ?: phases.run(context.plan.id,
                        "novel-text", "novel-text:${spec.novelId}", Pool.NETWORK) {
                        val html = Client.appApi.getNovelText(spec.novelId).use { it.string() }
                        WebNovelParser.parsePixivObject(html)?.novel ?: throw IOException("Invalid novel response")
                    }
                    if (spec.legacyTxt) {
                        val text = "\n\n<===== Shaft Novel Start =====>\n\n" + spec.header.orEmpty() +
                            "\n正文：\n\n" + ceui.pixiv.ui.task.DownloadNovelTask.replaceBrWithNewLine(web.text) +
                            "\n\n<===== Shaft Novel End =====>\n\n"
                        session.capture("text/plain") { it.write(text.toByteArray(Charsets.UTF_8)) }
                    } else {
                        val result = NovelExportManager.raw(format).export(app, novel, web,
                            spec.tokens?.map { it.token() } ?: ceui.pixiv.ui.novel.reader.paginate.ContentParser.tokenize(web),
                            spec.destination.relativePath(), retry)
                        if (result is ExportResult.Failure) throw IOException(result.message, result.cause)
                    }
                }
            }
            context.progress("novel-render")
            if (format == ExportFormat.Pdf) phases.run(context.plan.id, "novel-pdf", "novel-input:${context.plan.id}", Pool.ENCODE) { render() }
            else render()
        }
        context.progress("novel-publish")
        publisher.publish(context, source, spec.destination) { uri -> novel?.let { record(it, uri) } }
    }
    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val spec = request(plan)
        val novel = spec.novelJson?.let { gson.fromJson(it, Novel::class.java) }
        return publisher.reconcile(plan, spec.destination) { uri -> novel?.let { record(it, uri) } }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        phases.run(plan.id, "novel-cleanup", "novel-input:${plan.id}", Pool.COPY) {
            val directory = source(plan).parentFile!!
            if (directory.exists() && !directory.deleteRecursively()) throw IOException("Cannot remove owned novel input")
        }
    }
    companion object {
        private fun mode(policy: EpubImageRetryPolicy) = when (policy) {
            is EpubImageRetryPolicy.Foreground -> "foreground"; is EpubImageRetryPolicy.Batch -> "batch"; else -> "auto"
        }
        @Volatile private var instance: Pair<Context, QueuedNovelExports>? = null
        @JvmStatic fun get(context: Context): QueuedNovelExports {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedNovelExports(app).also { instance = app to it }
            }
        }
    }
}
