package ceui.pixiv.ui.novel.reader.export

import android.content.Context
import androidx.annotation.Keep
import ceui.lisa.activities.Shaft
import ceui.lisa.fragments.WebNovelParser
import ceui.loxia.Novel
import ceui.pixiv.api.Client
import ceui.pixiv.api.model.NovelSeriesDetail
import ceui.pixiv.api.model.WebNovel
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.copy.CopyDestination
import ceui.pixiv.download.model.*
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.download.sanitize.FsSanitizer
import ceui.pixiv.download.template.PageNumbering
import ceui.pixiv.download.template.SafeTemplateRender
import com.google.gson.Gson
import java.io.IOException
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

@Keep data class NovelGroupSeries(val id: Long, val title: String, val detail: String? = null,
    val initial: List<String> = emptyList())
@Keep data class NovelGroupNaming(val template: String, val fromOne: Boolean, val padded: Boolean,
    val createdAt: Long, val destination: CopyDestination)
@Keep data class NovelGroupRequest(val version: Int = 1, val mode: String, val series: List<NovelGroupSeries>,
    val format: String, val naming: NovelGroupNaming, val authorName: String? = null, val authorId: Long = 0,
    val askOnImages: Boolean = false) {
    fun validate() {
        require(version == 1 && mode in setOf("series", "per-series", "all"))
        require(series.isNotEmpty() && series.all { it.id > 0 } && series.map { it.id }.distinct().size == series.size)
        ExportFormat.valueOf(format); naming.destination.storageChoice(); naming.destination.overwrite()
    }
}
@Keep data class NovelGroupPart(var detail: String? = null, val novels: MutableList<String> = mutableListOf(),
    var listed: Boolean = false, var pages: Int = 0, var next: Int = 0,
    val chapters: MutableList<ExportChapter> = mutableListOf(), var skipped: Int = 0,
    var output: String? = null, var written: Boolean = false, var failure: String? = null)
@Keep data class NovelGroupState(val parts: MutableList<NovelGroupPart> = mutableListOf(),
    var selectedFormat: String? = null, var awaitingFormat: Boolean = false,
    var success: Int = 0, var skipped: Int = 0, var chapters: Int = 0, var complete: Boolean = false)
internal data class NovelGroupPage(val detail: String?, val novels: List<String>, val hasNext: Boolean)

/** Durable chapter collection. Group coordinators never reserve a leaf or physical slot. */
class QueuedNovelGroups internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    private val exports: QueuedNovelExports = QueuedNovelExports.get(app),
    private val page: suspend (Long, Int?) -> NovelGroupPage = { id, after ->
        val response = Client.appApi.getNovelSeries(id, after)
        NovelGroupPage(response.novel_series_detail?.let(Gson()::toJson),
            response.novels.orEmpty().map(Gson()::toJson), !response.next_url.isNullOrEmpty())
    },
    private val chapter: suspend (Novel) -> WebNovel = { novel ->
        val html = Client.appApi.getNovelText(novel.id).use { it.string() }
        WebNovelParser.parsePixivObject(html)?.novel ?: throw IOException("Invalid novel ${novel.id}")
    },
    private val wait: suspend (Long) -> Unit = { delay(it) },
    private val autoAllowed: () -> Boolean = ceui.lisa.utils.DownloadLimitTypeUtil::autoStartAllowed,
) : ContentPlanHandler {
    override val kind = "novel-series-group"
    override val isGroup = true
    private val gson = Gson()
    private val manual = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    init { queue.register(this) }
    override fun onUserResume(plan: ContentPlanStore.Plan) { manual.add(plan.family) }
    override fun onNetworkPolicyRestricted() { manual.clear() }
    override fun canRun(plan: ContentPlanStore.Plan): Boolean {
        if (manual.contains(plan.family)) return true
        val request = gson.fromJson(plan.request, NovelGroupRequest::class.java)
        val state = gson.fromJson(plan.checkpoint, NovelGroupState::class.java)
        val local = (state.selectedFormat ?: request.format) != ExportFormat.Epub.name &&
            (queue.store.finishRequested(plan.id) || (state.parts.size == request.series.size &&
                state.parts.all { it.output != null || it.failure != null || (it.listed && it.next >= it.novels.size) }))
        return local || autoAllowed()
    }
    fun enqueue(request: NovelGroupRequest): ContentPlanTicket {
        request.validate()
        val family = "novel-group:${UUID.randomUUID()}"; manual.add(family)
        return queue.submit(family, kind) { gson.toJson(request) }
    }
    fun state(id: String): NovelGroupState? = queue.store.get(id)?.let { gson.fromJson(it.checkpoint, NovelGroupState::class.java) }
    fun chooseFormat(ticket: ContentPlanTicket, format: ExportFormat) {
        queue.store.selectFormat(ticket.id, format.name); queue.resume(ticket.id)
    }
    override suspend fun execute(context: ContentPlanContext) {
        val request = gson.fromJson(context.plan.request, NovelGroupRequest::class.java).also { it.validate() }
        val state = gson.fromJson(context.checkpoint, NovelGroupState::class.java)
        if (state.parts.isEmpty()) state.parts.addAll(request.series.map { NovelGroupPart(it.detail, it.initial.toMutableList()) })
        suspend fun save() { context.checkpoint(gson.toJson(state)) }
        fun finishRequested() = queue.store.finishRequested(context.plan.id)
        for ((index, input) in request.series.withIndex()) {
            val part = state.parts[index]
            if (part.output != null || part.failure != null) continue
            if (!part.listed && !finishRequested()) {
                try {
                    while (part.pages < 50 && !finishRequested()) {
                        context.progress("series:${index + 1}/${request.series.size}:page:${part.pages + 1}")
                        val after = part.novels.size.takeIf { it > 0 }
                        val response = phases.run(context.plan.id, "novel-series-page", "novel-series:${input.id}", Pool.NETWORK) {
                            page(input.id, after)
                        }
                        if (part.detail == null) part.detail = response.detail
                        val ids = part.novels.map { gson.fromJson(it, Novel::class.java).id }.toHashSet()
                        val fresh = response.novels.filter { ids.add(gson.fromJson(it, Novel::class.java).id) }
                        part.novels.addAll(fresh); part.pages++
                        part.listed = !response.hasNext || fresh.isEmpty()
                        save()
                        if (part.listed) break
                        wait(1000)
                    }
                    // A bounded listing never spins forever on a malformed pagination endpoint.
                    if (part.pages >= 50) { part.listed = true; save() }
                } catch (cancel: CancellationException) { throw cancel }
                catch (error: Exception) {
                    if (part.novels.isEmpty()) { part.failure = error.message ?: "系列读取失败"; save(); continue }
                    // Keep the already resolved chapters; do not silently claim a complete listing.
                    part.listed = true; context.progress("warning:系列列表不完整:${error.message}"); save()
                }
            }
            while (part.next < part.novels.size && !finishRequested()) {
                val novel = gson.fromJson(part.novels[part.next], Novel::class.java)
                context.progress("series:${index + 1}/${request.series.size}:chapter:${part.next + 1}/${part.novels.size}")
                try {
                    val web = phases.run(context.plan.id, "novel-chapter", "novel-text:${novel.id}", Pool.NETWORK) { chapter(novel) }
                    part.chapters += ExportChapter.capture(MergedChapter.numbered(part.next + 1,
                        novel.title.orEmpty(), ceui.pixiv.ui.task.DownloadNovelTask.replaceBrWithNewLine(web.text), web), gson)
                } catch (cancel: CancellationException) { throw cancel }
                catch (error: Exception) { part.skipped++; context.progress("warning:章节 ${novel.id}: ${error.message}") }
                part.next++; save()
                if (part.next < part.novels.size && !finishRequested()) wait(1500)
            }
            if (request.mode != "all" && part.chapters.isNotEmpty()) {
                val format = selectFormat(context, request, state, part.chapters)
                val detail = part.detail?.let { gson.fromJson(it, NovelSeriesDetail::class.java) }
                val content = MergedNovelContent(detail?.title ?: input.title, detail?.user?.name,
                    "https://www.pixiv.net/novel/series/${input.id}", detail?.caption,
                    part.chapters.map { it.chapter(gson) }, "novel_series_${input.id}")
                val meta = ItemMeta(id = input.id, title = content.displayTitle,
                    author = Author(detail?.user?.id ?: 0, content.author.orEmpty()),
                    createdAt = Instant.ofEpochMilli(request.naming.createdAt),
                    flags = setOf(Flag.Series) + if (part.novels.any { (gson.fromJson(it, Novel::class.java).x_restrict ?: 0) > 0 }) setOf(Flag.R18) else emptySet(),
                    seriesTitle = input.title.takeIf { it.isNotBlank() }, seriesTotal = part.chapters.size)
                val destination = destination(request.naming, meta, format)
                val family = "${context.plan.id}:output:$index"
                queue.store.ownChild(context.plan.id, family)
                val child = exports.enqueueMerged(format, content, destination, family, reuseSucceeded = true,
                    manualStart = manual.contains(context.plan.family))
                awaitChild(context, child)
                val receipt = gson.fromJson(queue.store.get(child.id).checkpoint, ArtifactCheckpoint::class.java)
                part.output = child.id; part.written = !receipt.skipped; save()
            } else if (part.chapters.isEmpty() && !finishRequested()) {
                part.failure = "没有可导出的章节"; save()
            }
            if (finishRequested()) break
        }
        if (request.mode == "all") {
            val actual = state.parts.sumOf { it.chapters.size }
            if (actual == 0) throw IOException(if (finishRequested()) "用户停止，尚无可保存章节" else "没有可导出的章节")
            val format = selectFormat(context, request, state, state.parts.flatMap { it.chapters })
            val chapters = state.parts.flatMapIndexed { index, part ->
                if (part.chapters.isEmpty()) emptyList() else listOf(MergedChapter(
                    "<系列 ${index + 1}/${request.series.size}>《${request.series[index].title}》",
                    "SeriesId: ${request.series[index].id}\nChapters: ${part.novels.size}")) + part.chapters.map { it.chapter(gson) }
            }
            val content = MergedNovelContent("${request.authorName.orEmpty()} 全系列合集", request.authorName,
                "https://www.pixiv.net/users/${request.authorId}", "${request.series.size} 个系列", chapters,
                "pixiv_author_${request.authorId}_merged")
            val meta = ItemMeta(id = 0, title = "", author = Author(request.authorId, request.authorName.orEmpty()),
                createdAt = Instant.ofEpochMilli(request.naming.createdAt), seriesTotal = actual)
            val rendered = destination(request.naming, meta, format)
            val base = request.authorName.orEmpty().replace(Regex("[\\\\/:*?\"<>|]"), "").trim().take(40)
                .ifEmpty { "user_${request.authorId}" }
            val path = RelativePath(rendered.relativePath().directory + "${base}_全系列合集_U${request.authorId}.${format.extension}")
            val target = rendered.copy(segments = path.segments)
            val family = "${context.plan.id}:output:all"
            queue.store.ownChild(context.plan.id, family)
            val child = exports.enqueueMerged(format, content, target, family, reuseSucceeded = true,
                manualStart = manual.contains(context.plan.family))
            awaitChild(context, child)
            val receipt = gson.fromJson(queue.store.get(child.id).checkpoint, ArtifactCheckpoint::class.java)
            state.success = if (receipt.skipped) 0 else 1
        } else state.success = state.parts.count { it.written }
        state.skipped = state.parts.sumOf { it.skipped }; state.chapters = state.parts.sumOf { it.chapters.size }
        if (state.chapters == 0 && state.success == 0) throw IOException("没有可导出的章节")
        state.complete = true; save()
        context.commit {}
    }
    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val saved = gson.fromJson(plan.checkpoint, NovelGroupState::class.java)
        val children = queue.store.children(plan.id)
        return saved.complete && children.isNotEmpty() && children.all { it.state == State.SUCCEEDED }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state == State.CANCELLED) queue.store.children(plan.id).forEach { queue.ticket(it.id)?.cancel() }
    }
    private suspend fun selectFormat(context: ContentPlanContext, request: NovelGroupRequest,
        state: NovelGroupState, chapters: List<ExportChapter>): ExportFormat {
        state.selectedFormat = queue.store.selectedFormat(context.plan.id) ?: state.selectedFormat
        if (state.selectedFormat == null && request.askOnImages && request.format == ExportFormat.Txt.name &&
            chapters.any { it.webJson?.let { json -> gson.fromJson(json, WebNovel::class.java) }?.let {
                !it.illusts.isNullOrEmpty() || !it.images.isNullOrEmpty() } == true }) {
            state.awaitingFormat = true; context.checkpoint(gson.toJson(state)); context.progress("await-format")
            queue.ticket(context.plan.id)!!.pause()
            throw CancellationException("请选择有插图小说的导出格式后继续")
        }
        state.awaitingFormat = false
        return ExportFormat.valueOf(state.selectedFormat ?: request.format)
    }
    private suspend fun awaitChild(context: ContentPlanContext, child: ContentPlanTicket) {
        try {
            val end = child.status.first { context.progress("output:${it.phase}", it.bytes, it.total); it.state in ContentPlanTicket.OBSERVABLE_END }
            when (end.state) {
                State.SUCCEEDED -> Unit
                State.PAUSED -> { queue.ticket(context.plan.id)!!.pause(); throw CancellationException("输出已暂停") }
                State.CANCELLED -> { queue.ticket(context.plan.id)!!.cancel(); throw CancellationException("输出已取消") }
                else -> throw IOException(end.error ?: "小说合集写入失败")
            }
        } catch (cancel: CancellationException) {
            withContext(NonCancellable) {
                if (queue.ticket(context.plan.id)?.status?.value?.state == State.CANCEL_REQUESTED) child.cancel() else child.pause()
                child.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
            }
            throw cancel
        }
    }
    companion object {
        fun naming(): NovelGroupNaming {
            val config = DownloadsRegistry.store.loadOrFallback(); val resolved = config.resolve(Bucket.NovelSeries)
            return NovelGroupNaming(resolved.template, config.pageIndexFrom1, config.padPageNumber, System.currentTimeMillis(),
                CopyDestination.capture(resolved.storage, RelativePath(listOf("novel-group.txt")), "text/plain", resolved.overwrite))
        }
        internal fun destination(naming: NovelGroupNaming, meta: ItemMeta, format: ExportFormat): CopyDestination {
            val rendered = FsSanitizer.clean(SafeTemplateRender.render(naming.template, Bucket.NovelSeries,
                meta, format.extension, PageNumbering(naming.fromOne, naming.padded)))
            val old = rendered.filename.substringAfterLast('.', "")
            val name = if (old.lowercase() in setOf("txt", "md", "epub", "pdf"))
                rendered.filename.substringBeforeLast('.') + ".${format.extension}" else rendered.filename + ".${format.extension}"
            return naming.destination.copy(segments = rendered.directory + name, mime = format.mimeType)
        }
        @Volatile private var instance: Pair<Context, QueuedNovelGroups>? = null
        @JvmStatic fun get(context: Context): QueuedNovelGroups {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedNovelGroups(app).also { instance = app to it }
            }
        }
    }
}
