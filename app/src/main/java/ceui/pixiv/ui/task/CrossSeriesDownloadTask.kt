package ceui.pixiv.ui.task

import android.content.Context
import androidx.fragment.app.FragmentActivity
import androidx.lifecycle.lifecycleScope
import ceui.lisa.R
import ceui.lisa.models.NovelSeriesItem
import ceui.pixiv.download.queue.ContentPlanTicket
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.download.toast.DownloadToastKind
import ceui.pixiv.download.toast.DownloadToasts
import ceui.pixiv.ui.novel.reader.export.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

/** Lifecycle controls observation only. The complete multi-series intention is admitted once. */
class CrossSeriesDownloadTask(context: Context) {
    private val app = context.applicationContext
    enum class Mode { PerSeriesSelected, PerSeriesAll, AllMergedOne }
    data class SeriesFailure(val seriesTitle: String, val reason: String)
    private fun submit(series: List<NovelSeriesItem>, format: ExportFormat, mode: String,
        author: String? = null, authorId: Long = 0): Pair<QueuedNovelGroups, ContentPlanTicket> {
        val service = QueuedNovelGroups.get(app)
        val ticket = service.enqueue(NovelGroupRequest(mode = mode,
            series = series.distinctBy { it.id }.map { NovelGroupSeries(it.id.toLong(), it.title.orEmpty()) },
            format = format.name, naming = QueuedNovelGroups.naming(), authorName = author, authorId = authorId))
        return service to ticket
    }
    fun runPerSeries(activity: FragmentActivity, seriesList: List<NovelSeriesItem>, format: ExportFormat,
        onFinished: (success: Int, failures: List<SeriesFailure>) -> Unit) {
        if (seriesList.isEmpty()) { onFinished(0, emptyList()); return }
        val (service, ticket) = submit(seriesList, format, "per-series")
        activity.lifecycleScope.launch {
            val (end, result) = withContext(Dispatchers.IO) {
                ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END } to service.state(ticket.id)
            }
            val unique = seriesList.distinctBy { it.id }
            val failures = result?.parts?.mapIndexedNotNull { index, part ->
                part.failure?.let { SeriesFailure(unique[index].title.orEmpty(), it) }
            }.orEmpty().toMutableList()
            if (end.state != State.SUCCEEDED) failures += SeriesFailure("", end.error ?: "任务已暂停或取消")
            val success = result?.success ?: 0
            DownloadToasts.show(DownloadToastKind.NOVEL_SERIES_EXPORT,
                app.getString(R.string.cross_series_download_all_done, success, failures.size))
            onFinished(success, failures)
        }
    }
    fun runAllMergedOne(activity: FragmentActivity, seriesList: List<NovelSeriesItem>, authorName: String?,
        authorId: Long, format: ExportFormat, onFinished: (ok: Boolean, skippedChapters: Int) -> Unit) {
        if (seriesList.isEmpty()) { onFinished(false, 0); return }
        val (service, ticket) = submit(seriesList, format, "all", authorName, authorId)
        activity.lifecycleScope.launch {
            val (end, result) = withContext(Dispatchers.IO) {
                ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END } to service.state(ticket.id)
            }
            DownloadToasts.show(DownloadToastKind.NOVEL_SERIES_EXPORT, end.error ?: if (end.state == State.SUCCEEDED)
                "小说合集已处理" else "小说合集任务已暂停或取消")
            onFinished(end.state == State.SUCCEEDED && (result?.success ?: 0) > 0, result?.skipped ?: -1)
        }
    }
}
