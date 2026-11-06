package ceui.pixiv.ui.task

import android.content.Context
import ceui.lisa.activities.Shaft
import ceui.loxia.Novel
import ceui.pixiv.api.model.NovelSeriesDetail
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.bulk.FetchEvent
import ceui.pixiv.ui.novel.reader.export.*
import com.google.gson.Gson
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

/** The dialog observes a process-owned group; closing the page never owns its chapter fetches. */
class MergeDownloadNovelSeriesTask(context: Context) {
    private val app = context.applicationContext
    fun bulkMergeNovelSeries(seriesDetail: NovelSeriesDetail, knownNovels: List<Novel>, format: ExportFormat,
        stopSignal: AtomicBoolean, allowAutoEpub: Boolean = false,
        confirmFormat: (suspend (ExportFormat) -> ExportFormat?)? = null): Flow<FetchEvent> = flow {
        val started = System.currentTimeMillis()
        emit(FetchEvent.Started("merge-novel-series --format=${format.extension}", "/novel/series/${seriesDetail.id}"))
        val service = QueuedNovelGroups.get(app); val gson = Gson()
        val ticket = service.enqueue(NovelGroupRequest(mode = "series", series = listOf(NovelGroupSeries(
            seriesDetail.id, seriesDetail.title.orEmpty(), gson.toJson(seriesDetail), knownNovels.map(gson::toJson))),
            format = format.name, naming = QueuedNovelGroups.naming(), askOnImages = allowAutoEpub &&
                Shaft.sSettings.isDefaultNovelExportEpubOnImages))
        coroutineScope {
            val stopWatcher = launch {
                while (!stopSignal.get()) delay(100)
                service.queue.finishByUser(ticket)
            }
            try {
                var last = ""
                val end = ticket.status.first { status ->
                    if (status.phase != last) { last = status.phase; emit(FetchEvent.Log(status.phase)) }
                    when (status.state) {
                        State.PAUSED -> {
                            if (service.state(ticket.id)?.awaitingFormat == true && confirmFormat != null) {
                                val chosen = confirmFormat(format)
                                if (chosen != null) { service.chooseFormat(ticket, chosen); false }
                                else { ticket.cancel(); true }
                            } else true
                        }
                        State.SUCCEEDED, State.CANCELLED, State.FAILED -> true
                        else -> false
                    }
                }
                val result = service.state(ticket.id)
                if (end.state == State.SUCCEEDED) {
                    if ((result?.skipped ?: 0) > 0) emit(FetchEvent.Warning("跳过 ${result?.skipped} 章"))
                    if (stopSignal.get()) emit(FetchEvent.Log("用户停止 · 已保存抓到的章节"))
                    emit(FetchEvent.Done(result?.chapters ?: 0, System.currentTimeMillis() - started, result?.chapters ?: 0))
                } else emit(FetchEvent.Errored(end.error ?: "任务已暂停或取消，可在下载管理中继续", result?.chapters ?: 0))
            } finally { stopWatcher.cancel() }
        }
    }.flowOn(Dispatchers.IO)
}
