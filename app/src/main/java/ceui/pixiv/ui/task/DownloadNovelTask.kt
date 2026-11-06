package ceui.pixiv.ui.task

import ceui.lisa.R
import ceui.loxia.Novel
import ceui.pixiv.api.model.WebNovel
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.model.RelativePath
import ceui.pixiv.download.toast.DownloadToastKind
import ceui.pixiv.download.toast.DownloadToasts
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

class DownloadNovelTask(
    private val coroutineScope: CoroutineScope,
    private val novel: Novel,
    private val webNovel: WebNovel? = null,
) : QueuedRunnable<Unit>() {

    private val destination: RelativePath = DownloadItems.novelDestinationFromLoxia(novel)
    private val fileName: String = destination.filename

    override fun start(onNext: () -> Unit) {
        super.start(onNext)
        coroutineScope.launch { execute() }
    }

    override suspend fun execute() {
        if (_status.value is TaskStatus.Executing || _status.value is TaskStatus.Finished) {
            onIgnore(); return
        }
        try {
            onStart(); _status.value = TaskStatus.Executing(0)
            val result = ceui.pixiv.ui.novel.reader.export.QueuedNovelExports.get(context).single(
                ceui.pixiv.ui.novel.reader.export.ExportFormat.Txt, novel, webNovel, null, destination,
                ceui.pixiv.ui.novel.reader.export.EpubImageRetryPolicy.Auto, legacy = true)
            when (result) {
                is ceui.pixiv.ui.novel.reader.export.ExportResult.Failure -> onError(RuntimeException(result.message, result.cause))
                else -> {
                    DownloadToasts.show(DownloadToastKind.NOVEL_SAVE, context.getString(R.string.string_181))
                    _status.value = TaskStatus.Finished; onEnd(Unit)
                }
            }
        } catch (cancelled: kotlinx.coroutines.CancellationException) { throw cancelled }
        catch (error: Exception) { onError(error) }
    }

    // 定义替换方法，将 <br> 替换为换行符
    companion object {
        fun replaceBrWithNewLine(input: String?): String {
            return input
                ?.replace(Regex("<br\\s*/?>"), "\n") // 替换 <br> 和 <br/> 为换行符
                ?.replace(Regex("<[^>]*>"), "") // 移除其他 HTML 标签
                ?: ""
        }
    }
}

