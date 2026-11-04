package ceui.pixiv.download

import android.content.Context
import androidx.annotation.MainThread
import ceui.lisa.R
import ceui.lisa.activities.Shaft
import ceui.lisa.core.Manager
import ceui.lisa.utils.Common
import ceui.lisa.utils.PixivOperate
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.config.DownloadItems
import dagger.hilt.android.qualifiers.ApplicationContext
import java.io.File
import java.io.FileInputStream
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import timber.log.Timber

/** Saves derived translation files. Original pages share Manager with selected and bulk pages. */
@Singleton
class ImagePageSaver @Inject constructor(@ApplicationContext context: Context) {

    private val app: Context = context.applicationContext

    data class PageKey(val illustId: Long, val page: Int)

    /** 存哪张图:原图(取大图页已加载的那份,不重下),或这一页当前显示的译图文件。 */
    sealed interface Target {
        data class Original(val imageUrl: String) : Target

        data class Translated(val file: File) : Target
    }

    sealed interface PageState {
        val target: Target

        /** [percent] 是原图加载进度;来源已在本地时直接是 100。 */
        data class Saving(override val target: Target, val percent: Int) : PageState

        data class Failed(override val target: Target) : PageState
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)

    private val _states = MutableStateFlow<Map<PageKey, PageState>>(emptyMap())
    val states: StateFlow<Map<PageKey, PageState>> = _states.asStateFlow()

    /**
     * 还能取消的保存。[cancel] 与 [claimWrite] 谁先把条目摘掉谁说了算:取消先摘到就不写盘,
     * 写盘先摘到就不再认取消 —— 两边都只做一次原子 remove,不会出现「取消了却已经写完」。
     */
    private val cancellable = ConcurrentHashMap<PageKey, Job>()

    /** 本进程存过的译图文件。译图每次产出都是新文件、也不进「已下载」库,译图页的「已保存」只能靠它判断。 */
    private val savedTranslations = ConcurrentHashMap.newKeySet<String>()

    fun isTranslationSaved(file: File): Boolean = file.path in savedTranslations

    /**
     * 开始(或重试)保存这一页。这一页正在存时什么都不做,返回 false。
     * 成功后按设置「下载后自动收藏」,收藏心由大图页对 ObjectPool 的观察刷新。
     */
    @MainThread
    fun save(illust: Illust, page: Int, target: Target): Boolean {
        if (target is Target.Original) {
            require(!illust.isGif() && page in 0 until illust.page_count.coerceAtLeast(1))
            IllustCaptionExporter.export(illust)
            val item = ceui.lisa.core.DownloadItem(illust, page).apply {
                url = target.imageUrl
                showUrl = ceui.lisa.download.IllustDownload.getShowUrl(illust, page)
            }
            Manager.get().addTasks(listOf(item))
            return true
        }
        val key = PageKey(illust.id, page)
        if (_states.value[key] is PageState.Saving) return false
        _states.update { it + (key to PageState.Saving(target, 0)) }
        // LAZY:先登记再开跑。Main.immediate 会当场执行到第一个挂起点,登记晚了取消就找不到它。
        val job =
            scope.launch(start = CoroutineStart.LAZY) {
                // 收尾放 finally:意料之外的异常(照旧抛出去)也要把「保存中」解掉记成失败,
                // 否则按钮一直转圈、点了也取消不掉。
                var failed = true
                try {
                    val ok =
                        when (target) {
                            is Target.Original -> error("Original pages are handled by Manager")
                            is Target.Translated -> saveTranslated(key, illust, page, target.file)
                        }
                    failed = !ok
                    if (ok) {
                        // 先记再解「保存中」:解掉那一刻大图页就按它判断译图页的绿勾
                        if (target is Target.Translated) savedTranslations.add(target.file.path)
                        Common.showToast(R.string.string_181)
                        if (Shaft.sSettings.isAutoPostLikeWhenDownload && !illust.isBookmarked) {
                            PixivOperate.postLikeDefaultStarType(illust)
                        }
                    }
                } catch (e: CancellationException) {
                    failed = false
                    Common.showToast(R.string.save_image_cancelled)
                    throw e
                } finally {
                    cancellable.remove(key, coroutineContext.job)
                    _states.update { if (failed) it + (key to PageState.Failed(target)) else it - key }
                }
            }
        cancellable[key] = job
        job.start()
        return true
    }

    /** 取消这一页的保存。已经开始写盘(或根本没在存)时返回 false,保存照常走完。 */
    @MainThread
    fun cancel(key: PageKey): Boolean {
        val job = cancellable.remove(key) ?: return false
        job.cancel()
        return true
    }

    /** 进入写盘:从此不再认取消。取消已经抢先时抛 [CancellationException],调用方不得写盘。 */
    private fun claimWrite(key: PageKey) {
        if (cancellable.remove(key) == null) throw CancellationException("save cancelled before writing")
    }

    private fun publishProgress(key: PageKey, percent: Int) {
        _states.update { states ->
            val current = states[key]
            if (current is PageState.Saving && current.percent != percent) {
                states + (key to current.copy(percent = percent))
            } else {
                states
            }
        }
    }

    private fun toastFailure(error: Throwable?) {
        val detail = error?.message ?: error?.javaClass?.simpleName ?: "load error"
        Common.showToast(app.getString(R.string.save_image_failed, detail))
    }

    /**
     * 保存译图:按用户插画模板命名并加 [TRANSLATED_FILENAME_SUFFIX],与原图并存、互不覆盖。
     * 译图是派生图片,不写「已下载」库 —— 那张表的记录会被详情页当原图本地副本复用。
     */
    private suspend fun saveTranslated(key: PageKey, illust: Illust, page: Int, file: File): Boolean =
        withContext(Dispatchers.IO) {
            claimWrite(key)
            runCatching {
                // open 返回 null = Skip 策略且同名译图已存在 → 视为已保存
                val handle =
                    DownloadsRegistry.downloads.openDerived(
                        DownloadItems.illustTranslated(illust, page),
                        TRANSLATED_FILENAME_SUFFIX,
                    ) ?: return@runCatching true
                try {
                    handle.stream.use { out -> FileInputStream(file).use { it.copyTo(out) } }
                    handle.onFinish()
                } catch (t: Throwable) {
                    handle.onAbort()
                    throw t
                }
                true
            }
                .getOrElse { ex ->
                    if (ex is CancellationException) throw ex
                    Timber.e(ex, "[ImagePageSaver] saveTranslated failed page=%d", page)
                    withContext(Dispatchers.Main) { toastFailure(ex) }
                    false
                }
        }

    private companion object {
        /** 译图文件名后缀,插在用户模板文件名与扩展名之间,避免覆盖原图。 */
        const val TRANSLATED_FILENAME_SUFFIX = "_translated"
    }
}

