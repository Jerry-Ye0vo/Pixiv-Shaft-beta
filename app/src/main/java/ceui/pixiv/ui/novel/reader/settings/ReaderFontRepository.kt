package ceui.pixiv.ui.novel.reader.settings

import android.app.Application
import ceui.lisa.http.GithubProxy
import ceui.pixiv.ui.novel.reader.paginate.TypefaceProvider
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import timber.log.Timber
import javax.inject.Inject
import javax.inject.Singleton

/**
 * 阅读器可下载字体（[ReaderWebFont]）的下载、删除与状态（#1060）。
 *
 * 进程级：关掉字体面板、甚至退出阅读器，下载照常跑完，回来时面板直接看到进度。
 * 构造廉价（见 [ceui.pixiv.services.ServicesProvider]）—— OkHttpClient 与首次的磁盘检查都延迟到真用时。
 */
@Singleton
class ReaderFontRepository @Inject constructor(private val app: Application) {

    sealed interface State {
        object Absent : State
        /** [fraction] 按目录里写死的字节数算，加速站不回 Content-Length 也有进度。 */
        data class Downloading(val fraction: Float) : State
        object Installed : State
        object Failed : State
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val owner by lazy { QueuedReaderFonts.get(app) }

    private val mutableStates by lazy {
        MutableStateFlow<Map<ReaderWebFont, State>>(
            ReaderWebFont.entries.associateWith { font ->
                if (font.file(app).exists()) State.Installed else State.Absent
            },
        ).also { states ->
            scope.launch {
                owner.queue.states.collect { rows ->
                    rows.values.filter { it.kind == "reader-font" }.mapNotNull { status ->
                        val plan = owner.queue.store.get(status.id) ?: return@mapNotNull null
                        Triple(plan.sequence, status, com.google.gson.Gson().fromJson(plan.request, ReaderFontRequest::class.java))
                    }.groupBy { it.third.fontId }.values.mapNotNull { it.maxByOrNull { row -> row.first } }.forEach { (_, status, request) ->
                        val font = ReaderWebFont.byId(request.fontId) ?: return@forEach
                        val value = when (status.state) {
                            ceui.pixiv.download.queue.ContentQueueStore.State.SUCCEEDED -> if (request.delete) State.Absent else State.Installed
                            ceui.pixiv.download.queue.ContentQueueStore.State.CANCELLED -> if (font.file(app).exists()) State.Installed else State.Absent
                            ceui.pixiv.download.queue.ContentQueueStore.State.FAILED -> if (font.file(app).exists()) State.Installed else State.Failed
                            ceui.pixiv.download.queue.ContentQueueStore.State.PAUSED -> State.Absent
                            else -> State.Downloading(if (status.total > 0) status.bytes.toFloat() / status.total else 0f)
                        }
                        states.update { it + (font to value) }
                    }
                }
            }
        }
    }

    val states: StateFlow<Map<ReaderWebFont, State>> get() = mutableStates

    fun isInstalled(font: ReaderWebFont): Boolean = states.value[font] == State.Installed

    fun download(font: ReaderWebFont) {
        if (isInstalled(font)) return
        owner.enqueue(font)
    }

    fun cancel(font: ReaderWebFont) { owner.queue.latest("reader-font:${font.id}")?.cancel() }
    fun delete(font: ReaderWebFont) { owner.enqueue(font, delete = true) }

    private fun set(font: ReaderWebFont, state: State) {
        mutableStates.update { it + (font to state) }
    }
}
