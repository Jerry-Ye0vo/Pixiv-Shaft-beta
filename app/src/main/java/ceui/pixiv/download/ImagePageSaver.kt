package ceui.pixiv.download

import android.content.Context
import androidx.annotation.MainThread
import ceui.lisa.R
import ceui.lisa.activities.Shaft
import ceui.lisa.core.Manager
import ceui.lisa.utils.Common
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.copy.CopyStatus
import ceui.pixiv.download.copy.CopyTicket
import ceui.pixiv.download.copy.QueuedCopies
import ceui.pixiv.download.queue.ContentQueueStore.State
import dagger.hilt.android.qualifiers.ApplicationContext
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

/** UI facade only. Ordinary pages and derived file writes both require durable queue permissions. */
@Singleton
class ImagePageSaver private constructor(
    context: Context,
    queueFactory: () -> ceui.pixiv.download.copy.DurableCopyQueue,
    private val requestFactory: (Illust, Int, File) -> ceui.pixiv.download.copy.CopyRequest,
) {
    @Inject constructor(@ApplicationContext context: Context) : this(context,
        { QueuedCopies.get(context.applicationContext) }, QueuedCopies::translatedRequest)
    internal constructor(context: Context, queue: ceui.pixiv.download.copy.DurableCopyQueue,
                         factory: (Illust, Int, File) -> ceui.pixiv.download.copy.CopyRequest) : this(context, { queue }, factory)
    private val app = context.applicationContext
    private val queue by lazy(queueFactory)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val pending = ConcurrentHashMap<PageKey, CopyTicket>()
    data class PageKey(val illustId: Long, val page: Int)
    sealed interface Target {
        data class Original(val imageUrl: String) : Target
        data class Translated(val file: File) : Target
    }
    sealed interface PageState {
        val target: Target
        data class Saving(override val target: Target, val percent: Int) : PageState
        data class Failed(override val target: Target) : PageState
    }
    private val mutable = MutableStateFlow<Map<PageKey, PageState>>(emptyMap())
    val states: StateFlow<Map<PageKey, PageState>> = mutable.asStateFlow()

    init {
        scope.launch {
            queue.states.collect { rows ->
                val latest = rows.values.filter { it.request?.kind == QueuedCopies.TRANSLATION }
                    .groupBy { PageKey(it.request!!.subjectId, it.request.page) }
                    .mapValues { (_, versions) -> versions.maxWith(compareBy<CopyStatus> { it.request!!.submittedAt }.thenBy { it.sequence }) }
                mutable.update { current ->
                    val next = current.toMutableMap()
                    latest.forEach { (key, status) ->
                        if (!pending.containsKey(key)) {
                            val target = Target.Translated(File(status.request!!.sourcePath))
                            when (status.state) {
                                State.SUCCEEDED, State.CANCELLED -> next.remove(key)
                                State.FAILED -> next[key] = PageState.Failed(target)
                                else -> next[key] = PageState.Saving(target, status.progress)
                            }
                        }
                    }
                    next
                }
            }
        }
    }

    fun isTranslationSaved(file: File): Boolean = QueuedCopies.TRANSLATION + ":" + file.absolutePath in queue.savedSources.value

    @MainThread
    fun save(illust: Illust, page: Int, target: Target): Boolean {
        require(!illust.isGif() && page in 0 until illust.page_count.coerceAtLeast(1))
        if (target is Target.Original) {
            IllustCaptionExporter.export(illust)
            val item = ceui.lisa.core.DownloadItem(illust, page).apply {
                url = target.imageUrl
                showUrl = ceui.lisa.download.IllustDownload.getShowUrl(illust, page)
            }
            Manager.get().addTasks(listOf(item))
            return true
        }
        val translated = target as Target.Translated
        val key = PageKey(illust.id, page)
        if (mutable.value[key] is PageState.Saving) return false
        val ticket = try {
            queue.submit(QueuedCopies.family(illust.id, page, translated.file), translated.file) {
                requestFactory(illust, page, translated.file)
            }
        } catch (error: Exception) {
            Common.showToast(app.getString(R.string.save_image_failed, error.message.orEmpty()))
            return false
        }
        pending[key] = ticket
        mutable.update { it + (key to PageState.Saving(target, 0)) }
        scope.launch {
            val terminal = ticket.status.first { status ->
                if (pending[key] === ticket) {
                    mutable.update { current ->
                        when (status.state) {
                            State.SUCCEEDED, State.CANCELLED -> current - key
                            State.FAILED -> current + (key to PageState.Failed(target))
                            else -> current + (key to PageState.Saving(target, status.progress))
                        }
                    }
                }
                status.state in setOf(State.SUCCEEDED, State.CANCELLED, State.FAILED)
            }
            pending.remove(key, ticket)
            if (terminal.state == State.FAILED) Common.showToast(app.getString(R.string.save_image_failed, terminal.error.orEmpty()))
            else if (terminal.state == State.CANCELLED) Common.showToast(R.string.save_image_cancelled)
        }
        return true
    }

    /** The same restored ticket is controllable; cancellation never interrupts an output already writing. */
    @MainThread
    fun cancel(key: PageKey): Boolean {
        val ticket = pending[key] ?: queue.states.value.values.filter {
            it.request?.kind == QueuedCopies.TRANSLATION && it.request.subjectId == key.illustId && it.request.page == key.page
                && it.state !in setOf(State.SUCCEEDED, State.CANCELLED, State.FAILED)
        }.maxByOrNull { it.request!!.submittedAt }?.let { queue.ticket(it.id) } ?: return false
        return ticket.cancel()
    }
}
