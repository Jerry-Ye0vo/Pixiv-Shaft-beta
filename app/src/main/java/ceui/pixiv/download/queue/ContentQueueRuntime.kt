package ceui.pixiv.download.queue

import android.content.Context
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update

/** One recovery authority for every adapter using content_queue.db in this process. IO only. */
class ContentQueueRuntime(context: Context, name: String) : java.io.Closeable {
    private val application = context.applicationContext
    val store = ContentQueueStore(application, name)
    private var recovered = false
    private val signal = MutableStateFlow(0L)
    private val listeners = java.util.concurrent.CopyOnWriteArrayList<Runnable>()
    val changes: StateFlow<Long> = signal.asStateFlow()

    @Synchronized fun initialize() {
        if (!recovered) {
            store.recoverAfterProcessDeath()
            // Physical child stages have no independent replay authority. Their durable parent
            // rechecks artifacts and submits fresh stages; abandoned children must not become ghosts.
            store.cancelScope(ContentPhaseRunner.PREFIX)
            recovered = true
        }
    }
    fun addListener(listener: Runnable) { listeners.add(listener) }
    fun changed() {
        signal.update { it + 1 }
        listeners.forEach { listener ->
            try { listener.run() }
            catch (error: Exception) { timber.log.Timber.tag("ContentQueue").w(error, "Queue wake-up failed") }
        }
    }
    override fun close() {
        listeners.clear()
        store.close()
        synchronized(Companion) { if (instance === this) instance = null }
    }

    companion object {
        @Volatile private var instance: ContentQueueRuntime? = null
        @JvmStatic fun get(context: Context): ContentQueueRuntime {
            val application = context.applicationContext
            return instance?.takeIf { it.application === application } ?: synchronized(this) {
                instance?.takeIf { it.application === application }
                    ?: ContentQueueRuntime(application, "content_queue.db").also { instance = it }
            }
        }
    }
}
