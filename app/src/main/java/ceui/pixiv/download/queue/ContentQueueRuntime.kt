package ceui.pixiv.download.queue

import android.content.Context
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update

/** One recovery authority for every adapter using content_queue.db in this process. IO only. */
class ContentQueueRuntime(context: Context, name: String) : java.io.Closeable {
    val store = ContentQueueStore(context, name)
    private var recovered = false
    private val signal = MutableStateFlow(0L)
    val changes: StateFlow<Long> = signal.asStateFlow()

    @Synchronized fun initialize() {
        if (!recovered) {
            store.recoverAfterProcessDeath()
            recovered = true
        }
    }
    fun changed() { signal.update { it + 1 } }
    override fun close() { store.close() }

    companion object {
        @Volatile private var instance: ContentQueueRuntime? = null
        @JvmStatic fun get(context: Context): ContentQueueRuntime = instance ?: synchronized(this) {
            instance ?: ContentQueueRuntime(context.applicationContext, "content_queue.db").also { instance = it }
        }
    }
}
