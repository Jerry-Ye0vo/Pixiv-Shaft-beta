package ceui.pixiv.download.queue

import android.content.Context
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State

/** One recovery authority for every adapter using content_queue.db in this process. IO only. */
class ContentQueueRuntime @JvmOverloads constructor(context: Context, name: String,
    private val clock: () -> Long = { android.os.SystemClock.elapsedRealtime() },
    private val agingMillis: Long = 2000L) : java.io.Closeable {
    private val application = context.applicationContext
    val store = ContentQueueStore(application, name)
    private var recovered = false
    private val signal = MutableStateFlow(0L)
    private val listeners = java.util.concurrent.CopyOnWriteArrayList<Runnable>()
    val changes: StateFlow<Long> = signal.asStateFlow()
    private data class Ready(val id: String, val pool: Pool, var priority: Int, val since: Long, val sequence: Long)
    private val arbitration = Any()
    private val ready = mutableMapOf<String, Ready>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var aging: Job? = null
    private val retiredStages = java.util.concurrent.atomic.AtomicInteger()

    /** All eligible adapters compete here. Waiting on a dependency must happen before registration. */
    @JvmOverloads fun claim(id: String, pool: Pool, capacity: Int, priority: Int = SAVE): ContentQueueStore.Task? {
        var notify = false
        val result = synchronized(arbitration) {
            val task = store.get(id) ?: return@synchronized null
            if (task.state != State.QUEUED || task.pool != pool) { ready.remove(id); return@synchronized null }
            val old = ready[id]
            if (old == null) {
                ready[id] = Ready(id, pool, priority.coerceIn(BACKGROUND, VISIBLE), clock(), task.sequence); notify = true
            } else if (priority > old.priority) { old.priority = priority.coerceAtMost(VISIBLE); notify = true }
            val stale = ready.keys.filter { store.get(it)?.state != State.QUEUED }
            stale.forEach(ready::remove)
            val now = clock()
            val winner = ready.values.asSequence().filter { it.pool == pool && store.resourceAvailable(it.id) }
                .sortedWith(compareByDescending<Ready> { (it.priority + ((now - it.since).coerceAtLeast(0) / agingMillis)).coerceAtMost(VISIBLE.toLong()) }
                    .thenBy { it.sequence }).firstOrNull()
            val lease = if (winner?.id == id) store.claim(id, pool, capacity) else null
            if (lease != null) ready.remove(id)
            if (ready.isNotEmpty() && aging?.isActive != true) {
                // One process timer promotes old work. Ordinary dispatch uses explicit change events.
                aging = scope.launch {
                    while (true) {
                        delay(agingMillis)
                        val pending = synchronized(arbitration) {
                            ready.keys.filter { store.get(it)?.state != State.QUEUED }.forEach(ready::remove)
                            ready.isNotEmpty()
                        }
                        if (!pending) break
                        changed()
                    }
                }
            }
            lease
        }
        if (notify) changed()
        return result
    }
    fun withdraw(id: String) {
        synchronized(arbitration) { ready.remove(id) }
        if (id.startsWith(ContentPhaseRunner.PREFIX) && retiredStages.incrementAndGet() % 128 == 0) store.compactStages(1024)
        changed()
    }
    fun prioritize(id: String, priority: Int) {
        synchronized(arbitration) { ready[id]?.let { it.priority = maxOf(it.priority, priority.coerceIn(BACKGROUND, VISIBLE)) } }
        changed()
    }

    @Synchronized fun initialize() {
        if (!recovered) {
            store.recoverAfterProcessDeath()
            // Physical child stages have no independent replay authority. Their durable parent
            // rechecks artifacts and submits fresh stages; abandoned children must not become ghosts.
            store.cancelScope(ContentPhaseRunner.PREFIX)
            store.compactStages(1024)
            recovered = true
        }
    }
    fun addListener(listener: Runnable) { listeners.add(listener) }
    fun removeListener(listener: Runnable) { listeners.remove(listener) }
    fun changed() {
        signal.update { it + 1 }
        listeners.forEach { listener ->
            try { listener.run() }
            catch (error: Exception) { timber.log.Timber.tag("ContentQueue").w(error, "Queue wake-up failed") }
        }
    }
    override fun close() {
        scope.cancel(); synchronized(arbitration) { ready.clear() }
        listeners.clear()
        store.close()
        synchronized(Companion) { if (instance === this) instance = null }
    }

    companion object {
        const val BACKGROUND = 0
        const val SAVE = 1
        const val VISIBLE = 2
        @JvmStatic fun notifyConfigurationChanged() { instance?.changed() }
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
