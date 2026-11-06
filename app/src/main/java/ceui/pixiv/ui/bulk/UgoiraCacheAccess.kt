package ceui.pixiv.ui.bulk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** A cache clear waits outside physical pools; active pipelines/exports keep their directory lease. */
internal class UgoiraCacheGate {
    private val lock = Any()
    private val writer = Mutex()
    private var readers = 0
    private var clearing = false
    private var changed = CompletableDeferred<Unit>()
    private class ReadLease(val gate: UgoiraCacheGate) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<ReadLease>
    }
    private fun signal() { changed.complete(Unit); changed = CompletableDeferred() }
    suspend fun <T> read(block: suspend () -> T): T {
        if (currentCoroutineContext()[ReadLease]?.gate === this) return block()
        while (true) {
            currentCoroutineContext().ensureActive()
            val wait = synchronized(lock) {
                if (!clearing) { readers++; null } else changed
            }
            if (wait == null) break
            wait.await()
        }
        try { return withContext(ReadLease(this)) { block() } }
        finally { synchronized(lock) { readers--; check(readers >= 0); signal() } }
    }
    suspend fun <T> clear(block: suspend () -> T): T = writer.withLock {
        check(currentCoroutineContext()[ReadLease]?.gate !== this) { "A cache reader cannot await its own deletion" }
        synchronized(lock) { clearing = true; signal() }
        try {
            while (true) {
                val wait = synchronized(lock) { if (readers == 0) null else changed }
                if (wait == null) break
                wait.await()
            }
            block()
        } finally { synchronized(lock) { clearing = false; signal() } }
    }
}

object UgoiraCacheAccess {
    private val gate = UgoiraCacheGate()
    internal suspend fun <T> read(block: suspend () -> T) = gate.read(block)
    internal suspend fun <T> maintain(block: suspend () -> T) = gate.clear {
        ceui.pixiv.download.queue.ContentPhases.run("ugoira-cache", "maintenance", "ugoira-cache-clear",
            ceui.pixiv.download.queue.ContentQueueStore.Pool.COPY, block = block)
    }
    @JvmStatic fun clear(context: android.content.Context) = kotlinx.coroutines.runBlocking {
        gate.clear {
            ceui.pixiv.download.queue.ContentPhases.run("ugoira-cache", "clear", "ugoira-cache-clear",
                ceui.pixiv.download.queue.ContentQueueStore.Pool.COPY) {
                val root = ceui.lisa.file.LegacyFile.gifCacheFolder(context)
                root.listFiles()?.forEach { file ->
                    if (!file.deleteRecursively()) throw java.io.IOException("Cannot remove unused ugoira cache")
                }
                UgoiraEngine.invalidateAll()
            }
        }
    }
}
