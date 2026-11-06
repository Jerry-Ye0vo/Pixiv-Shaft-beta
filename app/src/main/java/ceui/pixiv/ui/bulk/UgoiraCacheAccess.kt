package ceui.pixiv.ui.bulk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Shared directory gate; compatibility name keeps the existing cache regression suite. */
internal typealias UgoiraCacheGate = ceui.pixiv.download.queue.ContentResourceGate

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
