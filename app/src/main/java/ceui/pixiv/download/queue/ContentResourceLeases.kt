package ceui.pixiv.download.queue

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Directory/source ownership is acquired before physical pools. Writers cannot starve behind new readers. */
class ContentResourceGate {
    private val lock = Any()
    private val writer = Mutex()
    private var readers = 0
    private var writing = false
    private var changed = CompletableDeferred<Unit>()
    private class Read(val gates: Set<ContentResourceGate>) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<Read>
    }
    private class Write(val gates: Set<ContentResourceGate>) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<Write>
    }
    private fun signal() { changed.complete(Unit); changed = CompletableDeferred() }
    suspend fun <T> read(block: suspend () -> T): T {
        val context = currentCoroutineContext()
        if (context[Read]?.gates?.contains(this) == true || context[Write]?.gates?.contains(this) == true) return block()
        while (true) {
            currentCoroutineContext().ensureActive()
            val wait = synchronized(lock) { if (!writing) { readers++; null } else changed }
            if (wait == null) break
            wait.await()
        }
        try { return withContext(Read((context[Read]?.gates ?: emptySet()) + this)) { block() } }
        finally { synchronized(lock) { readers--; check(readers >= 0); signal() } }
    }
    suspend fun <T> write(block: suspend () -> T): T {
        val context = currentCoroutineContext()
        if (context[Write]?.gates?.contains(this) == true) return block()
        check(context[Read]?.gates?.contains(this) != true) { "A reader cannot await replacement of its own source" }
        return writer.withLock {
            synchronized(lock) { writing = true; signal() }
            try {
                while (true) {
                    val wait = synchronized(lock) { if (readers == 0) null else changed }
                    if (wait == null) break
                    wait.await()
                }
                withContext(Write((context[Write]?.gates ?: emptySet()) + this)) { block() }
            } finally { synchronized(lock) { writing = false; signal() } }
        }
    }
    /** Compatibility spelling used by cache clearing adapters. */
    suspend fun <T> clear(block: suspend () -> T): T = write(block)
}

object ContentResourceLeases {
    private class Entry { val gate = ContentResourceGate(); var users = 0 }
    private val entries = mutableMapOf<String, Entry>()
    private suspend fun <T> use(key: String, write: Boolean, block: suspend () -> T): T {
        require(key.isNotBlank())
        val entry = synchronized(entries) { entries.getOrPut(key) { Entry() }.also { it.users++ } }
        try { return if (write) entry.gate.write(block) else entry.gate.read(block) }
        finally { synchronized(entries) { if (--entry.users == 0) entries.remove(key, entry) } }
    }
    suspend fun <T> read(key: String, block: suspend () -> T): T = use(key, false, block)
    suspend fun <T> write(key: String, block: suspend () -> T): T = use(key, true, block)
}
