package ceui.pixiv.download.copy

import java.io.Closeable
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

/** Main-safe input reservations. Map critical sections contain no filesystem work. */
object CopyInputReferences {
    private class Entry(val file: File) {
        val users = AtomicInteger()
        val deleteRequested = AtomicBoolean()
    }
    private val entries = ConcurrentHashMap<String, Entry>()
    private val io = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    fun reserve(file: File): Closeable {
        val key = file.absolutePath
        var reserved: Entry? = null
        entries.compute(key) { _, old ->
            val entry = old ?: Entry(file.absoluteFile)
            check(entry.users.get() >= 0) { "Copy input is already being removed" }
            entry.users.incrementAndGet()
            reserved = entry
            entry
        }
        val entry = requireNotNull(reserved)
        val closed = AtomicBoolean()
        return Closeable {
            if (closed.compareAndSet(false, true)) {
                var delete = false
                entries.computeIfPresent(key) { _, current ->
                    check(current === entry)
                    val remaining = current.users.decrementAndGet()
                    check(remaining >= 0)
                    if (remaining == 0) {
                        delete = current.deleteRequested.get()
                        if (delete) current else null
                    } else current
                }
                if (delete) scheduleDelete(key, entry)
            }
        }
    }

    fun requestDelete(file: File): kotlinx.coroutines.Job {
        val key = file.absolutePath
        var pending: Entry? = null
        entries.compute(key) { _, old ->
            (old ?: Entry(file.absoluteFile)).also { it.deleteRequested.set(true); pending = it }
        }
        return scheduleDelete(key, requireNotNull(pending))
    }

    private fun scheduleDelete(key: String, entry: Entry): kotlinx.coroutines.Job = io.launch {
            var delete = false
            entries.computeIfPresent(key) { _, current ->
                if (current === entry && current.users.compareAndSet(0, -1)) delete = true
                current
            }
            if (!delete) return@launch
            try { entry.file.delete() }
            finally { entries.remove(key, entry) }
        }
}
