package ceui.pixiv.download.copy

import ceui.pixiv.download.queue.ContentQueueStore.State
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

/** Non-blocking user command gate. An accepted cancellation always precedes output opening. */
class CopyTicket internal constructor(val id: String, val family: String) {
    private val gate = AtomicInteger(OPEN)
    private val mutable = MutableStateFlow(CopyStatus(id, null, null))
    val status: StateFlow<CopyStatus> = mutable
    @Volatile private var delegate: CopyTicket? = null
    internal var intent: CopyIntent? = null
    internal var sequence: Long = 0
    internal var signal: () -> Unit = {}

    @Synchronized fun cancel(): Boolean {
        delegate?.let { return it.cancel() }
        while (true) {
            val current = gate.get()
            if (current == CANCELLED || current == COMMITTING) return false
            if (gate.compareAndSet(current, CANCELLED)) break
        }
        signal()
        return true
    }
    @Synchronized fun pause(): Boolean {
        delegate?.let { return it.pause() }
        if (!gate.compareAndSet(OPEN, PAUSED)) return false
        signal(); return true
    }
    internal fun checkCancellation() {
        if (gate.get() == CANCELLED || gate.get() == PAUSED) throw CancellationException("Copy stopped before output writing")
    }
    internal fun beginWrite() {
        if (!gate.compareAndSet(OPEN, COMMITTING)) throw CancellationException("Copy cancelled before output writing")
    }
    internal fun retry() { gate.set(OPEN) }
    internal fun publish(state: State?, error: String? = null, progress: Int = 0) {
        mutable.value = CopyStatus(id, intent?.request, state, error, progress, sequence)
    }
    @Synchronized internal fun bind(other: CopyTicket, scope: CoroutineScope) {
        if (gate.get() == CANCELLED) other.cancel()
        if (gate.get() == PAUSED) other.pause()
        delegate = other
        mutable.value = other.status.value
        scope.launch {
            other.status.first {
                mutable.value = it
                it.state in setOf(State.SUCCEEDED, State.CANCELLED, State.FAILED)
            }
        }
    }
    internal fun isCancelled() = gate.get() == CANCELLED
    internal fun isPaused() = gate.get() == PAUSED
    companion object { private const val OPEN = 0; private const val CANCELLED = 1; private const val COMMITTING = 2; private const val PAUSED = 3 }
}

data class CopyStatus(
    val id: String,
    val request: CopyRequest?,
    val state: State?,
    val error: String? = null,
    val progress: Int = 0,
    val sequence: Long = 0,
)
