package ceui.pixiv.download.queue

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import ceui.pixiv.download.queue.ContentQueueStore.State

data class ContentPlanStatus(val id: String, val family: String, val kind: String, val state: State?,
    val phase: String = "queued", val bytes: Long = 0, val total: Long = -1, val error: String? = null)

/** Commands are memory-only on Main; a physical commit wins once beginCommit has succeeded. */
class ContentPlanTicket internal constructor(private val reservationId: String, val family: String, val kind: String) {
    val id: String get() = delegate?.id ?: reservationId
    private val gate = AtomicInteger(OPEN)
    private val mutable = MutableStateFlow(ContentPlanStatus(reservationId, family, kind, null))
    val status: StateFlow<ContentPlanStatus> = mutable
    @Volatile private var delegate: ContentPlanTicket? = null
    internal var admitted = false
    internal var sequence = 0L
    internal var signal: () -> Unit = {}

    @Synchronized fun pause(): Boolean {
        delegate?.let { return it.pause() }
        while (true) {
            val current = gate.get()
            if (current != OPEN && current != YIELD) return false
            if (gate.compareAndSet(current, PAUSE)) { signal(); return true }
        }
    }
    @Synchronized fun cancel(): Boolean {
        delegate?.let { return it.cancel() }
        while (true) {
            val current = gate.get()
            if (current == COMMITTING || current == CANCEL) return false
            if (gate.compareAndSet(current, CANCEL)) { signal(); return true }
        }
    }
    internal fun yieldForPolicy(): Boolean {
        if (!gate.compareAndSet(OPEN, YIELD)) return false
        signal(); return true
    }
    internal fun stop(): Int = gate.get().takeIf { it == PAUSE || it == CANCEL || it == YIELD } ?: OPEN
    internal fun checkStop() { if (stop() != OPEN) throw CancellationException("Parent stopped before commit") }
    internal fun beginCommit() {
        if (!gate.compareAndSet(OPEN, COMMITTING)) throw CancellationException("Parent stopped before commit")
    }
    internal fun reset(state: State) {
        gate.set(when (state) { State.SUCCEEDED, State.COMMITTED -> COMMITTING; State.CANCELLED -> CANCEL; State.PAUSED -> PAUSE; else -> OPEN })
    }
    /** Acknowledgement must never overwrite a concurrently accepted stop command. */
    internal fun finished(state: State) {
        when (state) {
            State.FAILED -> gate.compareAndSet(COMMITTING, OPEN)
            State.QUEUED -> gate.compareAndSet(YIELD, OPEN)
            State.PAUSED -> {
                gate.compareAndSet(OPEN, PAUSE)
                gate.compareAndSet(YIELD, PAUSE)
            }
            State.CANCELLED -> {
                while (true) {
                    val current = gate.get()
                    if (current == CANCEL || current == COMMITTING) break
                    if (gate.compareAndSet(current, CANCEL)) break
                }
            }
            State.SUCCEEDED -> gate.compareAndSet(OPEN, COMMITTING)
            else -> Unit
        }
    }
    @Synchronized internal fun reopen(): Boolean {
        if (gate.get() == CANCEL || gate.get() == COMMITTING) return false
        gate.set(OPEN); return true
    }
    internal fun publish(state: State?, error: String? = null) {
        mutable.value = mutable.value.copy(state = state, error = error)
    }
    internal fun progress(phase: String, bytes: Long, total: Long) {
        mutable.value = mutable.value.copy(phase = phase, bytes = bytes, total = total)
    }
    @Synchronized internal fun bind(other: ContentPlanTicket, scope: CoroutineScope) {
        when (stop()) { CANCEL -> other.cancel(); PAUSE -> other.pause(); YIELD -> other.yieldForPolicy() }
        delegate = other
        mutable.value = other.status.value
        scope.launch {
            other.status.first { mutable.value = it; it.state in OBSERVABLE_END }
        }
    }
    companion object {
        internal const val OPEN = 0; internal const val PAUSE = 1; internal const val CANCEL = 2
        internal const val YIELD = 4
        private const val COMMITTING = 3
        internal val OBSERVABLE_END = setOf(State.PAUSED, State.CANCELLED, State.SUCCEEDED, State.FAILED)
    }
}
