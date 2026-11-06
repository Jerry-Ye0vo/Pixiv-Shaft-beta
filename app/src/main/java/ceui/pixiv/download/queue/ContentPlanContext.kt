package ceui.pixiv.download.queue

import java.io.IOException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

interface ContentPlanHandler {
    val kind: String
    /** Group coordinators may await leaf parents; they use a separate planning lane. */
    val isGroup: Boolean get() = false
    fun onUserResume(plan: ContentPlanStore.Plan) {}
    fun priority(plan: ContentPlanStore.Plan): Int = ContentQueueRuntime.SAVE
    /** Cheap admission predicate; a policy change must call refreshEligibility, not poll per plan. */
    fun canRun(plan: ContentPlanStore.Plan): Boolean = true
    suspend fun execute(context: ContentPlanContext)
    /** Only remove inputs owned by this plan; completed output and old library records are separate. */
    suspend fun cleanup(plan: ContentPlanStore.Plan, state: ContentQueueStore.State) {}
}

/** Inherited by child stages and source consumers, without retaining an Activity or task request. */
class ContentPriority(val value: Int) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
    companion object Key : kotlin.coroutines.CoroutineContext.Key<ContentPriority>
}

class ContentPlanContext internal constructor(
    val plan: ContentPlanStore.Plan,
    private val ticket: ContentPlanTicket,
    private val store: ContentPlanStore,
    private val changed: () -> Unit,
) {
    @Volatile internal var outputCommitted = false
    var checkpoint: String = plan.checkpoint
        private set
    private val progressLock = Any()
    private var lastPhase = ""
    private var lastReport = 0L
    fun checkStop() { ticket.checkStop() }
    fun yieldForPolicy(): Nothing {
        ticket.yieldForPolicy()
        throw kotlinx.coroutines.CancellationException("Parent waiting for policy eligibility")
    }
    fun progress(phase: String, bytes: Long = 0, total: Long = -1) {
        val now = System.nanoTime()
        val accepted = synchronized(progressLock) {
            if (phase != lastPhase || now - lastReport >= 100_000_000L || (total >= 0 && bytes >= total)) {
                lastPhase = phase; lastReport = now; true
            } else false
        }
        if (!accepted) return
        ticket.progress(phase, bytes, total); changed()
    }
    /** Nonce-guarded IO checkpoint. Call before opening a resumable provider output. */
    suspend fun checkpoint(value: String) = withContext(Dispatchers.IO) {
        currentCoroutineContext().ensureActive(); ticket.checkStop()
        if (!store.checkpoint(plan.id, plan.attempt, value)) throw IOException("Stale parent checkpoint")
        checkpoint = value
    }
    /** Make the smallest final output transaction non-interruptible, not the dependency pipeline. */
    suspend fun commit(output: suspend () -> Unit) {
        currentCoroutineContext().ensureActive()
        withContext(NonCancellable + Dispatchers.IO) {
            ticket.beginCommit()
            output()
            outputCommitted = true // prompt cancellation on delivery cannot undo installed output
            if (!store.markCommitted(plan.id, plan.attempt)) throw IOException("Stale parent commit")
        }
    }
}
