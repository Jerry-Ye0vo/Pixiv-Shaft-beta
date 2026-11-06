package ceui.pixiv.download.queue

import android.content.Context
import ceui.lisa.activities.Shaft
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.IOException
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

/** Persisted physical stages of a parent-owned pipeline. The parent owns replay and output receipts.
 * A stage never reserves one pool while waiting for another: acquire/release around actual work only.
 * The caller must check an existing artifact again inside [run] before performing side effects.
 */
class ContentPhaseRunner(
    private val runtime: ContentQueueRuntime,
    private val capacity: (Pool) -> Int,
) {
    data class Intent(val version: Int = 1, val owner: String, val phase: String, val resource: String,
                      val context: String, val submittedAt: Long = System.currentTimeMillis())

    suspend fun <T> run(owner: String, phase: String, resource: String, pool: Pool,
                        context: String = "{}", block: suspend () -> T): T = withContext(Dispatchers.IO) {
        require(owner.isNotBlank() && phase.isNotBlank() && resource.isNotBlank())
        check(currentCoroutineContext()[HeldPhase] == null) { "A queued stage cannot await another stage while holding capacity" }
        val id = PREFIX + UUID.randomUUID()
        var admitted = false
        var lease: ContentQueueStore.Task? = null
        var committed = false
        var failure: Throwable? = null
        try {
            runtime.initialize()
            runtime.store.admit(id, id, resource, pool, Gson().toJson(Intent(owner = owner,
                phase = phase, resource = resource, context = context)), State.QUEUED, false)
            admitted = true
            runtime.changed()
            while (lease == null) {
                currentCoroutineContext().ensureActive()
                val signal = runtime.changes.value
                val state = runtime.store.get(id)?.state ?: throw IOException("Queued stage disappeared")
                if (state == State.CANCELLED || state == State.CANCEL_REQUESTED) throw CancellationException("Stage withdrawn")
                if (state == State.QUEUED) lease = runtime.store.claim(id, pool, capacity(pool).coerceIn(1, 5))
                if (lease == null) runtime.changes.first { it != signal }
            }
            val result = withContext(HeldPhase(pool)) { currentCoroutineContext().ensureActive(); block() }
            // Do not insert a cancellation point between a successful physical commit and its receipt.
            withContext(NonCancellable) {
                check(runtime.store.markCommitted(id, lease!!.attempt)) { "Stale stage commit" }
                committed = true
            }
            result
        } catch (error: Throwable) { failure = error; throw error }
        finally {
            withContext(NonCancellable + Dispatchers.IO) {
                try {
                    if (admitted) {
                        val current = lease
                        if (failure is CancellationException && !committed) runtime.store.cancel(id)
                        if (current != null) check(runtime.store.finish(id, current.attempt, committed,
                            failure?.javaClass?.simpleName)) { "Stale stage acknowledgement" }
                        // A failed/cancelled body is already closed before this acknowledgement.
                    }
                } catch (acknowledgement: Throwable) {
                    // A failed acknowledgement retains the live SQLite slot until process recovery.
                    val original = failure
                    if (original == null) throw acknowledgement
                    if (original !== acknowledgement) original.addSuppressed(acknowledgement)
                } finally { runtime.changed() }
            }
        }
    }

    private class HeldPhase(val pool: Pool) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<HeldPhase>
    }
    companion object { const val PREFIX = "phase:" }
}

/** Adapters share the existing process recovery owner and SQLite permit authority. */
object ContentPhases {
    fun runner(context: Context): ContentPhaseRunner = ContentPhaseRunner(ContentQueueRuntime.get(context)) { pool ->
        if (pool == Pool.ENCODE) 1 else Shaft.sSettings?.maxConcurrentDownloads ?: 1
    }
    suspend fun <T> run(owner: String, phase: String, resource: String, pool: Pool,
                        context: String = "{}", block: suspend () -> T): T =
        runner(Shaft.getContext()).run(owner, phase, resource, pool, context, block)
}
