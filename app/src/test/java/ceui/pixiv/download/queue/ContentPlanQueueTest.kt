package ceui.pixiv.download.queue

import android.app.Application
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ContentPlanQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "parent-queue-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    @Before fun setup() { runtime = ContentQueueRuntime(app, database); queue = ContentPlanQueue(runtime, 1) }
    @After fun teardown() { runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database) }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(12_000, block) }
    private suspend fun end(ticket: ContentPlanTicket) = withTimeout(8_000) { ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END } }
    private suspend fun eventually(test: () -> Boolean) { withTimeout(5_000) { while (!test()) delay(10) } }
    private fun register(body: suspend (ContentPlanContext) -> Unit) {
        queue.register(object : ContentPlanHandler {
            override val kind = "model"
            override suspend fun execute(context: ContentPlanContext) = body(context)
        })
    }
    private fun submit(family: String = "model-target", request: String = "{\"version\":1}") =
        queue.submit(family, "model") { request }

    @Test fun cancellationDuringAdmissionCannotBeLostOrStartAHandlerBeforePersistence() = scenario {
        val factoryEntered = CompletableDeferred<Unit>()
        val releaseFactory = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        register { context -> calls.incrementAndGet(); context.commit {} }
        queue.ready.await()
        val ticket = queue.submit("target", "model") { factoryEntered.complete(Unit); releaseFactory.await(); "{}" }
        factoryEntered.await()
        assertTrue(queue.store.activePage(0, Long.MAX_VALUE, 20).isEmpty()); assertEquals(0, calls.get())
        assertTrue(ticket.cancel()); releaseFactory.complete(Unit)
        assertEquals(State.CANCELLED, end(ticket).state)
        assertEquals(State.CANCELLED, queue.store.get(ticket.id).state)
        assertEquals(0, calls.get()); assertEquals(0L, runtime.store.highWaterMark())
    }

    @Test fun aCapacityOneParentCanRunNetworkThenCopyWithoutOwningEitherSlotWhileWaiting() = scenario {
        val phases = ContentPhaseRunner(runtime) { 1 }
        val calls = mutableListOf<String>()
        register { context ->
            assertEquals(State.RUNNING, queue.store.get(context.plan.id).state)
            phases.run("model", "download", "zip", Pool.NETWORK) { calls += "network" }
            context.checkpoint("{\"source\":\"preserved.zip\"}")
            phases.run("model", "install", "target", Pool.COPY) { context.commit { calls += "install" } }
        }
        val ticket = submit()
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(listOf("network", "install"), calls)
        val rows = runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 20)
        assertEquals(2, rows.size); assertTrue(rows.all { it.state == State.SUCCEEDED })
        assertEquals("{\"source\":\"preserved.zip\"}", queue.store.get(ticket.id).checkpoint)
    }

    @Test fun cancellationHoldsParentWidthAndPhysicalCapacityThroughDelayedCleanup() = scenario {
        val entered = CompletableDeferred<Unit>()
        val cleaning = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val phases = ContentPhaseRunner(runtime) { 1 }
        register {
            phases.run("model", "download", "zip", Pool.NETWORK) {
                try { entered.complete(Unit); awaitCancellation() }
                finally { withContext(NonCancellable) { cleaning.complete(Unit); withTimeout(5_000) { release.await() } } }
            }
        }
        val ticket = submit(); entered.await()
        assertTrue(ticket.cancel()); cleaning.await()
        val second = queue.store.admit("other-parent", "other-family", "model", "{}")
        assertNull(queue.store.claim(second.id, 1))
        assertEquals(State.CANCEL_REQUESTED, queue.store.get(ticket.id).state)
        val other = runtime.store.enqueue("other", "other", Pool.NETWORK, "x")
        assertNull(runtime.store.claim(other, Pool.NETWORK, 1))
        release.complete(Unit)
        assertEquals(State.CANCELLED, end(ticket).state)
        assertNotNull(queue.store.claim(second.id, 1)); assertNotNull(runtime.store.claim(other, Pool.NETWORK, 1))
    }

    @Test fun finalCommitCannotBeCancelledAndItsWidthRemainsReservedUntilOutputFinishes() = scenario {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        register { context -> context.commit { entered.complete(Unit); withTimeout(5_000) { release.await() } } }
        val ticket = submit(); entered.await()
        assertFalse(ticket.cancel()); assertFalse(ticket.pause())
        queue.store.admit("other", "other", "model", "{}")
        assertNull(queue.store.claim("other", 1))
        release.complete(Unit)
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertNotNull(queue.store.claim("other", 1))
    }

    @Test fun coldRecoveryReplaysTheSameParentCheckpointWithANewAttempt() = scenario {
        queue.ready.await(); queue.shutdown()
        val store = ContentPlanStore(runtime.store)
        store.admit("old-parent", "same-target", "model", "frozen-spec")
        val previous = store.claim("old-parent", 1)
        store.checkpoint(previous.id, previous.attempt, "owned-input")
        runtime.close(); runtime = ContentQueueRuntime(app, database); queue = ContentPlanQueue(runtime, 1)
        register { context ->
            assertEquals("old-parent", context.plan.id); assertEquals("frozen-spec", context.plan.request)
            assertEquals("owned-input", context.checkpoint); assertNotEquals(previous.attempt, context.plan.attempt)
            assertFalse(queue.store.checkpoint(previous.id, previous.attempt, "late"))
            context.commit {}
        }
        queue.ready.await()
        eventually { queue.ticket("old-parent") != null }
        assertEquals(State.SUCCEEDED, end(queue.ticket("old-parent")!!).state)
    }

    @Test fun rejectedParentInsertHasNoDirectFallbackAndDoesNotPoisonLaterAdmission() = scenario {
        val calls = AtomicInteger()
        register { context -> calls.incrementAndGet(); context.commit {} }
        queue.ready.await()
        runtime.store.writableDatabase.execSQL("CREATE TRIGGER reject_parent BEFORE INSERT ON plans BEGIN SELECT RAISE(ABORT,'blocked'); END")
        val rejected = submit()
        assertEquals(State.FAILED, end(rejected).state); assertEquals(0, calls.get())
        assertTrue(queue.store.activePage(0, Long.MAX_VALUE, 20).isEmpty())
        runtime.store.writableDatabase.execSQL("DROP TRIGGER reject_parent")
        assertEquals(State.SUCCEEDED, end(submit()).state); assertEquals(1, calls.get())
    }

    @Test fun retryReusesTheFailedCheckpointAndDoesNotImmediatelyObserveTheOldFailure() = scenario {
        val attempts = AtomicInteger()
        register { context ->
            if (attempts.incrementAndGet() == 1) { context.checkpoint("owned-input"); throw IOException("temporary output failure") }
            assertEquals("owned-input", context.checkpoint); context.commit {}
        }
        val initial = submit()
        assertEquals(State.FAILED, end(initial).state)
        val retry = submit()
        assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(initial.id, retry.id); assertEquals(2, attempts.get())
    }

    @Test fun pauseWaitsForExitAndAnExplicitResumeKeepsIdentityAndInput() = scenario {
        val entered = CompletableDeferred<Unit>()
        val attempts = AtomicInteger()
        register { context ->
            if (attempts.incrementAndGet() == 1) {
                context.checkpoint("preserved"); entered.complete(Unit); awaitCancellation()
            }
            assertEquals("preserved", context.checkpoint); context.commit {}
        }
        val ticket = submit(); entered.await()
        assertTrue(ticket.pause()); assertEquals(State.PAUSED, end(ticket).state)
        queue.resume(ticket.id)
        eventually { ticket.status.value.state != State.PAUSED }
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(2, attempts.get())
    }

    @Test fun anIneligibleParentWaitsWithoutTakingAPlanningOrPhysicalSlot() = scenario {
        val allowed = AtomicBoolean(false)
        val calls = AtomicInteger()
        queue.register(object : ContentPlanHandler {
            override val kind = "model"
            override fun canRun(plan: ContentPlanStore.Plan) = allowed.get()
            override suspend fun execute(context: ContentPlanContext) { calls.incrementAndGet(); context.commit {} }
        })
        val ticket = submit()
        eventually { queue.store.get(ticket.id)?.state == State.QUEUED }
        assertEquals(0, calls.get())
        queue.store.admit("other", "other", "unregistered", "{}")
        val held = queue.store.claim("other", 1)
        assertNotNull(held)
        assertTrue(queue.store.finish(held.id, held.attempt, true, null))
        allowed.set(true); queue.refreshEligibility()
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(1, calls.get())
    }

    @Test fun policyYieldRetainsCheckpointAndAutomaticallyRequeuesAfterAnEligibilityNotification() = scenario {
        val allowed = AtomicBoolean(true)
        val entered = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        queue.register(object : ContentPlanHandler {
            override val kind = "model"
            override fun canRun(plan: ContentPlanStore.Plan) = allowed.get()
            override suspend fun execute(context: ContentPlanContext) {
                if (calls.incrementAndGet() == 1) {
                    context.checkpoint("saved-input"); entered.complete(Unit); awaitCancellation()
                }
                assertEquals("saved-input", context.checkpoint); context.commit {}
            }
        })
        val ticket = submit(); entered.await()
        allowed.set(false); queue.refreshEligibility()
        eventually { ticket.status.value.state == State.QUEUED }
        assertEquals(1, calls.get()); assertEquals("saved-input", queue.store.get(ticket.id).checkpoint)
        allowed.set(true); queue.refreshEligibility()
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(2, calls.get())
    }

    @Test fun openingTheNetworkGateCannotOverrideAnExplicitPauseAfterPolicyYield() = scenario {
        val allowed = AtomicBoolean(true)
        val entered = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        queue.register(object : ContentPlanHandler {
            override val kind = "model"
            override fun canRun(plan: ContentPlanStore.Plan) = allowed.get()
            override suspend fun execute(context: ContentPlanContext) {
                if (calls.incrementAndGet() == 1) { entered.complete(Unit); awaitCancellation() }
                context.commit {}
            }
        })
        val ticket = submit(); entered.await()
        allowed.set(false); queue.refreshEligibility()
        eventually { ticket.status.value.state == State.QUEUED }
        assertTrue(ticket.pause()); assertEquals(State.PAUSED, end(ticket).state)
        allowed.set(true); queue.refreshEligibility()
        assertEquals(State.PAUSED, queue.store.get(ticket.id).state)
        assertEquals(1, calls.get())
        queue.resume(ticket.id)
        eventually { ticket.status.value.state != State.PAUSED }
        assertEquals(State.SUCCEEDED, end(ticket).state)
    }

    @Test fun parentNestingIsRejectedBeforeAdmissionInsteadOfDeadlockingTheActorOrPlanningWidth() = scenario {
        register { context ->
            queue.submit("nested-execution", "model") { "{}" }
            context.commit {}
        }
        val failedFactory = queue.submit("outer-factory", "model") {
            queue.submit("nested-factory", "model") { "{}" }; "{}"
        }
        assertEquals(State.FAILED, end(failedFactory).state)
        assertEquals(0L, queue.store.highWaterMark())
        assertEquals(State.FAILED, end(submit("outer-execution")).state)
        assertEquals(1L, queue.store.highWaterMark())
        assertEquals(0L, runtime.store.highWaterMark())
    }
}
