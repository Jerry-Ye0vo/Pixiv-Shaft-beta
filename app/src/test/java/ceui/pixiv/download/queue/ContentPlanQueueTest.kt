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

    @Test fun aGroupCanAwaitAChildAtWidthOneWithoutPermittingRecursiveGroups() = scenario {
        register { context -> context.commit {} }
        queue.register(object : ContentPlanHandler {
            override val kind = "test-group"
            override val isGroup = true
            override suspend fun execute(context: ContentPlanContext) {
                assertThrows(IllegalStateException::class.java) { queue.submit("recursive", kind) { "{}" } }
                val child = submit("owned-leaf")
                assertEquals(State.SUCCEEDED, end(child).state)
                context.commit {}
            }
        })
        val ticket = queue.submit("group", "test-group") { "{}" }
        assertEquals(State.SUCCEEDED, end(ticket).state)
    }

    @Test fun explicitResumeCanReleaseOnlyTheSelectedPolicyBlockedQueuedParent() = scenario {
        val allowed = mutableSetOf<String>()
        queue.register(object : ContentPlanHandler {
            override val kind = "policy"
            override fun canRun(plan: ContentPlanStore.Plan) = synchronized(allowed) { plan.id in allowed }
            override fun onUserResume(plan: ContentPlanStore.Plan) { synchronized(allowed) { allowed += plan.id } }
            override suspend fun execute(context: ContentPlanContext) { context.commit {} }
        })
        val first = queue.submit("first", "policy") { "{}" }
        val second = queue.submit("second", "policy") { "{}" }
        eventually { queue.store.get(first.id) != null && queue.store.get(second.id) != null }
        queue.resume(first.id)
        assertEquals(State.SUCCEEDED, end(first).state)
        assertEquals(State.QUEUED, queue.store.get(second.id).state)
    }

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

    @Test fun pausingBeforeAdmissionPersistsTheIntentInsteadOfDiscardingItsResumeRecord() = scenario {
        val calls = AtomicInteger(); val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        register { context -> if (context.plan.family == "pause-before-admit") calls.incrementAndGet(); context.commit {} }
        val blocking = queue.submit("busy-factory", "model") { entered.complete(Unit); release.await(); "busy" }
        entered.await()
        val ticket = queue.submit("pause-before-admit", "model") { "frozen" }
        assertTrue(ticket.pause()); release.complete(Unit)
        assertEquals(State.PAUSED, end(ticket).state); assertEquals("frozen", queue.store.get(ticket.id).request)
        assertEquals(0, calls.get()); queue.resume(ticket.id)
        ticket.status.first { it.state == State.SUCCEEDED }; assertEquals(1, calls.get())
        assertEquals(State.SUCCEEDED, end(blocking).state)
    }

    @Test fun migratedFailedAndPausedStatesDoNotStartAWorkerBeforeManualResume() = scenario {
        val calls = AtomicInteger(); register { context -> calls.incrementAndGet(); context.commit {} }
        val failed = queue.submit("failed-legacy", "model", resumeStopped = false, initialState = State.FAILED) { "failed" }
        val paused = queue.submit("paused-legacy", "model", resumeStopped = false, initialState = State.PAUSED) { "paused" }
        assertEquals(State.FAILED, end(failed).state); assertEquals(State.PAUSED, end(paused).state)
        assertEquals(0, calls.get()); assertEquals(0L, runtime.store.highWaterMark())
        queue.resume(failed.id); failed.status.first { it.state == State.SUCCEEDED }; assertEquals(1, calls.get())
    }

    @Test fun legacyJournalReplayKeepsTheExistingFailedPausedOrCancelledParentAndItsFrozenRequest() = scenario {
        register { context -> context.commit {} }
        val first = queue.submit("legacy-stable", "model", resumeStopped = false, initialState = State.FAILED) { "original" }
        assertEquals(State.FAILED, end(first).state)
        val replay = queue.submit("legacy-stable", "model", reusePersisted = true) { error("Cannot refreeze an existing legacy intention") }
        assertEquals(State.FAILED, end(replay).state); assertEquals(first.id, replay.id)
        assertTrue(first.cancel()); first.status.first { it.state == State.CANCELLED }
        val withdrawn = queue.submit("legacy-stable", "model", reusePersisted = true) { "different" }
        assertEquals(State.CANCELLED, end(withdrawn).state); assertEquals(first.id, withdrawn.id)
        assertEquals(1L, queue.store.highWaterMark())
    }

    @Test fun aReplayedGroupRowCanReuseItsCommittedChildBeforeTheGroupAcknowledgement() = scenario {
        val calls = AtomicInteger()
        register { context -> calls.incrementAndGet(); context.commit {} }
        val first = queue.submit("batch-row:123", "model", reuseSucceeded = true) { "original" }
        assertEquals(State.SUCCEEDED, end(first).state)
        val replay = queue.submit("batch-row:123", "model", reuseSucceeded = true) { error("Already fulfilled row must not re-freeze settings") }
        assertEquals(State.SUCCEEDED, end(replay).state); assertEquals(first.id, replay.id); assertEquals(1, calls.get())
        assertEquals(1L, queue.store.highWaterMark())
    }

    @Test fun explicitNewIntentionsStillExecuteAfterAnEarlierSuccessInTheSameFamily() = scenario {
        val calls = AtomicInteger()
        register { context -> calls.incrementAndGet(); context.commit {} }
        val first = submit(); assertEquals(State.SUCCEEDED, end(first).state)
        val next = submit(); assertEquals(State.SUCCEEDED, end(next).state)
        assertNotEquals(first.id, next.id); assertEquals(2, calls.get())
    }

    @Test fun aDurableDeleteSuccessorWaitsForAnAlreadyCommittingInstallation() = scenario {
        val committing = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        register { context ->
            if (calls.incrementAndGet() == 1) context.commit {
                committing.complete(Unit); withTimeout(5_000) { release.await() }
            } else context.commit {}
        }
        val first = submit(); committing.await()
        val next = queue.submit(first.family, "model", replaceActive = true) { "delete" }
        eventually { queue.store.get(next.id)?.state == State.QUEUED }
        assertFalse(first.cancel()); assertNotEquals(first.id, next.id); assertEquals(1, calls.get())
        release.complete(Unit)
        assertEquals(State.SUCCEEDED, end(first).state)
        assertEquals(State.SUCCEEDED, end(next).state); assertEquals(2, calls.get())
    }

    @Test fun aSuccessorCanBeAdmittedDuringCancelButWaitsForThePredecessorsHandlerCleanup() = scenario {
        val entered = CompletableDeferred<Unit>()
        val cleaning = CompletableDeferred<Unit>()
        val releaseCleanup = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        queue.register(object : ContentPlanHandler {
            override val kind = "model"
            override suspend fun execute(context: ContentPlanContext) {
                if (calls.incrementAndGet() == 1) { entered.complete(Unit); awaitCancellation() }
                context.commit {}
            }
            override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
                if (state == State.CANCELLED) {
                    cleaning.complete(Unit)
                    withTimeout(5_000) { releaseCleanup.await() }
                }
            }
        })
        val first = submit(); entered.await()
        assertTrue(first.cancel())
        val next = submit()
        cleaning.await()
        eventually { queue.store.get(next.id)?.state == State.QUEUED }
        assertNotEquals(first.id, next.id)
        assertEquals(State.CANCEL_REQUESTED, queue.store.get(first.id).state)
        assertEquals(1, calls.get())
        releaseCleanup.complete(Unit)
        assertEquals(State.CANCELLED, end(first).state)
        assertEquals(State.SUCCEEDED, end(next).state)
        assertEquals(2, calls.get())
    }
}
