package ceui.pixiv.ui.bulk

import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test

class UgoiraCacheGateTest {
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(6_000, body) }
    @Test fun simultaneousPipelinesCanReadWhileClearWaitsForTheirActualExit() = scenario {
        val gate = UgoiraCacheGate(); val first = CompletableDeferred<Unit>(); val second = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>(); val cleared = CompletableDeferred<Unit>()
        val a = launch { gate.read { first.complete(Unit); release.await() } }
        val b = launch { gate.read { second.complete(Unit); release.await() } }
        first.await(); second.await()
        val clear = launch(start = CoroutineStart.UNDISPATCHED) { gate.clear { cleared.complete(Unit) } }
        assertFalse(cleared.isCompleted); release.complete(Unit); a.join(); b.join(); clear.join(); assertTrue(cleared.isCompleted)
    }
    @Test fun aWaitingClearPreventsNewReadersFromOvertakingIt() = scenario {
        val gate = UgoiraCacheGate(); val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val order = mutableListOf<String>()
        val reader = launch { gate.read { entered.complete(Unit); release.await() } }; entered.await()
        val clear = launch(start = CoroutineStart.UNDISPATCHED) { gate.clear { order += "clear" } }
        val next = launch(start = CoroutineStart.UNDISPATCHED) { gate.read { order += "next" } }
        assertTrue(order.isEmpty()); release.complete(Unit); reader.join(); clear.join(); next.join()
        assertEquals(listOf("clear", "next"), order)
    }
    @Test fun cancelledReadersKeepTheirReferenceUntilTheirCleanupFinishes() = scenario {
        val gate = UgoiraCacheGate(); val entered = CompletableDeferred<Unit>(); val cleaning = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>(); val cleared = CompletableDeferred<Unit>()
        val reader = launch { gate.read {
            try { entered.complete(Unit); awaitCancellation() }
            finally { withContext(NonCancellable) { cleaning.complete(Unit); release.await() } }
        } }; entered.await(); reader.cancel(); cleaning.await()
        val clear = launch(start = CoroutineStart.UNDISPATCHED) { gate.clear { cleared.complete(Unit) } }
        assertFalse(cleared.isCompleted); release.complete(Unit); reader.join(); clear.join(); assertTrue(cleared.isCompleted)
    }
    @Test fun cancellingAWaitingClearReleasesItsAdmissionFence() = scenario {
        val gate = UgoiraCacheGate(); val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val reader = launch { gate.read { entered.complete(Unit); release.await() } }; entered.await()
        val clear = launch(start = CoroutineStart.UNDISPATCHED) { gate.clear { fail("Cancelled clear must not delete") } }
        clear.cancelAndJoin(); assertEquals("next", gate.read { "next" }); release.complete(Unit); reader.join()
    }
    @Test fun failedDeletionAlwaysReopensReaders() = scenario {
        val gate = UgoiraCacheGate()
        try { gate.clear { throw java.io.IOException("storage") }; fail("Expected delete error") } catch (expected: java.io.IOException) { }
        assertEquals("read", gate.read { "read" })
    }
    @Test fun nestedReadDoesNotWaitBehindAWriterWhichIsWaitingForItsOuterRead() = scenario {
        val gate = UgoiraCacheGate(); val entered = CompletableDeferred<Unit>(); val nested = CompletableDeferred<Unit>()
        val reader = launch { gate.read { entered.complete(Unit); nested.await(); assertEquals(1, gate.read { 1 }) } }
        entered.await(); val clear = launch(start = CoroutineStart.UNDISPATCHED) { gate.clear {} }
        nested.complete(Unit); reader.join(); clear.join()
    }
    @Test fun clearingInsideAReadFailsPromptlyInsteadOfDeadlocking() = scenario {
        val gate = UgoiraCacheGate()
        gate.read { try { gate.clear {}; fail("Cannot delete a leased input") } catch (expected: IllegalStateException) { } }
        gate.clear {}
    }
}
