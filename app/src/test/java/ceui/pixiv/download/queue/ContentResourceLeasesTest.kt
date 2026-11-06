package ceui.pixiv.download.queue

import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test

class ContentResourceLeasesTest {
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(6_000, body) }
    @Test fun aWriterCanCallNestedWritersAndReadersWithoutWaitingForItself() = scenario {
        val key = "nested"
        ContentResourceLeases.write(key) {
            assertEquals(42, ContentResourceLeases.read(key) { ContentResourceLeases.write(key) { 42 } })
        }
        assertEquals(1, ContentResourceLeases.write(key) { 1 })
    }
    @Test fun independentDirectoriesDoNotBlockEachOtherWhileAnExportHoldsAReadLease() = scenario {
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val replaced = CompletableDeferred<Unit>()
        val reader = launch { ContentResourceLeases.read("first") { entered.complete(Unit); release.await() } }; entered.await()
        val writer = launch(start = CoroutineStart.UNDISPATCHED) { ContentResourceLeases.write("first") { replaced.complete(Unit) } }
        assertEquals(2, ContentResourceLeases.write("second") { 2 }); assertFalse(replaced.isCompleted)
        release.complete(Unit); reader.join(); writer.join(); assertTrue(replaced.isCompleted)
    }
    @Test fun acquiringAnotherDirectoryDoesNotHideAnOuterWriterLease() = scenario {
        ContentResourceLeases.write("outer") { ContentResourceLeases.write("inner") {
            assertEquals(7, ContentResourceLeases.read("outer") { 7 })
        } }
    }
    @Test fun anOuterReadStillCannotBeUpgradedAfterReadingAnotherDirectory() = scenario {
        ContentResourceLeases.read("outer") { ContentResourceLeases.read("inner") {
            try { ContentResourceLeases.write("outer") { fail("Cannot upgrade a held source") }; fail("Upgrade must fail") }
            catch (expected: IllegalStateException) { }
        } }
    }
    @Test fun legacyNonblockingReadsDeferWhileClearIsWaitingAndTransferredTokensCloseOnlyOnce() = scenario {
        val gate = ContentResourceGate(); val token = gate.acquireRead(); val entered = CompletableDeferred<Unit>()
        val writer = launch(start = CoroutineStart.UNDISPATCHED) { gate.clear { entered.complete(Unit) } }
        assertNull(gate.tryRead()); assertFalse(entered.isCompleted)
        token.close(); token.close(); writer.join(); assertTrue(entered.isCompleted)
        val next = requireNotNull(gate.tryRead()); next.close()
        gate.clear {}
    }
}
