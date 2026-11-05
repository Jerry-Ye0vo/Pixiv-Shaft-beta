package ceui.pixiv.download.queue

import android.app.Application
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ContentQueueStoreTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private lateinit var name: String
    private lateinit var store: ContentQueueStore
    @Before fun setup() {
        name = "queue-${UUID.randomUUID()}.db"
        store = ContentQueueStore(app, name)
    }
    @After fun cleanup() { store.close(); app.deleteDatabase(name) }
    private fun enqueue(key: String, resource: String = key, pool: Pool = Pool.NETWORK) =
        store.enqueue(key, resource, pool, "{\"page\":1}")

    @Test fun enqueueCommitsBeforeReturningAndSurvivesReopen() {
        val id = enqueue("save")
        assertEquals(State.QUEUED, store.get(id).state)
        store.close()
        store = ContentQueueStore(app, name)
        assertEquals(id, enqueue("save"))
        assertEquals("{\"page\":1}", store.get(id).payload)
    }

    @Test fun simultaneousEnqueuesDeduplicateAndClaimsNeverExceedCapacity() {
        store.writableDatabase // initialize schema before contenders
        val executor = Executors.newFixedThreadPool(8)
        try {
            val ids = executor.invokeAll((0..19).map { Callable { enqueue("same") } })
                .map { it.get(10, TimeUnit.SECONDS) }
            assertEquals(1, ids.toSet().size)
            repeat(10) { enqueue("other-$it") }
            val claims = executor.invokeAll((0..19).map { Callable { store.claim(Pool.NETWORK, 3) } })
                .mapNotNull { it.get(10, TimeUnit.SECONDS) }
            assertEquals(3, claims.size)
            assertEquals(3, claims.map { it.id }.toSet().size)
        } finally { executor.shutdownNow() }
    }

    @Test fun distinctStoreHandlesShareCapacityAndDoNotResetLiveWorkers() {
        enqueue("one"); enqueue("two")
        val first = store.claim(Pool.NETWORK, 1)
        ContentQueueStore(app, name).use { other ->
            assertNull(other.claim(Pool.NETWORK, 1))
            assertEquals(first.attempt, other.get(first.id).attempt)
        }
    }

    @Test fun stoppingWorkerOwnsSlotUntilAcknowledgedAndOldAttemptsAreIgnored() {
        val id = enqueue("one")
        val first = store.claim(Pool.NETWORK, 1)
        store.pause(id)
        assertFalse(store.resume(id))
        assertNull(store.claim(Pool.NETWORK, 1))
        assertTrue(store.finish(id, first.attempt, false, null))
        assertEquals(State.PAUSED, store.get(id).state)
        assertTrue(store.resume(id))
        val second = store.claim(Pool.NETWORK, 1)
        assertNotEquals(first.attempt, second.attempt)
        assertFalse(store.finish(id, first.attempt, true, null))
        assertEquals(State.RUNNING, store.get(id).state)
        store.cancel(id)
        store.pause(id)
        assertEquals(State.CANCEL_REQUESTED, store.get(id).state)
        assertTrue(store.finish(id, second.attempt, false, null))
        assertEquals(State.CANCELLED, store.get(id).state)
    }

    @Test fun sharedResourceCannotRunTwiceButSeparatePoolsCanMakeProgress() {
        enqueue("gallery", "original")
        val first = store.claim(Pool.NETWORK, 2)
        val snapshot = enqueue("snapshot", "original")
        assertNull(store.claim(Pool.NETWORK, 2))
        val copy = enqueue("local-copy", "other-file", Pool.COPY)
        assertEquals(copy, store.claim(Pool.COPY, 1).id)
        assertTrue(store.finish(first.id, first.attempt, true, null))
        assertEquals(snapshot, store.claim(Pool.NETWORK, 2).id)
    }

    @Test fun restartRecoversRunningWorkButPreservesStopIntentAndInvalidatesTokens() {
        val running = enqueue("run")
        val old = store.claim(Pool.NETWORK, 3)
        val paused = enqueue("pause")
        store.claim(Pool.NETWORK, 3); store.pause(paused)
        val cancelled = enqueue("cancel")
        store.claim(Pool.NETWORK, 3); store.cancel(cancelled)
        store.close(); store = ContentQueueStore(app, name)
        store.recoverAfterProcessDeath()
        store.recoverAfterProcessDeath() // recovery is idempotent
        assertEquals(State.QUEUED, store.get(running).state)
        assertEquals(State.PAUSED, store.get(paused).state)
        assertEquals(State.CANCELLED, store.get(cancelled).state)
        assertFalse(store.finish(running, old.attempt, true, null))
        assertNotEquals(old.attempt, store.claim(Pool.NETWORK, 1).attempt)
    }

    @Test fun committedOutputWinsOverLateCancellation() {
        val id = enqueue("save")
        val attempt = store.claim(Pool.NETWORK, 1).attempt
        store.cancel(id)
        assertTrue(store.finish(id, attempt, true, null))
        assertEquals(State.SUCCEEDED, store.get(id).state)
        assertFalse(store.resume(id))
    }

    @Test fun failedAttemptKeepsItsErrorUntilExplicitRetry() {
        val id = enqueue("failure")
        val attempt = store.claim(Pool.NETWORK, 1).attempt
        assertTrue(store.finish(id, attempt, false, "HTTP 503"))
        assertEquals("HTTP 503", store.get(id).error)
        assertNull(store.claim(Pool.NETWORK, 1))
        assertEquals(id, enqueue("failure"))
        assertTrue(store.resume(id))
        assertNull(store.get(id).error)
        assertEquals(id, store.claim(Pool.NETWORK, 1).id)
    }

    @Test fun newIntentDuringCancellationWaitsForOldWorkerToExit() {
        val old = enqueue("same-output", "same-resource")
        val attempt = store.claim(Pool.NETWORK, 2).attempt
        store.cancel(old)
        val fresh = enqueue("same-output", "same-resource")
        assertNotEquals(old, fresh)
        assertNull(store.claim(Pool.NETWORK, 2))
        assertTrue(store.finish(old, attempt, false, null))
        assertEquals(fresh, store.claim(Pool.NETWORK, 2).id)
    }

    @Test fun failedTransactionCannotCreatePhantomQueuedTask() {
        store.writableDatabase.execSQL("CREATE TRIGGER fail_insert BEFORE INSERT ON tasks BEGIN SELECT RAISE(ABORT, 'disk write rejected'); END")
        try { enqueue("failed"); fail("enqueue must report persistence failure") }
        catch (expected: android.database.sqlite.SQLiteException) { /* durable enqueue failed */ }
        store.readableDatabase.rawQuery("SELECT COUNT(*) FROM tasks", null).use {
            assertTrue(it.moveToFirst()); assertEquals(0, it.getInt(0))
        }
        assertNull(store.claim(Pool.NETWORK, 1))
    }
}
