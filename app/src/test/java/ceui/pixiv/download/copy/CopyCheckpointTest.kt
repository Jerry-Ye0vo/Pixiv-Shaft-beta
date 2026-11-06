package ceui.pixiv.download.copy

import android.app.Application
import ceui.pixiv.download.queue.ContentQueueStore
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class CopyCheckpointTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "checkpoint-${UUID.randomUUID()}.db"
    private lateinit var store: ContentQueueStore
    @Before fun setup() { store = ContentQueueStore(app, name) }
    @After fun cleanup() { store.close(); app.deleteDatabase(name) }
    @Test fun staleInputOrReceiptCannotReplaceTheCurrentAttemptCheckpoint() {
        val id = store.enqueue("output", "input", Pool.COPY, "initial")
        val old = store.claim(id, Pool.COPY, 1)
        assertTrue(store.checkpoint(id, old.attempt, "old receipt"))
        store.pause(id); store.finish(id, old.attempt, false, null); store.resume(id)
        val current = store.claim(id, Pool.COPY, 1)
        assertTrue(store.checkpoint(id, current.attempt, "new receipt"))
        assertFalse(store.checkpoint(id, old.attempt, "late old receipt"))
        assertEquals("new receipt", store.get(id).payload)
        store.finish(id, current.attempt, true, null)
        assertFalse(store.checkpoint(id, current.attempt, "post-finish mutation"))
    }
    @Test fun receiptScanIncludesCompletedCopiesButRespectsPrefixAndHighWaterMark() {
        store.admit("copy:one", "one", "one", Pool.COPY, "one", State.QUEUED, false)
        store.admit("copy:two", "two", "two", Pool.COPY, "two", State.QUEUED, false)
        val lease = store.claim("copy:one", Pool.COPY, 1)
        store.finish(lease.id, lease.attempt, true, null)
        val through = store.highWaterMark()
        store.admit("copy:later", "later", "later", Pool.COPY, "later", State.QUEUED, false)
        val first = store.tasksPage("copy:", 0, through, 1).single()
        assertEquals(State.SUCCEEDED, first.state)
        assertEquals("copy:two", store.tasksPage("copy:", first.sequence, through, 10).single().id)
    }
}
