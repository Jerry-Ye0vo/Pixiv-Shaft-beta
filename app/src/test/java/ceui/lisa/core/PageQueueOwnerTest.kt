package ceui.lisa.core

import android.app.Application
import androidx.room.Room
import ceui.lisa.database.AppDatabase
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
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
class PageQueueOwnerTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val gson = Gson()
    private val name = "owner-${UUID.randomUUID()}.db"
    private lateinit var db: AppDatabase
    private lateinit var owner: PageQueueOwner
    private fun item(i: Int, url: String = "https://example.invalid/$i.jpg") = gson.fromJson(
        """{"uuid":"task-$i","name":"p$i.jpg","url":"$url","index":$i,"illust":{"id":1,"page_count":100}}""", DownloadItem::class.java)
    @Before fun setup() {
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        owner = PageQueueOwner(app, name)
    }
    @After fun cleanup() { owner.close(); db.close(); app.deleteDatabase(name) }
    private fun reopen() { owner.close(); owner = PageQueueOwner(app, name) }

    @Test fun legacyOnlyIntentIsImportedIdempotentlyWithStableIdentity() {
        val task = item(1)
        PageQueuePersistence.admit(db, gson, task)
        repeat(3) { assertEquals(listOf(task.uuid), owner.restore(db, gson).map { it.uuid }) }
        assertEquals(1L, owner.store.highWaterMark())
        assertNotNull(db.downloadDao().getDownloadingByFileName(task.name))
    }
    @Test fun coreFailureLeavesReplayableJournalAndDoesNotAuthorizeWorker() {
        val task = item(1)
        owner.store.writableDatabase.execSQL("CREATE TRIGGER reject_insert BEFORE INSERT ON tasks BEGIN SELECT RAISE(ABORT,'rejected'); END")
        try { owner.admit(db, gson, task); fail("must fail closed") } catch (_: android.database.sqlite.SQLiteException) { }
        assertNotNull(db.downloadDao().getDownloadingByFileName(task.name))
        assertNull(owner.claim(task, 1))
        owner.store.writableDatabase.execSQL("DROP TRIGGER reject_insert")
        assertEquals(task.uuid, owner.restore(db, gson).single().uuid)
        assertNotNull(owner.claim(task, 1))
    }
    @Test fun successfulTombstoneSuppressesOldJournalAfterRestart() {
        val task = item(1)
        owner.admit(db, gson, task)
        val lease = owner.claim(task, 1)!!
        owner.committed(lease)
        // Simulates death after output acknowledgement but before compatibility journal removal.
        reopen()
        assertTrue(owner.restore(db, gson).isEmpty())
        assertEquals(State.SUCCEEDED, owner.store.get(PageQueueOwner.id(task)).state)
        assertNull(db.downloadDao().getDownloadingByFileName(task.name))
    }
    @Test fun coreOnlyIntentRestoresAndPreservesExplicitPauseOverStaleMirror() {
        val task = item(1)
        owner.admit(db, gson, task)
        owner.update(db, gson, task, true, DownloadItem.DownloadState.INIT)
        db.downloadDao().deleteAllDownloading()
        reopen()
        assertTrue(owner.restore(db, gson).single().isPaused)
    }
    @Test fun pauseThenResumeKeepsPermitUntilCleanupAndUsesNewAttempt() {
        val task = item(1); val next = item(2)
        owner.admit(db, gson, task); owner.admit(db, gson, next)
        val old = owner.claim(task, 1)!!
        owner.update(db, gson, task, true, DownloadItem.DownloadState.INIT)
        owner.update(db, gson, task, false, DownloadItem.DownloadState.INIT)
        assertEquals(State.YIELD_REQUESTED, owner.store.get(old.id).state)
        assertNull(owner.claim(next, 1))
        assertTrue(owner.finish(db, gson, task, old, false, true))
        assertNotEquals(old.attempt, owner.claim(task, 1)!!.attempt)
        assertFalse(owner.finish(db, gson, task, old, true, false))
    }
    @Test fun sharedResourceWaitsEvenWithDifferentOutputsAndSpareCapacity() {
        val task = item(1); val next = item(2, task.url)
        owner.admit(db, gson, task); owner.admit(db, gson, next)
        val first = owner.claim(task, 3)!!
        assertNull(owner.claim(next, 3))
        owner.committed(first)
        assertNull(owner.claim(next, 3))
        assertTrue(owner.finish(db, gson, task, first, true, false))
        assertNotNull(owner.claim(next, 3))
    }
    @Test fun clearImportsUnseenRecordsBeforeWithdrawingThem() {
        val task = item(1)
        PageQueuePersistence.admit(db, gson, task)
        owner.clear(db, gson)
        PageQueuePersistence.admit(db, gson, task) // stale legacy writer replay
        reopen()
        assertTrue(owner.restore(db, gson).isEmpty())
        assertEquals(State.CANCELLED, owner.store.get(PageQueueOwner.id(task)).state)
    }
    @Test fun cancelledWorkerRetainsPermitUntilExitAndCannotBeRevivedByImport() {
        val task = item(1); val next = item(2)
        owner.admit(db, gson, task); owner.admit(db, gson, next)
        val first = owner.claim(task, 1)!!
        owner.cancel(db, task)
        assertNull(owner.claim(next, 1))
        owner.finish(db, gson, task, first, false, true)
        PageQueuePersistence.admit(db, gson, task)
        assertEquals(listOf(next.uuid), owner.restore(db, gson).map { it.uuid })
        assertNotNull(owner.claim(next, 1))
    }
}
