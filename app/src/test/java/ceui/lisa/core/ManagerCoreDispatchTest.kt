package ceui.lisa.core

import android.app.Application
import android.os.Looper
import androidx.room.Room
import ceui.lisa.activities.Shaft
import ceui.lisa.database.AppDatabase
import ceui.lisa.utils.Settings
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.download.toast.DownloadToastKind
import com.blankj.utilcode.util.Utils
import com.google.gson.Gson
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.annotation.LooperMode
import org.robolectric.util.ReflectionHelpers
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
@LooperMode(LooperMode.Mode.PAUSED)
class ManagerCoreDispatchTest {
    private lateinit var db: AppDatabase
    private lateinit var manager: Manager
    private lateinit var writes: ExecutorService
    private lateinit var owner: PageQueueOwner
    private val started = mutableListOf<DownloadItem>()
    private val gson = Gson()
    private fun item(i: Int, url: String = "https://example.invalid/$i.jpg") = gson.fromJson(
        """{"uuid":"task-$i","name":"p$i.jpg","url":"$url","index":$i,"illust":{"id":1,"page_count":100}}""", DownloadItem::class.java)
    @Before fun setup() {
        val app = RuntimeEnvironment.getApplication()
        Utils.init(app)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        app.deleteDatabase("content_queue.db")
        Shaft.sGson = gson
        Shaft.sSettings = Settings().apply {
            maxConcurrentDownloads = 1
            downloadLimitType = 2
            mutedDownloadToasts = DownloadToastKind.entries.mapTo(linkedSetOf()) { it.name }
        }
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", db)
        manager = ReflectionHelpers.callConstructor(Manager::class.java)
        owner = ReflectionHelpers.getField(manager, "pageQueue")
        writes = ReflectionHelpers.getField(manager, "queueWrites")
        ReflectionHelpers.setField(manager, "workerStarter", Consumer<DownloadItem> {
            // This assertion runs at the actual boundary before network/storage preparation.
            assertEquals(State.RUNNING, owner.store.get(PageQueueOwner.id(it)).state)
            assertNotNull(db.downloadDao().getDownloadingByFileName(it.name))
            started.add(it)
        })
    }
    private fun cycle() {
        repeat(4) { writes.submit {}.get(10, TimeUnit.SECONDS); shadowOf(Looper.getMainLooper()).idle() }
    }
    private fun exit(item: DownloadItem) {
        ReflectionHelpers.callInstanceMethod<Void>(manager, "workerTerminated", ReflectionHelpers.ClassParameter.from(DownloadItem::class.java, item))
        cycle()
    }
    @After fun cleanup() {
        cycle(); writes.shutdown(); owner.close()
        AppDatabase.destroyInstance(); db.close()
        RuntimeEnvironment.getApplication().deleteDatabase("content_queue.db")
    }
    @Test fun manualStartRequiresCommittedLeaseAndCannotExceedCapacity() {
        val a = item(1); val b = item(2)
        manager.addTask(a); manager.addTask(b); cycle()
        manager.startOne(a.uuid); manager.startOne(b.uuid); cycle()
        assertEquals(listOf(a), started)
        manager.stopOne(a.uuid); cycle()
        assertEquals(State.PAUSE_REQUESTED, owner.store.get(PageQueueOwner.id(a)).state)
        assertEquals(listOf(a), started) // stopping/preparation still owns slot
        exit(a)
        assertEquals(listOf(a,b), started)
        assertEquals(State.PAUSED, owner.store.get(PageQueueOwner.id(a)).state)
    }
    @Test fun clearBeforeClaimNeverStartsWorkerAndCreatesTerminalIntent() {
        val a = item(1)
        manager.addTask(a); cycle()
        val gate = java.util.concurrent.CountDownLatch(1)
        writes.execute { check(gate.await(10, TimeUnit.SECONDS)) }
        try { manager.startOne(a.uuid); manager.clearOne(a.uuid) } finally { gate.countDown() }
        cycle()
        assertTrue(started.isEmpty())
        assertEquals(State.CANCELLED, owner.store.get(PageQueueOwner.id(a)).state)
        assertNull(db.downloadDao().getDownloadingByFileName(a.name))
    }
    @Test fun sharedResourceIsSerializedUntilDurableCleanupAcknowledgement() {
        Shaft.sSettings.maxConcurrentDownloads = 3
        val a = item(1); val b = item(2, a.url)
        // Normal admission deduplicates artwork/page/output; these are distinct pages sharing bytes.
        manager.addTask(a); manager.addTask(b); cycle()
        manager.startAll(); cycle()
        assertEquals(listOf(a), started)
        manager.stopOne(a.uuid); cycle(); exit(a)
        assertEquals(listOf(a,b), started)
    }
    @Test fun failedWorkerDoesNotBlockTheNextRequestedPage() {
        val a = item(1); val b = item(2)
        manager.addTask(a); manager.addTask(b); cycle()
        manager.startOne(a.uuid); manager.startOne(b.uuid); cycle()
        ReflectionHelpers.callInstanceMethod<Void>(manager, "complete",
            ReflectionHelpers.ClassParameter.from(DownloadItem::class.java, a),
            ReflectionHelpers.ClassParameter.from(Boolean::class.javaPrimitiveType!!, false))
        cycle()
        assertEquals(State.FAILED, owner.store.get(PageQueueOwner.id(a)).state)
        assertEquals(listOf(a,b), started)
        assertNotNull(db.downloadDao().getDownloadingByFileName(a.name))
    }

    @Test fun releasedCapacityInAnotherAdapterWakesBlockedPageClaims() {
        val a = item(1)
        manager.addTask(a); cycle()
        val other = owner.store.enqueue("other-output", "other-source", ceui.pixiv.download.queue.ContentQueueStore.Pool.COPY, "other")
        val held = owner.store.claim(other, ceui.pixiv.download.queue.ContentQueueStore.Pool.COPY, 1)
        manager.startOne(a.uuid); cycle()
        assertTrue(started.isEmpty())
        owner.store.finish(other, held.attempt, true, null)
        ceui.pixiv.download.queue.ContentQueueRuntime.get(RuntimeEnvironment.getApplication()).changed()
        cycle()
        assertEquals(listOf(a), started)
    }

}
