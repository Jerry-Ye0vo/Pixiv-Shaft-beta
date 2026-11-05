package ceui.lisa.core

import android.app.Application
import android.os.Looper
import androidx.room.Room
import ceui.lisa.activities.Shaft
import ceui.lisa.database.AppDatabase
import ceui.lisa.utils.Settings
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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
@LooperMode(LooperMode.Mode.PAUSED)
class ManagerAdmissionTest {
    private lateinit var db: AppDatabase
    private lateinit var manager: Manager
    private lateinit var writes: ExecutorService
    private lateinit var content: CopyOnWriteArrayList<DownloadItem>
    private val gson = Gson()

    private fun item(id: Int, name: String = "p$id.jpg", url: String = "https://example.invalid/$id.jpg") =
        gson.fromJson("""{"uuid":"task-$id","name":"$name","url":"$url","index":$id,
            "illust":{"id":1,"page_count":100,"title":"test"}}""", DownloadItem::class.java)

    @Before fun setup() {
        val app = RuntimeEnvironment.getApplication()
        Utils.init(app)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        Shaft.sGson = gson
        Shaft.sSettings = Settings().apply {
            maxConcurrentDownloads = 3
            downloadLimitType = 2
            mutedDownloadToasts = DownloadToastKind.entries.mapTo(linkedSetOf()) { it.name }
        }
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", db)
        manager = ReflectionHelpers.callConstructor(Manager::class.java)
        writes = ReflectionHelpers.getField(manager, "queueWrites")
        // Saturated transport slots keep tests on real admission/state code without starting network IO.
        content = CopyOnWriteArrayList((0..2).map { item(it).apply { state = DownloadItem.DownloadState.DOWNLOADING } })
        ReflectionHelpers.setField(manager, "content", content)
    }

    @After fun cleanup() {
        drain()
        writes.shutdown()
        AppDatabase.destroyInstance()
        db.close()
    }

    private fun drain() { writes.submit {}.get(10, TimeUnit.SECONDS) }
    private fun holdWrites(): CountDownLatch {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        writes.execute { started.countDown(); check(release.await(10, TimeUnit.SECONDS)) }
        assertTrue(started.await(10, TimeUnit.SECONDS))
        return release
    }
    private fun ready(): DownloadItem? = ReflectionHelpers.callInstanceMethod(manager, "getFirstReady")

    @Test fun slowPersistenceDoesNotBlockUiOrAuthorizeManualDownloadBeforeCommit() {
        val release = holdWrites()
        val target = item(7)
        val acknowledged = AtomicInteger()
        try {
            manager.addTask(target) { acknowledged.incrementAndGet() }
            manager.startOne(target.uuid)
            assertTrue(manager.contentSnapshot().contains(target))
            assertNull(db.downloadDao().getDownloadingByFileName(target.name))
            assertNull(ready())
            shadowOf(Looper.getMainLooper()).idle()
            assertEquals(0, acknowledged.get())
        } finally { release.countDown() }
        drain()
        shadowOf(Looper.getMainLooper()).idle()
        assertEquals(1, acknowledged.get())
        assertEquals(target.uuid, db.downloadDao().getDownloadingByFileName(target.name).uuid)
        assertSame(target, ready())
    }

    @Test fun failedInsertCannotRunOrAcknowledgeAndExplicitRetryPersistsAgain() {
        db.openHelper.writableDatabase.execSQL("CREATE TRIGGER fail_admission BEFORE INSERT ON illust_downloading_table BEGIN SELECT RAISE(ABORT, 'test full disk'); END")
        val target = item(7)
        val acknowledged = AtomicInteger()
        manager.addTask(target) { acknowledged.incrementAndGet() }
        drain()
        shadowOf(Looper.getMainLooper()).idle()
        assertTrue(target.isFailed)
        assertEquals(0, acknowledged.get())
        assertNull(db.downloadDao().getDownloadingByFileName(target.name))
        assertNull(ready())
        db.openHelper.writableDatabase.execSQL("DROP TRIGGER fail_admission")
        manager.startOne(target.uuid)
        drain()
        assertEquals(target.uuid, db.downloadDao().getDownloadingByFileName(target.name).uuid)
        assertSame(target, ready())
    }

    @Test fun queuedInsertCannotResurrectATaskClearedBeforeCommit() {
        val release = holdWrites()
        try {
            val target = item(7)
            manager.addTask(target)
            manager.clearOne(target.uuid)
        } finally { release.countDown() }
        drain()
        assertNull(db.downloadDao().getDownloadingByFileName("p7.jpg"))
        assertTrue(manager.contentSnapshot().none { it.uuid == "task-7" })
    }

    @Test fun clearThenReaddSameObjectCreatesANewAdmissionTicket() {
        val target = item(7)
        val release = holdWrites()
        try {
            manager.addTask(target)
            manager.clearOne(target.uuid)
            manager.addTask(target)
        } finally { release.countDown() }
        drain()
        assertEquals(target.uuid, db.downloadDao().getDownloadingByFileName(target.name).uuid)
        assertTrue(manager.contentSnapshot().contains(target))
    }

    @Test fun clearAllIsOrderedBeforeSubsequentNewAdmissions() {
        val release = holdWrites()
        try {
            manager.addTask(item(7))
            manager.clearAll()
            manager.addTask(item(8))
        } finally { release.countDown() }
        drain()
        assertNull(db.downloadDao().getDownloadingByFileName("p7.jpg"))
        assertEquals("task-8", db.downloadDao().getDownloadingByFileName("p8.jpg").uuid)
        assertEquals(listOf("task-8"), manager.contentSnapshot().map { it.uuid })
    }

    @Test fun pauseAndFailureCommandsSurviveRecoveryWithLegacyTableLayout() {
        val target = item(7)
        manager.addTask(target)
        drain()
        manager.startOne(target.uuid)
        manager.stopOne(target.uuid)
        drain()
        val paused = Manager.readRestoredDownloads(db.downloadDao(), gson).single()
        assertTrue(paused.isPaused)
        assertEquals(1, paused.queueRecordVersion)
        manager.startOne(target.uuid)
        ReflectionHelpers.callInstanceMethod<Void>(manager, "complete",
            ReflectionHelpers.ClassParameter.from(DownloadItem::class.java, target),
            ReflectionHelpers.ClassParameter.from(java.lang.Boolean.TYPE, false))
        drain()
        assertTrue(Manager.readRestoredDownloads(db.downloadDao(), gson).single().isFailed)
    }

    @Test fun delayedOldCompletionCannotDeleteAReplacementWithTheSameFilename() {
        val old = item(7, name = "shared.jpg")
        manager.addTask(old); drain()
        manager.clearOne(old.uuid)
        val replacement = item(8, name = "shared.jpg")
        manager.addTask(replacement); drain()
        ReflectionHelpers.callInstanceMethod<Void>(manager, "complete",
            ReflectionHelpers.ClassParameter.from(DownloadItem::class.java, old),
            ReflectionHelpers.ClassParameter.from(java.lang.Boolean.TYPE, true))
        drain()
        assertEquals(replacement.uuid, db.downloadDao().getDownloadingByFileName("shared.jpg").uuid)
    }

    @Test fun identicalUrlAndFilenameDoNotSilentlyDeduplicateDifferentPages() {
        val first = item(7, "shared.jpg", "https://example.invalid/shared-preview.jpg")
        val second = item(8, "shared.jpg", "https://example.invalid/shared-preview.jpg")
        manager.addTask(first)
        manager.addTask(second)
        drain()
        assertTrue(manager.contentSnapshot().contains(second))
        assertTrue("Different page must report a collision, not disappear", second.isFailed)
        assertEquals(first.uuid, db.downloadDao().getDownloadingByFileName(first.name).uuid)
    }

    @Test fun batchPreservesDistinctOutputNamesForTheSameResource() {
        val first = item(7, "first-output.jpg")
        val second = item(7, "second-output.jpg").apply { uuid = "other-output" }
        manager.addTasks(listOf(first, second))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (manager.contentSnapshot().none { it.uuid == second.uuid }) {
            check(System.nanoTime() < deadline) { "Batch was not reserved" }
            Thread.sleep(5)
        }
        drain()
        assertEquals(first.uuid, db.downloadDao().getDownloadingByFileName(first.name).uuid)
        assertEquals(second.uuid, db.downloadDao().getDownloadingByFileName(second.name).uuid)
    }
}
