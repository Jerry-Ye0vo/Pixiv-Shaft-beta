package ceui.lisa.core

import android.app.Application
import androidx.room.Room
import ceui.lisa.database.AppDatabase
import ceui.lisa.activities.Shaft
import ceui.lisa.utils.Settings
import com.blankj.utilcode.util.Utils
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Before
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ManagerManualQueueTest {
    private lateinit var manager: Manager
    private lateinit var content: CopyOnWriteArrayList<DownloadItem>
    private lateinit var database: AppDatabase
    private fun item(id: Int, state: Int = DownloadItem.DownloadState.INIT): DownloadItem =
        Gson().fromJson("""{"uuid":"task-$id","name":"p$id.jpg","url":"https://example.invalid/$id.jpg",
          "index":$id,"state":$state,"illust":{"id":1,"page_count":20,"title":"test"}}""",
            DownloadItem::class.java)

    @Before fun setup() {
        val app = RuntimeEnvironment.getApplication()
        Utils.init(app)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        Shaft.sSettings = Settings().apply { maxConcurrentDownloads = 3; downloadLimitType = 2 }
        Shaft.sGson = Gson()
        database = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", database)
        manager = ReflectionHelpers.callConstructor(Manager::class.java)
        content = CopyOnWriteArrayList((0..2).map { item(it, DownloadItem.DownloadState.DOWNLOADING) })
        content.addAll((3..12).map { item(it) })
        ReflectionHelpers.setField(manager, "content", content)
    }

    @After fun cleanup() {
        val writes = ReflectionHelpers.getField<java.util.concurrent.ExecutorService>(manager, "queueWrites")
        writes.submit {}.get(10, TimeUnit.SECONDS)
        writes.shutdown()
        AppDatabase.destroyInstance()
        database.close()
    }

    @Test fun repeatedManualRequestsWaitAtCapacity() {
        repeat(3) { (3..11).forEach { manager.startOne("task-$it") } }
        assertEquals(3, content.count { it.state == DownloadItem.DownloadState.DOWNLOADING })
        assertTrue(content.drop(3).all { it.state == DownloadItem.DownloadState.INIT })
        assertTrue(manager.willDispatchPage(1, 3))
        assertFalse(manager.willDispatchPage(1, 12))
        assertFalse(ReflectionHelpers.getField<Boolean>(manager, "isRunning"))
    }

    @Test fun selectedPageRemainsEligibleAfterAnActivePageFinishes() {
        manager.startOne("task-7")
        content.removeAt(0) // transfer completion frees a slot
        assertSame(content.first { it.uuid == "task-7" },
            ReflectionHelpers.callInstanceMethod<DownloadItem>(manager, "getFirstReady"))
    }

    @Test fun pauseWithdrawsTheWaitingRequestWithoutStartingSiblingPages() {
        manager.startOne("task-7")
        manager.stopOne("task-7")
        assertFalse(manager.willDispatchPage(1, 7))
        assertNull(ReflectionHelpers.callInstanceMethod<DownloadItem?>(manager, "getFirstReady"))
    }

    @Test fun unknownIdDoesNotReleaseOtherTasks() {
        manager.startOne("missing")
        assertNull(ReflectionHelpers.callInstanceMethod<DownloadItem?>(manager, "getFirstReady"))
        assertFalse(manager.willDispatchPage(1, 3))
    }

    @Test fun pausedWorkerRetainsCapacityUntilItsBodyExits() {
        Shaft.sSettings.maxConcurrentDownloads = 1
        content.removeIf { it.uuid == "task-1" || it.uuid == "task-2" }
        val running = content.first()
        val handles = ReflectionHelpers.getField<MutableMap<String, DownloadTask>>(manager, "handles")
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val ended = CountDownLatch(1)
        val io = Executors.newSingleThreadExecutor()
        val task = DownloadTask.create({
            started.countDown()
            while (release.count > 0) {
                try { release.await() } catch (_: InterruptedException) { /* delayed shutdown */ }
            }
        }, {}, {}, {
            ReflectionHelpers.callInstanceMethod<Void>(manager, "workerTerminated",
                ReflectionHelpers.ClassParameter.from(DownloadItem::class.java, running))
            ended.countDown()
        })
        handles[running.uuid] = task
        task.start(io)
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS))
            manager.stopOne(running.uuid)
            manager.startOne("task-7")
            assertSame(task, handles[running.uuid])
            assertEquals(1, ReflectionHelpers.callInstanceMethod<Int>(manager, "activeCount"))
            assertEquals(DownloadItem.DownloadState.INIT, content.first { it.uuid == "task-7" }.state)
        } finally {
            release.countDown()
            io.shutdown()
            assertTrue(io.awaitTermination(2, TimeUnit.SECONDS))
        }
        assertTrue(ended.await(2, TimeUnit.SECONDS))
        assertFalse(handles.containsKey(running.uuid))
        assertEquals(0, ReflectionHelpers.callInstanceMethod<Int>(manager, "activeCount"))
    }

    @Test fun pauseAndResumeDuringPreparationDoesNotDispatchTwice() {
        val pending = content.first()
        val dispatching = ReflectionHelpers.getField<MutableSet<String>>(manager, "dispatching")
        dispatching.add(pending.uuid)
        manager.stopAll()
        manager.startOne(pending.uuid)
        assertTrue(dispatching.contains(pending.uuid))
        assertEquals(1, ReflectionHelpers.callInstanceMethod<Int>(manager, "activeCount"))
        assertNull(ReflectionHelpers.callInstanceMethod<DownloadItem?>(manager, "getFirstReady"))
    }

    @Test fun latePauseCannotTurnCommittedOutputBackIntoQueuedWork() {
        val committed = content.first()
        val handles = ReflectionHelpers.getField<MutableMap<String, DownloadTask>>(manager, "handles")
        val task = DownloadTask.create({}, {}, {}, {})
        handles[committed.uuid] = task
        task.cancel()
        committed.state = DownloadItem.DownloadState.SUCCESS
        committed.isPaused = true
        ReflectionHelpers.callInstanceMethod<Void>(manager, "workerTerminated",
            ReflectionHelpers.ClassParameter.from(DownloadItem::class.java, committed))
        assertTrue(committed.isSuccess)
    }
}

