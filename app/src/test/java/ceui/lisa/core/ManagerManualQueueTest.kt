package ceui.lisa.core

import android.app.Application
import ceui.lisa.activities.Shaft
import ceui.lisa.utils.Settings
import com.blankj.utilcode.util.Utils
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers
import java.util.concurrent.CopyOnWriteArrayList

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ManagerManualQueueTest {
    private lateinit var manager: Manager
    private lateinit var content: CopyOnWriteArrayList<DownloadItem>
    private fun item(id: Int, state: Int = DownloadItem.DownloadState.INIT): DownloadItem =
        Gson().fromJson("""{"uuid":"task-$id","name":"p$id.jpg","url":"https://example.invalid/$id.jpg",
          "index":$id,"state":$state,"illust":{"id":1,"page_count":20,"title":"test"}}""",
            DownloadItem::class.java)

    @Before fun setup() {
        val app = RuntimeEnvironment.getApplication<Application>()
        Utils.init(app)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        Shaft.sSettings = Settings().apply { maxConcurrentDownloads = 3; downloadLimitType = 2 }
        manager = ReflectionHelpers.callConstructor(Manager::class.java)
        content = CopyOnWriteArrayList((0..2).map { item(it, DownloadItem.DownloadState.DOWNLOADING) })
        content.addAll((3..12).map { item(it) })
        ReflectionHelpers.setField(manager, "content", content)
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
}
