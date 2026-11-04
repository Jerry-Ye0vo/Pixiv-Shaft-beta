package ceui.pixiv.ui.detail

import android.app.Application
import ceui.lisa.core.DownloadItem
import ceui.pixiv.api.model.Illust
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class PageDownloadFabStateTest {
    private fun item(id: Long = 1, page: Int = 3, state: Int = DownloadItem.DownloadState.INIT,
                     paused: Boolean = false, percent: Int = 0): DownloadItem =
        Gson().fromJson("""{"illust":{"id":$id,"page_count":20},"index":$page,
            "uuid":"$id-$page","state":$state,"paused":$paused,"nonius":$percent}""", DownloadItem::class.java)

    private fun resolve(items: List<DownloadItem>, wifi: Boolean = true, limit: Int = 0,
                        scheduled: Boolean = false) =
        resolvePageDownloadFabState(items, 1, 3, wifi, limit, scheduled)

    @Test fun `other pages and artworks cannot mark this page as downloaded or change progress`() {
        assertNull(resolve(listOf(item(page = 0), item(id = 2))))
        assertEquals(DownloadFab.Downloading(37), resolve(listOf(
            item(state = DownloadItem.DownloadState.DOWNLOADING, percent = 37),
            item(page = 0, state = DownloadItem.DownloadState.DOWNLOADING, percent = 99),
        )))
    }

    @Test fun `mutable progress produces separate immutable UI values`() {
        val task = item(state = DownloadItem.DownloadState.DOWNLOADING, percent = 12)
        val before = resolve(listOf(task))
        task.nonius = 65
        assertEquals(DownloadFab.Downloading(12), before)
        assertEquals(DownloadFab.Downloading(65), resolve(listOf(task)))
    }

    @Test fun `paused failed and cellular waiting pages offer resume`() {
        assertEquals(DownloadFab.Paused(0), resolve(listOf(item(paused = true)), limit = 2))
        assertEquals(DownloadFab.Resume(0), resolve(listOf(item(state = DownloadItem.DownloadState.FAILED))))
        assertEquals(DownloadFab.Resume(0), resolve(listOf(item()), wifi = false, limit = 1))
        assertEquals(DownloadFab.Downloading(0), resolve(listOf(item()), wifi = false, limit = 1, scheduled = true))
    }

    @Test fun `empty queue requires a file probe and does not imply completed artwork`() {
        assertNull(resolve(emptyList()))
    }
}
