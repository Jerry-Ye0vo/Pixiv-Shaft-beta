package ceui.pixiv.download

import android.app.Application
import android.net.Uri
import androidx.room.Room
import ceui.lisa.activities.Shaft
import ceui.lisa.database.AppDatabase
import ceui.lisa.database.DownloadEntity
import ceui.lisa.utils.Settings
import ceui.pixiv.api.model.Illust
import ceui.pixiv.imageloader.ImageFetcher
import ceui.pixiv.imageloader.ImageLoadState
import ceui.pixiv.imageloader.ImageLoadTask
import ceui.pixiv.imageloader.ImageRequest
import ceui.pixiv.imageloader.ImageTaskRegistry
import com.google.gson.Gson
import com.hjq.toast.Toaster
import java.io.File
import java.io.Closeable
import java.util.ArrayDeque
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ImagePageSaverRecoveryTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val scheduler = TestCoroutineScheduler()
    private val main = UnconfinedTestDispatcher(scheduler)
    private val io = GateDispatcher()
    private val scope = CoroutineScope(SupervisorJob() + main)
    private val illust = Illust(id = 42L, title = "恢复漫画", page_count = 8)
    private val key = ImagePageSaver.PageKey(42L, 3)
    private val url = "https://example.test/42_p3.jpg"
    private val journal get() = ImagePageSaveJournal(File(app.noBackupFilesDir, "image-page-saves.properties"))

    @Before fun setUp() {
        Dispatchers.setMain(main)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        Shaft.sGson = Gson()
        Shaft.sSettings = Settings()
        ImageSaveCacheGuard.resetForTests()
        Toaster.init(app)
        journal.write(emptyList())
    }

    @After fun tearDown() {
        scope.cancel()
        io.drain()
        ImageSaveCacheGuard.resetForTests()
        Dispatchers.resetMain()
    }

    private fun seed(error: String? = null, queuedAt: Long = 100L) {
        journal.write(listOf(ImagePageSaveJournal.Entry(Shaft.sGson.toJson(illust), 3, url, queuedAt, error)))
    }

    private fun saver() = ImagePageSaver(app, io, scope)

    @Test fun `pending save is restored without opening an artwork and cancellation stays removed`() {
        seed()
        val saver = saver()
        saver.start()
        io.runNext() // Load intents; hold the write/fetch pipeline before any network work.
        assertTrue(saver.states.value[key] is ImagePageSaver.PageState.Saving)
        assertEquals(illust.title, saver.request(key)?.illust?.title)
        assertEquals(3, saver.request(key)?.page)
        assertTrue(saver.cancel(key))
        io.drain()
        assertTrue(saver.states.value.isEmpty())
        assertTrue(journal.read().isEmpty())
        val reopened = saver()
        reopened.start()
        io.drain()
        assertTrue(reopened.states.value.isEmpty())
    }

    @Test fun `fresh save cancelled while loading does not revive an older disk intent`() {
        seed()
        val saver = saver()
        saver.start()
        assertTrue(saver.save(illust, 3, ImagePageSaver.Target.Original(url)))
        assertTrue(saver.cancel(key))
        io.drain()
        assertTrue(saver.states.value.isEmpty())
        assertTrue(journal.read().isEmpty())
    }

    @Test fun `failure survives restart and is hidden only after removal is committed`() {
        seed("network unavailable")
        val saver = saver()
        saver.start()
        io.drain()
        assertEquals("network unavailable", (saver.states.value[key] as ImagePageSaver.PageState.Failed).error)
        assertEquals("network unavailable", journal.read().single().error)
        assertTrue(saver.dismissFailure(key))
        assertTrue(saver.states.value[key] is ImagePageSaver.PageState.Failed)
        assertFalse(saver.retry(key)) // Removal owns this key until its durable commit.
        io.drain()
        assertTrue(saver.states.value.isEmpty())
        assertTrue(journal.read().isEmpty())
    }

    @Test fun `a committed readable page is reconciled instead of being saved a second time`() {
        seed(queuedAt = 100L)
        val db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", db)
        try {
            val file = File(app.cacheDir, "committed-page.jpg").apply { writeBytes(byteArrayOf(1, 2, 3)) }
            db.downloadDao().insertDownload(DownloadEntity().apply {
                illustGson = Shaft.sGson.toJson(illust)
                fileName = "committed-page.jpg"
                filePath = Uri.fromFile(file).toString()
                page = 3
                downloadTime = 101L
            })
            val saver = saver()
            saver.start()
            io.drain()
            assertTrue(saver.states.value.isEmpty())
            assertTrue(journal.read().isEmpty())
            assertEquals(listOf<Byte>(1, 2, 3), file.readBytes().toList())
            assertEquals(1, db.downloadDao().getDownloadedPageCandidates(42L, 3).size)
        } finally {
            AppDatabase.destroyInstance()
            db.close()
        }
    }

    @Test fun `save retains its source while waiting and cancellation releases it without stopping display`() {
        val db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", db)
        val result = CompletableDeferred<File>()
        var acquired = 0
        var released = 0
        ImageSaveCacheGuard.install {
            assertEquals(url, it)
            acquired++
            Closeable { released++ }
        }
        val display = ImageLoadTask(ImageRequest(url), scope, object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File {
                onProgress(37)
                return result.await()
            }
        }, { 0L })
        try {
            display.start()
            val taskMap = ReflectionHelpers.getField<MutableMap<String, ImageLoadTask>>(ImageTaskRegistry, "taskMap")
            taskMap[url] = display
            val saver = saver()
            assertTrue(saver.save(illust, 3, ImagePageSaver.Target.Original(url)))
            io.drain()
            assertEquals(1, acquired)
            assertEquals(0, released)
            assertTrue(saver.states.value[key] is ImagePageSaver.PageState.Saving)
            assertTrue(saver.cancel(key))
            io.drain()
            assertEquals(1, released)
            assertTrue(saver.states.value.isEmpty())
            assertTrue(journal.read().isEmpty())
            assertEquals(ImageLoadState.Loading(37), display.state.value)
            result.complete(File(app.cacheDir, "display-after-cancel.jpg").apply { writeText("image") })
            assertTrue(display.state.value is ImageLoadState.Success)
        } finally {
            result.cancel()
            ImageTaskRegistry.remove(url)
            ImageSaveCacheGuard.resetForTests()
            AppDatabase.destroyInstance()
            db.close()
        }
    }

    /** Deterministic IO boundaries; no sleeps and no real image/network requests. */
    private class GateDispatcher : CoroutineDispatcher() {
        private val work = ArrayDeque<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { work.addLast(block) }
        fun runNext() { check(work.isNotEmpty()); work.removeFirst().run() }
        fun drain() {
            var count = 0
            while (work.isNotEmpty()) {
                check(++count <= 100) { "Unexpected save/recovery loop" }
                runNext()
            }
        }
    }
}

