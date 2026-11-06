package ceui.lisa.core

import android.app.Application
import androidx.room.Room
import ceui.lisa.database.AppDatabase
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.imageloader.ImageFetcher
import ceui.pixiv.imageloader.OriginalTransferQueue
import com.google.gson.Gson
import kotlinx.coroutines.*
import okio.ByteString.Companion.encodeUtf8
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import java.io.Closeable
import java.io.File

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class PageSharedSourceTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val gson = Gson()
    private lateinit var db: AppDatabase
    private lateinit var owner: PageQueueOwner
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    @Before fun setup() {
        app.deleteDatabase("content_queue.db")
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        owner = PageQueueOwner(app)
    }
    @After fun cleanup() = runBlocking {
        scope.coroutineContext[Job]!!.cancelAndJoin()
        owner.close(); db.close(); app.deleteDatabase("content_queue.db")
    }
    private fun item() = gson.fromJson("""{"uuid":"old-page","name":"p1.jpg","url":"https://example.invalid/original.jpg",
        "index":1,"illust":{"id":1,"page_count":3}}""", DownloadItem::class.java)
    @Test fun copyWaitingForItsSharedNetworkSourceMakesProgressAtCapacityOne() = runBlocking {
        val task = item()
        owner.admit(db, gson, task)
        val page = owner.claim(task, 1)!!
        assertEquals(Pool.COPY, page.pool)
        val runtime = ContentQueueRuntime.get(app)
        val file = File(app.cacheDir, "source.jpg").apply { writeText("complete original") }
        val q = OriginalTransferQueue(runtime, scope, object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File {
                assertEquals(State.RUNNING, owner.store.get(page.id).state)
                return file
            }
        }, { null }, { Closeable {} }, { 1 })
        try {
            withTimeout(5_000) { q.acquire(task.url, {}, saving = true).close() }
            // Opening the source adapter must never recover away the page's live COPY attempt.
            assertEquals(page.attempt, owner.store.get(page.id).attempt)
            owner.committed(page)
            assertTrue(owner.finish(db, gson, task, page, true, false))
        } finally { file.delete() }
    }
    @Test fun oldCoreOnlyPageIsReboundWithoutLosingIdPayloadOrTerminalHistory() {
        val task = item()
        owner.admit(db, gson, task)
        val id = PageQueueOwner.id(task)
        val before = owner.store.get(id)
        val oldResource = "pixiv-bytes:" + task.url.encodeUtf8().sha256().hex()
        owner.store.rebindWaiting(id, before.outputKey, before.resourceKey, oldResource, Pool.NETWORK)
        db.downloadDao().deleteAllDownloading() // recovery cannot depend solely on the compatibility mirror
        val restored = owner.restore(db, gson).single()
        assertEquals(task.uuid, restored.uuid)
        assertEquals(before.payload, owner.store.get(id).payload)
        assertEquals(Pool.COPY, owner.store.get(id).pool)
        assertEquals(before.resourceKey, owner.store.get(id).resourceKey)
    }
    @Test fun liveOldPageCannotBeReboundUntilProcessRecovery() {
        val task = item()
        owner.admit(db, gson, task)
        val before = owner.store.get(PageQueueOwner.id(task))
        val old = "pixiv-bytes:" + task.url.encodeUtf8().sha256().hex()
        owner.store.rebindWaiting(before.id, before.outputKey, before.resourceKey, old, Pool.NETWORK)
        val running = owner.store.claim(before.id, Pool.NETWORK, 1)
        try {
            owner.store.rebindWaiting(before.id, before.outputKey, old, before.resourceKey, Pool.COPY)
            fail("must retain live ownership")
        } catch (_: IllegalStateException) { }
        assertEquals(running.attempt, owner.store.get(before.id).attempt)
    }
}
