package ceui.pixiv.ui.novel.reader.settings

import android.app.Application
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import okio.Buffer
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ReaderFontQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "font-${UUID.randomUUID()}.db"
    private val font = ReaderWebFont.entries.first()
    private val bytes = "verified font test bytes".toByteArray()
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var server: MockWebServer
    private lateinit var service: QueuedReaderFonts
    private lateinit var client: OkHttpClient
    private val installed = mutableListOf<Boolean>()
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, name); queue = ContentPlanQueue(runtime, 1)
        server = MockWebServer().apply { start() }
        client = OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS).build()
        service = QueuedReaderFonts(app, queue, ContentPhaseRunner(runtime) { 1 }, { true }, client) { _, ready -> installed += ready }
    }
    @After fun teardown() {
        runBlocking { queue.shutdown() }; client.dispatcher.cancelAll(); server.shutdown()
        client.connectionPool.evictAll(); client.dispatcher.executorService.shutdown()
        runtime.close(); app.deleteDatabase(name); font.file(app).delete()
        File(app.noBackupFilesDir, "content_reader_fonts").deleteRecursively()
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(15_000, body) }
    private fun spec(delete: Boolean = false, checksum: String = hash(bytes)) = ReaderFontRequest(fontId = font.id,
        url = server.url("/font.ttf").toString(), bytes = bytes.size.toLong(), sha256 = checksum, delete = delete)
    private fun response() = MockResponse().setBody(Buffer().write(bytes)).setHeader("ETag", "\"font-v1\"")
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    private fun hash(bytes: ByteArray) = java.security.MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    private suspend fun eventually(check: () -> Boolean) { withTimeout(4000) { while (!check()) delay(10) } }
    @Test fun fontRequestsWaitForSharedNetworkCapacityBeforeAnySocketOpens() = scenario {
        queue.ready.await(); val id = runtime.store.enqueue("busy", "busy", Pool.NETWORK, "{}")
        val held = runtime.store.claim(id, Pool.NETWORK, 1)
        val ticket = service.enqueue(spec()); eventually { runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
            .any { it.pool == Pool.NETWORK && it.state == State.QUEUED } }
        assertEquals(0, server.requestCount); ticket.cancel(); assertEquals(State.CANCELLED, end(ticket).state)
        runtime.store.finish(id, held.attempt, true, null); runtime.changed()
    }
    @Test fun verifiedFontInstallsUnderItsOriginalIdAndNotifiesExistingSettings() = scenario {
        server.enqueue(response()); val ticket = service.enqueue(spec())
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertArrayEquals(bytes, font.file(app).readBytes()); assertEquals(listOf(true), installed)
        assertEquals("${font.id}.ttf", font.file(app).name)
    }
    @Test fun wrongChecksumNeverReplacesAnExistingInstalledFont() = scenario {
        font.file(app).parentFile!!.mkdirs(); font.file(app).writeText("older healthy font")
        server.enqueue(response()); val ticket = service.enqueue(spec(checksum = "0".repeat(64)))
        assertEquals(State.FAILED, end(ticket).state)
        assertEquals("older healthy font", font.file(app).readText()); assertTrue(installed.isEmpty())
    }
    @Test fun aVerifiedExistingFileCompletesWithoutAnyHttpRequest() = scenario {
        font.file(app).parentFile!!.mkdirs(); font.file(app).writeBytes(bytes)
        val ticket = service.enqueue(spec()); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(0, server.requestCount)
    }
    @Test fun cancellationClosesAStalledSocketBeforeASuccessorDeleteRuns() = scenario {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
        val first = service.enqueue(spec()); eventually { server.requestCount == 1 }
        val delete = service.enqueue(spec(delete = true))
        assertEquals(State.SUCCEEDED, end(delete).state); assertEquals(State.CANCELLED, end(first).state)
        assertFalse(font.file(app).exists()); assertEquals(listOf(false), installed)
    }
    @Test fun deletingAnInstalledFontUsesOnlyCopyAndKeepsTheOldDirectoryConvention() = scenario {
        font.file(app).parentFile!!.mkdirs(); font.file(app).writeBytes(bytes)
        val ticket = service.enqueue(spec(delete = true)); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertFalse(font.file(app).exists()); assertEquals(0, server.requestCount)
        assertTrue(runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100).all { it.pool == Pool.COPY })
    }
}
