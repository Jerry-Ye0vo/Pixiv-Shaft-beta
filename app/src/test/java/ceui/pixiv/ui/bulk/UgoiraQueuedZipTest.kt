package ceui.pixiv.ui.bulk

import android.app.Application
import ceui.pixiv.download.queue.ContentPhaseRunner
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class UgoiraQueuedZipTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "queued-zip-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var runner: ContentPhaseRunner
    private lateinit var server: MockWebServer
    private lateinit var root: File
    private lateinit var target: File
    private val client = OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS).build()
    private val url get() = server.url("/ugoira.zip").toString()
    @Before fun setup() {
        root = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        target = File(root, "ugoira.zip")
        runtime = ContentQueueRuntime(app, database); runtime.initialize()
        runner = ContentPhaseRunner(runtime) { 1 }
        server = MockWebServer().apply { start() }
    }
    @After fun teardown() {
        client.dispatcher.cancelAll(); server.shutdown(); client.connectionPool.evictAll()
        client.dispatcher.executorService.shutdown(); runtime.close(); app.deleteDatabase(database); root.deleteRecursively()
    }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }
    private fun rows() = runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 20)
    private suspend fun eventually(block: () -> Boolean) { withTimeout(5_000) { while (!block()) delay(10) } }
    private fun occupy(): ceui.pixiv.download.queue.ContentQueueStore.Task {
        val id = runtime.store.enqueue("original", "original", Pool.NETWORK, "x")
        return requireNotNull(runtime.store.claim(id, Pool.NETWORK, 1))
    }
    private fun release(held: ceui.pixiv.download.queue.ContentQueueStore.Task) {
        runtime.store.finish(held.id, held.attempt, true, null); runtime.changed()
    }

    @Test fun aFullSharedPoolPreventsEvenClientCreationAndQueuedCancellationStartsNoRequest() = scenario {
        val held = occupy()
        val clients = AtomicInteger()
        val job = launch { downloadQueuedZipTo(url, target, runner, { clients.incrementAndGet(); client }) }
        eventually { rows().singleOrNull()?.state == State.QUEUED }
        assertEquals(0, clients.get()); assertEquals(0, server.requestCount)
        job.cancelAndJoin()
        assertEquals(State.CANCELLED, rows().single().state)
        assertFalse(target.exists()); assertFalse(File(target.path + ".part").exists())
        release(held)
    }

    @Test fun aCacheProducedWhileWaitingIsRecheckedWithoutNetworkAccess() = scenario {
        val held = occupy()
        val clients = AtomicInteger()
        val job = launch { downloadQueuedZipTo(url, target, runner, { clients.incrementAndGet(); client }) }
        eventually { rows().size == 1 }
        target.writeText("previous playback bytes")
        release(held); job.join()
        assertEquals(0, clients.get()); assertEquals(0, server.requestCount)
        assertEquals("previous playback bytes", target.readText())
        assertEquals(State.SUCCEEDED, rows().single().state)
    }

    @Test fun playbackAndSaveRequestsSerializeTheTargetAndReuseTheFirstCompletedFile() = scenario {
        val held = occupy()
        server.enqueue(MockResponse().setHeader("ETag", "\"v1\"").setBody("complete zip bytes"))
        val first = launch { downloadQueuedZipTo(url, target, runner, { client }) }
        val second = launch { downloadQueuedZipTo(url, target, runner, { client }) }
        eventually { rows().size == 2 }
        release(held); joinAll(first, second)
        assertEquals(1, server.requestCount)
        assertEquals("complete zip bytes", target.readText())
        assertTrue(rows().all { it.state == State.SUCCEEDED })
    }

    @Test fun cancellationInterruptsAStalledSocketAndOnlyThenReleasesTheSharedPermit() = scenario {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
        val job = launch { downloadQueuedZipTo(url, target, runner, { client }) }
        assertNotNull(withContext(Dispatchers.IO) { server.takeRequest(5, TimeUnit.SECONDS) })
        val next = runtime.store.enqueue("next", "next", Pool.NETWORK, "x")
        assertNull(runtime.store.claim(next, Pool.NETWORK, 1))
        withTimeout(3_000) { job.cancelAndJoin() }
        assertEquals(State.CANCELLED, rows().single().state)
        val lease = runtime.store.claim(next, Pool.NETWORK, 1)
        assertNotNull(lease); runtime.store.finish(next, lease.attempt, true, null)
        assertFalse(target.exists())
    }
}
