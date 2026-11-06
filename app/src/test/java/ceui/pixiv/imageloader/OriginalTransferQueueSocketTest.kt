package ceui.pixiv.imageloader

import android.app.Application
import ceui.pixiv.cache.OriginalImageCacheManager
import ceui.pixiv.download.StageStore
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.io.Closeable
import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import okio.ByteString.Companion.encodeUtf8
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
class OriginalTransferQueueSocketTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "original-socket-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var server: MockWebServer
    private lateinit var root: File
    private lateinit var cache: File
    private lateinit var stage: File
    private lateinit var queue: OriginalTransferQueue
    private lateinit var scope: CoroutineScope
    private val pins = AtomicInteger()
    private val client = OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS).build()
    @Before fun setup() {
        root = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        cache = File(root, "cache").apply { mkdirs() }; stage = File(root, "stage").apply { mkdirs() }
        runtime = ContentQueueRuntime(app, database); runtime.initialize()
        server = MockWebServer().apply { start() }
        scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val raw = ResumableOkHttpFetcher({ client }, { error("No default context") }, { cache }, { stage })
        queue = OriginalTransferQueue(runtime, scope, raw, { OriginalImageCacheManager.find(cache, it) },
            { pins.incrementAndGet(); Closeable { pins.decrementAndGet() } }, { 1 })
    }
    @After fun teardown() {
        client.dispatcher.cancelAll()
        runBlocking { scope.coroutineContext[Job]?.cancelAndJoin() }
        server.shutdown(); client.connectionPool.evictAll(); client.dispatcher.executorService.shutdown()
        runtime.close(); app.deleteDatabase(database); root.deleteRecursively()
    }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }
    private suspend fun eventually(test: () -> Boolean) { withTimeout(5_000) { while (!test()) delay(10) } }

    private suspend fun CoroutineScope.cancelStalled(response: MockResponse, waitForPrefix: Boolean) {
        server.enqueue(response)
        val url = server.url("/original.jpg").toString()
        val consumer = launch { queue.acquire(url, {}).use { fail("a stalled transfer cannot produce a completed file") } }
        assertNotNull(withContext(Dispatchers.IO) { server.takeRequest(5, TimeUnit.SECONDS) })
        if (waitForPrefix) {
            eventually { StageStore.partFile(stage, StageStore.keyForUrl(url)).length() > 0 }
            delay(50) // let the next socket read block after the first complete chunk
        }
        val other = runtime.store.enqueue("next", "next", Pool.NETWORK, "x")
        assertNull(runtime.store.claim(other, Pool.NETWORK, 1))
        withTimeout(3_000) {
            consumer.cancelAndJoin()
            eventually { runtime.store.get("original:" + url.encodeUtf8().sha256().hex())?.state == State.CANCELLED && pins.get() == 0 }
        }
        val held = runtime.store.claim(other, Pool.NETWORK, 1)
        assertNotNull(held); runtime.store.finish(other, held.attempt, true, null)
        assertNull(OriginalImageCacheManager.find(cache, url))
        if (waitForPrefix) {
            assertTrue(StageStore.partFile(stage, StageStore.keyForUrl(url)).length() > 0)
            assertNotNull(StageStore.readManifest(StageStore.metaFile(stage, StageStore.keyForUrl(url))))
        }
    }

    @Test fun lastConsumerCancellationClosesARequestStuckBeforeResponseHeaders() = scenario {
        cancelStalled(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE), false)
    }
    @Test fun lastConsumerCancellationClosesAStalledBodyAndKeepsItsResumablePrefix() = scenario {
        cancelStalled(MockResponse().setHeader("ETag", "\"original-v1\"")
            .setBody("x".repeat(64 * 1024)).throttleBody(1024, 30, TimeUnit.SECONDS), true)
    }
}
