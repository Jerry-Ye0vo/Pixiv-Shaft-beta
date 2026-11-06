package ceui.pixiv.imageloader

import android.app.Application
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import kotlinx.coroutines.*
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
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
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class OriginalTransferQueueTest {
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var scope: CoroutineScope
    private lateinit var dir: File
    private val pins = AtomicInteger()
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "transfers-${UUID.randomUUID()}.db"
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, name)
        scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        dir = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
    }
    @After fun cleanup() = runBlocking {
        scope.coroutineContext[Job]!!.cancelAndJoin()
        runtime.close(); app.deleteDatabase(name); dir.deleteRecursively()
        assertEquals("every loan and worker pin must be released", 0, pins.get())
    }
    private fun queue(fetcher: ImageFetcher, cached: (String) -> File? = { null },
                      retry: (Throwable, Boolean) -> Boolean = { _, _ -> false }) = OriginalTransferQueue(
        runtime, scope, fetcher, cached,
        { pins.incrementAndGet(); val done = java.util.concurrent.atomic.AtomicBoolean()
          Closeable { if (done.compareAndSet(false, true)) pins.decrementAndGet() } },
        { 1 }, retry)
    private fun file() = File(dir, UUID.randomUUID().toString()).apply { writeText("image") }
    private suspend fun eventually(condition: () -> Boolean) = withTimeout(5_000) {
        while (!condition()) delay(10)
    }
    private fun taskId(url: String) = "original:" + okio.ByteString.encodeUtf8(url).sha256().hex()

    @Test fun viewAndSaveShareOneActualHttpTransferAndCancellationIsConsumerScoped() = runBlocking {
        val server = MockWebServer()
        server.start()
        try {
            server.enqueue(MockResponse().setBody("actual image bytes").setBodyDelay(150, java.util.concurrent.TimeUnit.MILLISECONDS))
            val cache = mutableMapOf<String, File>()
            val network = object : ImageFetcher {
                override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File = withContext(Dispatchers.IO) {
                    assertEquals(State.RUNNING, runtime.store.get(taskId(url)).state)
                    OkHttpClient().newCall(okhttp3.Request.Builder().url(url).build()).execute().use {
                        File(dir, "http.jpg").apply { writeBytes(it.body!!.bytes()); synchronized(cache) { cache[url] = this } }
                    }
                }
            }
            val q = queue(network, { synchronized(cache) { cache[it] } })
            val url = server.url("/original.jpg?quality=original").toString()
            val view = async { q.acquire(url, {}) }
            eventually { server.requestCount == 1 }
            val attached = CompletableDeferred<Unit>()
            val save = async { q.acquire(url, { attached.complete(Unit) }, saving = true) }
            withTimeout(5_000) { attached.await() }
            view.cancelAndJoin()
            save.await().use { assertEquals("actual image bytes", it.file.readText()) }
            assertEquals(1, server.requestCount)
            q.acquire(url, {}).close() // cache hit does not enqueue or fetch
            assertEquals(1, server.requestCount)
        } finally { server.shutdown() }
    }

    @Test fun fullNetworkCapacityWaitsWithoutStartingAnyTransport() = runBlocking {
        runtime.initialize()
        val other = runtime.store.enqueue("other", "other", Pool.NETWORK, "other")
        val held = runtime.store.claim(other, Pool.NETWORK, 1)
        val started = CompletableDeferred<Unit>()
        val q = queue(object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File { started.complete(Unit); return file() }
        })
        val url = "https://example.invalid/cap.jpg"
        val pending = async { q.acquire(url, {}) }
        eventually { runtime.store.get(taskId(url)) != null }
        assertFalse(started.isCompleted)
        assertEquals(State.QUEUED, runtime.store.get(taskId(url)).state)
        runtime.store.finish(other, held.attempt, true, null); runtime.changed()
        withTimeout(5_000) { pending.await().close() }
        assertTrue(started.isCompleted)
    }

    @Test fun lastConsumerCancellationRetainsPermitAndPinUntilWorkerReallyExits() = runBlocking {
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val q = queue(object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File {
                started.complete(Unit)
                withContext(NonCancellable) { release.await() }
                return file()
            }
        })
        val url = "https://example.invalid/cancel.jpg"
        val pending = async { q.acquire(url, {}) }
        try {
            started.await(); pending.cancelAndJoin()
            assertTrue(pins.get() > 0)
            val other = runtime.store.enqueue("other", "other", Pool.NETWORK, "other")
            assertNull(runtime.store.claim(other, Pool.NETWORK, 1))
            release.complete(Unit)
            eventually { runtime.store.get(taskId(url)).state == State.CANCELLED }
            assertNotNull(runtime.store.claim(other, Pool.NETWORK, 1))
        } finally { release.complete(Unit) }
    }

    @Test fun failedSharedAttemptCanBeRetriedWithANewToken() = runBlocking {
        val calls = AtomicInteger()
        val q = queue(object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File {
                if (calls.incrementAndGet() == 1) throw java.io.IOException("network failed")
                return file()
            }
        })
        val url = "https://example.invalid/retry.jpg"
        assertTrue(runCatching { q.acquire(url, {}) }.exceptionOrNull() is java.io.IOException)
        eventually { runtime.store.get(taskId(url)).state == State.FAILED }
        q.acquire(url, {}, saving = true).close()
        assertEquals(2, calls.get())
    }

    @Test fun differentQualityQueriesNeverShareAFileOrTaskIdentity() = runBlocking {
        val calls = AtomicInteger()
        val q = queue(object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File { calls.incrementAndGet(); return file() }
        })
        val a = "https://example.invalid/image?quality=preview"
        val b = "https://example.invalid/image?quality=original"
        q.acquire(a, {}).close(); q.acquire(b, {}).close()
        assertEquals(2, calls.get())
        assertNotEquals(taskId(a), taskId(b))
    }

    @Test fun sharedStallRetryHasExactlyOneBudget() = runBlocking {
        val calls = AtomicInteger()
        val q = queue(object : ImageFetcher {
            override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File {
                calls.incrementAndGet(); throw java.net.SocketTimeoutException("read stalled")
            }
        }, retry = { _, saving -> saving })
        assertTrue(runCatching { q.acquire("https://example.invalid/stall", {}, saving = true) }.isFailure)
        assertEquals(2, calls.get())
    }
}
