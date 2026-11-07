package ceui.pixiv.download.model

import android.app.Application
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.ByteArrayOutputStream
import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
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
class ModelSharedQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "model-${UUID.randomUUID()}.db"
    private val asset = "test-${UUID.randomUUID()}"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var handler: ModelPlanHandler
    private val server = MockWebServer()
    private val gson = Gson()
    @Volatile private var automatic = true
    @Before fun setup() {
        server.start(); runtime = ContentQueueRuntime(app, database); createQueue()
    }
    private fun createQueue() {
        queue = ContentPlanQueue(runtime, 1)
        handler = ModelPlanHandler(app, ContentPhaseRunner(runtime) { 1 }, { automatic }) {
            OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS).build()
        }
        queue.register(handler)
    }
    @After fun teardown() {
        runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database); server.shutdown()
        File(app.filesDir, "rife-models/$asset").deleteRecursively()
        File(app.filesDir, "content_model_inputs").deleteRecursively()
    }
    private fun scenario(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(15_000, block) }
    private fun request(operation: String = ModelRequest.DOWNLOAD, source: String = server.url("/model.zip").toString()) =
        ModelRequest(directory = "rife-models", asset = asset, required = listOf("model.onnx"), operation = operation, source = source)
    private fun submit(spec: ModelRequest = request()) = queue.submit(spec.family, "model") { gson.toJson(spec) }
    private suspend fun end(ticket: ContentPlanTicket) = withTimeout(8_000) {
        ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    }
    private suspend fun eventually(test: () -> Boolean) { withTimeout(5_000) { while (!test()) delay(10) } }
    private fun zip(name: String = "model.onnx", value: String = "weights"): ByteArray = ByteArrayOutputStream().also { bytes ->
        ZipOutputStream(bytes).use { it.putNextEntry(ZipEntry(name)); it.write(value.toByteArray()); it.closeEntry() }
    }.toByteArray()
    private fun response(bytes: ByteArray = zip()) = MockResponse().setBody(Buffer().write(bytes)).setHeader("ETag", "\"model-v1\"")
    private fun target() = File(app.filesDir, "rife-models/$asset/model.onnx")

    @Test fun durableAdmissionPrecedesNetworkAndUsesTheSharedCapacityOnePool() = scenario {
        runtime.initialize()
        val blocker = runtime.store.enqueue("blocker", "blocker", Pool.NETWORK, "{}"); val lease = runtime.store.claim(blocker, Pool.NETWORK, 1)!!
        server.enqueue(response()); val ticket = submit()
        eventually { runtime.store.tasksPage("phase:", 0, Long.MAX_VALUE, 20).any { it.pool == Pool.NETWORK } }
        assertNotNull(queue.store.get(ticket.id)); assertEquals(0, server.requestCount)
        runtime.store.cancel(blocker); assertTrue(runtime.store.finish(blocker, lease.attempt, false, "test")); runtime.changed()
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals("weights", target().readText()); assertEquals(1, server.requestCount)
    }
    @Test fun unsuccessfulHttpNeverDeletesAnExistingInstalledModel() = scenario {
        target().parentFile!!.mkdirs(); target().writeText("old")
        server.enqueue(MockResponse().setResponseCode(503)); val ticket = submit()
        assertEquals(State.FAILED, end(ticket).state); assertEquals("old", target().readText())
        assertEquals(0, runtime.store.tasksPage("phase:", 0, Long.MAX_VALUE, 30).count { it.state == State.RUNNING })
    }
    @Test fun missingModelFilesFailAndExplicitRetryFetchesAFreshPackage() = scenario {
        target().parentFile!!.mkdirs(); target().writeText("old")
        server.enqueue(response(zip("wrong", "invalid"))); val first = submit()
        assertEquals(State.FAILED, end(first).state); assertEquals("old", target().readText())
        server.enqueue(response()); val retry = submit()
        assertEquals(State.SUCCEEDED, end(retry).state); assertEquals(first.id, retry.id)
        assertEquals("weights", target().readText()); assertEquals(2, server.requestCount)
    }
    @Test fun duplicateActiveRequestsShareOneParentAndOneHttpBody() = scenario {
        automatic = false; val first = submit(); val second = submit()
        assertSame(first, second); queue.ready.await()
        server.enqueue(response()); automatic = true; queue.refreshEligibility()
        assertEquals(State.SUCCEEDED, end(first).state); assertEquals(1, server.requestCount)
    }
    @Test fun cancellationClosesAStalledCallAndReleasesThePhysicalSlotBeforeAcknowledgement() = scenario {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)); val ticket = submit()
        eventually { server.requestCount == 1 }
        val start = System.nanoTime(); assertTrue(ticket.cancel())
        assertEquals(State.CANCELLED, end(ticket).state)
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 3_000)
        val another = runtime.store.enqueue("after", "after", Pool.NETWORK, "{}")
        assertNotNull(runtime.store.claim(another, Pool.NETWORK, 1)); assertFalse(target().exists())
    }
    @Test fun localImportUsesCopyWithoutAnyHttpAndSurvivesAnAbsentUiObserver() = scenario {
        val input = File(app.cacheDir, "model-${UUID.randomUUID()}.zip"); input.writeBytes(zip())
        try {
            val ticket = submit(request(ModelRequest.IMPORT, android.net.Uri.fromFile(input).toString()))
            val observer = launch { ticket.status.first { it.state == State.SUCCEEDED } }; observer.cancelAndJoin()
            assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(0, server.requestCount); assertEquals("weights", target().readText())
        } finally { input.delete() }
    }
    @Test fun pausedPrefixSurvivesAColdQueueAndResumesWithTheSameValidator() = scenario {
        val bytes = zip(); val prefix = bytes.size / 2
        server.enqueue(MockResponse().setBody(Buffer().write(bytes, 0, prefix))
            .setHeader("Content-Length", bytes.size).setHeader("ETag", "\"model-v1\""))
        val spec = request(); val first = submit(spec)
        eventually {
            File(app.filesDir, "content_model_inputs/${ModelRequest.hash(first.id)}/source.zip.part").length() == prefix.toLong()
        }
        assertTrue(first.pause()); assertEquals(State.PAUSED, end(first).state)
        queue.shutdown(); createQueue(); queue.ready.await()
        server.enqueue(MockResponse().setResponseCode(206).setBody(Buffer().write(bytes, prefix, bytes.size - prefix))
            .setHeader("ETag", "\"model-v1\"").setHeader("Content-Range", "bytes $prefix-${bytes.size - 1}/${bytes.size}"))
        val resumed = submit(spec)
        assertEquals(State.SUCCEEDED, end(resumed).state); assertEquals(first.id, resumed.id)
        server.takeRequest(1, TimeUnit.SECONDS)
        val range = server.takeRequest(1, TimeUnit.SECONDS)!!
        assertEquals("bytes=$prefix-", range.getHeader("Range")); assertEquals("\"model-v1\"", range.getHeader("If-Range"))
        assertEquals("weights", target().readText())
    }
    @Test fun coldRecoveryUsesTheInstalledReceiptWithoutFetchingAgain() = scenario {
        val spec = request(); val json = gson.toJson(spec); queue.ready.await()
        val admitted = queue.store.admit("plan:restored", spec.family, "model", json)
        val worker = ModelInstaller(app.filesDir, spec, admitted.id, json)
        val input = File(app.cacheDir, "model-${UUID.randomUUID()}.zip"); input.writeBytes(zip())
        try { worker.prepare(input, ModelInstaller.digest(input)); worker.install() } finally { input.delete() }
        queue.shutdown(); createQueue()
        queue.ready.await(); val ticket = requireNotNull(queue.latest(spec.family))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(0, server.requestCount)
    }
    @Test fun aColdParentWaitsForPolicyAndAnExplicitModelPermissionDoesNotReleaseOtherFamilies() = scenario {
        automatic = false; val spec = request()
        val first = submit(spec); val otherSpec = spec.copy(asset = asset + "-other"); val other = submit(otherSpec)
        queue.ready.await(); eventually { queue.store.get(first.id) != null && queue.store.get(other.id) != null }
        assertEquals(0, server.requestCount)
        server.enqueue(response()); handler.allow(first.family); queue.refreshEligibility()
        assertEquals(State.SUCCEEDED, end(first).state); assertEquals(State.QUEUED, queue.store.get(other.id).state)
        assertEquals(1, server.requestCount); other.cancel(); end(other)
    }
    @Test fun deleteRunsAsADurableCopyParentAndKeepsTheExistingDirectoryConvention() = scenario {
        target().parentFile!!.mkdirs(); target().writeText("old")
        val ticket = submit(request(ModelRequest.DELETE, ""))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertFalse(target().exists()); assertEquals(0, server.requestCount)
        assertEquals(ModelRequest.DELETE, gson.fromJson(queue.store.get(ticket.id).request, ModelRequest::class.java).operation)
    }
}
