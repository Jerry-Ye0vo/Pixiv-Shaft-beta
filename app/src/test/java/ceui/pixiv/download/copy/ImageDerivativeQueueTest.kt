package ceui.pixiv.download.copy

import android.app.Application
import android.net.Uri
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.filter.FilterAdjust
import com.google.gson.Gson
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ImageDerivativeQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "derivative-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var source: File
    private lateinit var output: File
    private lateinit var service: QueuedImageDerivatives
    private val renders = AtomicInteger()
    private val opens = AtomicInteger()
    private var failOpen = false
    private var failRender = false
    private var entered: CompletableDeferred<Unit>? = null
    private var release: CompletableDeferred<Unit>? = null
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, name); queue = ContentPlanQueue(runtime, 1)
        source = File(app.cacheDir, "source-${UUID.randomUUID()}").apply { writeText("original pixels") }
        output = File(app.cacheDir, "result-${UUID.randomUUID()}")
        val provider = object : CopyOutput {
            override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
                opens.incrementAndGet(); if (failOpen) throw java.io.IOException("provider unavailable")
                return StorageBackend.WriteHandle(Uri.fromFile(output), output.outputStream(), {}, {})
            }
            override fun resume(uri: String, destination: CopyDestination) = open(destination)
            override fun matches(uri: String, sha256: String, bytes: Long) = output.isFile && output.length() == bytes &&
                runBlocking { ArtifactPublisher.digest(output) == sha256 }
            override fun finishRecovered(uri: String, destination: CopyDestination) {}
            override fun available(intent: CopyIntent) = output.isFile
        }
        service = QueuedImageDerivatives(app, queue, ContentPhaseRunner(runtime) { 1 }, provider,
            local = { _, _ -> source }, render = { _, _, input, _, target ->
                assertTrue(runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
                    .any { it.pool == Pool.ENCODE && it.state == State.RUNNING })
                renders.incrementAndGet(); entered?.complete(Unit); release?.await()
                if (failRender) throw java.io.IOException("render failed")
                target.writeText("rendered:${input.readText()}"); "image/png"
            })
    }
    @After fun teardown() {
        release?.complete(Unit); runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(name)
        source.delete(); output.delete(); File(app.filesDir, "content_image_derivatives").deleteRecursively()
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(15_000, body) }
    private fun request(type: String = QueuedImageDerivatives.FILTER_SAVE, share: String? = null): ImageDerivativeRequest {
        val jpg = CopyDestination("cache", segments = listOf("images", "frozen", "image.jpg"), mime = "image/jpeg", policy = "Rename")
        return ImageDerivativeRequest(type = type, illustJson = "{\"id\":123,\"type\":\"illust\"}", page = 2,
            sourceUrl = "https://example/image.png", adjust = List(FilterAdjust.entries.size) { 0 }, jpg = jpg,
            png = jpg.copy(segments = listOf("images", "frozen", "image.png"), mime = "image/png"), shareRoot = share)
    }
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    @Test fun derivativeIntentAndOwnedSourceExistBeforeActualEncodingAndPublication() = scenario {
        val ticket = service.enqueue(request()); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertNotNull(queue.store.get(ticket.id)); assertEquals(1, renders.get()); assertEquals(1, opens.get())
        assertEquals("rendered:original pixels", output.readText())
        assertTrue(source.exists())
    }
    @Test fun aQueuedEncoderCanBeCancelledWithoutOpeningAUserOutput() = scenario {
        queue.ready.await()
        val id = runtime.store.enqueue("busy", "busy", Pool.ENCODE, "{}")
        val lease = runtime.store.claim(id, Pool.ENCODE, 1)
        val ticket = service.enqueue(request())
        withTimeout(3000) { while (runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
            .none { it.pool == Pool.ENCODE && it.state == State.QUEUED }) delay(10) }
        ticket.cancel(); assertEquals(State.CANCELLED, end(ticket).state)
        assertEquals(0, renders.get()); assertEquals(0, opens.get())
        runtime.store.finish(id, lease.attempt, true, null); runtime.changed()
    }
    @Test fun retryUsesItsFrozenPrivateSourceEvenIfTheOriginalCacheWasChanged() = scenario {
        failRender = true; val ticket = service.enqueue(request())
        assertEquals(State.FAILED, end(ticket).state)
        source.writeText("different pixels"); failRender = false; queue.resume(ticket.id)
        val result = ticket.status.first { it.state == State.SUCCEEDED }
        assertEquals(State.SUCCEEDED, result.state); assertEquals("rendered:original pixels", output.readText())
    }
    @Test fun aProviderRetryReusesTheEncodedArtifactWithoutRepeatingNativeWork() = scenario {
        failOpen = true; val ticket = service.enqueue(request()); assertEquals(State.FAILED, end(ticket).state)
        source.delete(); failOpen = false; queue.resume(ticket.id)
        ticket.status.first { it.state == State.SUCCEEDED }
        assertEquals(1, renders.get()); assertEquals("rendered:original pixels", output.readText())
    }
    @Test fun pngEncodingUsesTheFrozenPngNameAndMimeRatherThanAJpegSuffix() = scenario {
        val ticket = service.enqueue(request()); assertEquals(State.SUCCEEDED, end(ticket).state)
        val result = service.await(ticket); assertEquals("image/png", result.mime)
        val outer = Gson().fromJson(queue.store.get(ticket.id).checkpoint, ArtifactCheckpoint::class.java)
        assertEquals("image/png", Gson().fromJson(outer.domain, ImageDerivativeState::class.java).mime)
    }
    @Test fun aShareOutputSurvivesParentInputCleanupInItsFileProviderDirectory() = scenario {
        val spec = request(QueuedImageDerivatives.FILTER_SHARE, app.cacheDir.absolutePath)
        val ticket = service.enqueue(spec); assertEquals(State.SUCCEEDED, end(ticket).state)
        val result = service.await(ticket); val target = File(requireNotNull(result.uri.path))
        assertTrue(target.canonicalPath.startsWith(File(app.cacheDir, "images").canonicalPath))
        assertEquals("rendered:original pixels", target.readText()); target.delete()
    }
    @Test fun wallpaperPreparationCopiesBytesAndDoesNotTakeTheEncoder() = scenario {
        val ticket = service.enqueue(request(QueuedImageDerivatives.WALLPAPER))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(0, renders.get())
        assertEquals("original pixels", output.readText())
    }
    @Test fun cancellingAnObserverDoesNotCancelTheDurableImageExport() = scenario {
        entered = CompletableDeferred(); release = CompletableDeferred()
        val ticket = service.enqueue(request()); entered!!.await()
        val observer = launch { service.await(ticket) }; observer.cancelAndJoin()
        release!!.complete(Unit)
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(1, opens.get())
    }
}
