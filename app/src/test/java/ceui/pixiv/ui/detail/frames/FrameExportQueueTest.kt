package ceui.pixiv.ui.detail.frames

import android.app.Application
import android.net.Uri
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.copy.*
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.io.File
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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
class FrameExportQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "frames-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var locals: QueuedLocalOutputs
    private lateinit var service: QueuedUgoiraFrameExports
    private lateinit var directory: File
    private lateinit var frames: List<File>
    private val opens = AtomicInteger()
    private val gifs = AtomicInteger()
    private val videos = AtomicInteger()
    private var videoAvailable = false
    private var failOutput = false
    private var entered: CountDownLatch? = null
    private var release: CountDownLatch? = null
    @Before fun setup() {
        directory = File(app.cacheDir, "frames-${UUID.randomUUID()}").apply { mkdirs() }
        frames = (0..2).map { File(directory, "$it.jpg").apply { writeText("frame:$it") } }
        runtime = ContentQueueRuntime(app, name); queue = ContentPlanQueue(runtime, 1)
        val phases = ContentPhaseRunner(runtime) { 1 }
        val provider = object : CopyOutput {
            override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
                opens.incrementAndGet(); if (failOutput) throw java.io.IOException("storage full")
                val target = File(directory, "out-${destination.relativePath().filename}")
                return StorageBackend.WriteHandle(Uri.fromFile(target), target.outputStream(), onFinish = {
                    entered?.countDown(); release?.let { check(it.await(5, TimeUnit.SECONDS)) }
                }, onAbort = {})
            }
            override fun resume(uri: String, destination: CopyDestination) = open(destination)
            override fun matches(uri: String, sha256: String, bytes: Long): Boolean {
                val file = File(requireNotNull(Uri.parse(uri).path))
                return file.isFile && file.length() == bytes && runBlocking { ArtifactPublisher.digest(file) == sha256 }
            }
            override fun finishRecovered(uri: String, destination: CopyDestination) {}
            override fun available(intent: CopyIntent) = false
        }
        locals = QueuedLocalOutputs(app, queue, phases, provider) { _, _ -> }
        service = QueuedUgoiraFrameExports(app, queue, phases, locals, provider, reload = { error("No unexpected network fetch") },
            videoEncoder = { target, _, _, _ -> phases.run("fixture", "video", "fixture-video", Pool.ENCODE) {
                videos.incrementAndGet()
                if (videoAvailable) target.apply { writeText("mp4") } else null
            } }, gifEncoder = { files, delays, output, progress ->
                assertEquals(listOf(60, 70), delays)
                assertTrue(runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
                    .any { it.state == State.RUNNING && it.pool == Pool.ENCODE })
                gifs.incrementAndGet(); output.write(files.joinToString("|") { it.readText() }.toByteArray()); progress(100)
            })
    }
    @After fun teardown() {
        release?.countDown(); runBlocking { queue.shutdown(); locals.shutdown() }
        runtime.close(); app.deleteDatabase(name); directory.deleteRecursively()
        File(app.filesDir, "content_frame_exports").deleteRecursively(); File(app.filesDir, "content_local_outputs").deleteRecursively()
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(15_000, body) }
    private fun spec(group: Boolean, video: Boolean = false, share: Boolean = false): FrameExportRequest {
        fun destination(name: String, mime: String) = CopyDestination("cache", segments = listOf("images", "selected", name), mime = mime, policy = "Replace")
        return FrameExportRequest(illustJson = "{\"id\":123,\"type\":\"ugoira\"}", indices = listOf(0, 1), paths = frames.take(2).map { it.absolutePath },
            delays = listOf(60, 70), destinations = if (group) listOf(destination("frame0.jpg", "image/jpeg"), destination("frame1.jpg", "image/jpeg"))
            else listOf(destination("clip.gif", "image/gif")), video = if (group) null else destination("clip.mp4", "video/mp4"), preferVideo = video,
            shareRoot = if (share) app.cacheDir.absolutePath else null, offlineDir = directory.absolutePath)
    }
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    @Test fun selectedFramesAreOneDurableGroupAndNeverUseAnEncoder() = scenario {
        val ticket = service.enqueue(spec(true), true); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(2, opens.get()); assertEquals(0, gifs.get()); assertEquals(0, videos.get())
        assertEquals("frame:0", File(directory, "out-frame0.jpg").readText()); assertEquals("frame:1", File(directory, "out-frame1.jpg").readText())
        assertEquals(2, queue.store.children(ticket.id).size)
    }
    @Test fun selectedFrameSharingUsesIndependentFileProviderCopies() = scenario {
        val ticket = service.enqueue(spec(true, share = true), true); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals("frame:0", File(app.cacheDir, "images/selected/frame0.jpg").readText())
        assertEquals("frame:1", File(app.cacheDir, "images/selected/frame1.jpg").readText())
        File(app.cacheDir, "images/selected").deleteRecursively()
    }
    @Test fun gifClipKeepsSelectedFrameOrderAndFrozenDelays() = scenario {
        val ticket = service.enqueue(spec(false), false); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals("frame:0|frame:1", File(directory, "out-clip.gif").readText()); assertEquals(1, gifs.get())
    }
    @Test fun unsupportedMp4FallsBackToGifWithTheCorrectDestinationAndMime() = scenario {
        val ticket = service.enqueue(spec(false, video = true), false); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(1, videos.get()); assertEquals(1, gifs.get())
        assertTrue(File(directory, "out-clip.gif").isFile); assertFalse(File(directory, "out-clip.mp4").exists())
    }
    @Test fun supportedMp4PublishesTheVideoWithoutRunningTheGifEncoder() = scenario {
        videoAvailable = true; val ticket = service.enqueue(spec(false, video = true), false)
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(0, gifs.get())
        assertEquals("mp4", File(directory, "out-clip.mp4").readText())
    }
    @Test fun outputRetryKeepsEncodedClipEvenAfterTheOriginalFrameCacheDisappears() = scenario {
        failOutput = true; val ticket = service.enqueue(spec(false), false); assertEquals(State.FAILED, end(ticket).state)
        frames.forEach { it.delete() }; failOutput = false; queue.resume(ticket.id)
        ticket.status.first { it.state == State.SUCCEEDED }
        assertEquals(1, gifs.get()); assertEquals("frame:0|frame:1", File(directory, "out-clip.gif").readText())
    }
    @Test fun aGroupCanPauseWhileAChildFinishesAndResumeWithoutPublishingThatFrameTwice() = scenario {
        entered = CountDownLatch(1); release = CountDownLatch(1)
        val ticket = service.enqueue(spec(true), true)
        withContext(Dispatchers.IO) { check(entered!!.await(5, TimeUnit.SECONDS)) }
        assertTrue(ticket.pause()); release!!.countDown()
        assertEquals(State.PAUSED, end(ticket).state)
        entered = null; release = null; queue.resume(ticket.id)
        ticket.status.first { it.state == State.SUCCEEDED }
        assertEquals(2, opens.get())
    }
    @Test fun missingOfflineFrameFailsVisiblyBeforeAnyUserOutput() = scenario {
        frames[0].delete(); val ticket = service.enqueue(spec(true), true)
        assertEquals(State.FAILED, end(ticket).state); assertEquals(0, opens.get())
    }
}
