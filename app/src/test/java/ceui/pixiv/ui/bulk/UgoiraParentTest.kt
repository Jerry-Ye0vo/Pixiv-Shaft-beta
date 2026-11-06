package ceui.pixiv.ui.bulk

import android.app.Application
import android.net.Uri
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.copy.*
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.ByteArrayOutputStream
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
class UgoiraParentTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "ugoira-parent-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var output: File
    private val gson = Gson()
    private val generated = AtomicInteger()
    private val opened = mutableListOf<CopyDestination>()
    private val recorded = mutableListOf<Pair<Boolean, String>>()
    private var actualVideo = false
    private var failRecord = false
    private var automatic = true
    private var entered: CompletableDeferred<Unit>? = null
    private var release: CompletableDeferred<Unit>? = null
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, database); queue = ContentPlanQueue(runtime, 1)
        output = File(app.cacheDir, "ugoira-${UUID.randomUUID()}")
        val provider = object : CopyOutput {
            override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
                opened += destination
                return StorageBackend.WriteHandle(Uri.fromFile(output), output.outputStream(), {}, {})
            }
            override fun resume(uri: String, destination: CopyDestination) = open(destination)
            override fun matches(uri: String, sha256: String, bytes: Long) = output.isFile && output.length() == bytes &&
                runBlocking { ArtifactPublisher.digest(output) == sha256 }
            override fun finishRecovered(uri: String, destination: CopyDestination) {}
            override fun available(intent: CopyIntent) = output.isFile
        }
        QueuedUgoira(app, queue, ContentPhaseRunner(runtime) { 1 }, provider, { automatic }, { _, _ -> },
            generate = { _, progress ->
                generated.incrementAndGet(); progress(UgoiraPhase.ENCODE)
                requireNotNull(UgoiraOutputSession.current.get()).capture(actualVideo, Pool.ENCODE) {
                    entered?.complete(Unit); release?.await(); it.write("generated-frames".toByteArray())
                }
            }, record = { _, _, video, name ->
                if (failRecord) throw java.io.IOException("history")
                recorded += video to name
            })
    }
    @After fun teardown() {
        release?.complete(Unit); runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database); output.delete()
        File(app.filesDir, "content_ugoira_inputs").deleteRecursively()
    }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }
    private fun request() = UgoiraSaveRequest(illustJson = "{\"id\":123,\"type\":\"ugoira\",\"page_count\":1}", preferVideo = true,
        gif = CopyDestination("cache", segments = listOf("frozen.gif"), mime = "image/gif", policy = "Rename"),
        video = CopyDestination("cache", segments = listOf("frozen.mp4"), mime = "video/mp4", policy = "Rename"))
    private fun submit() = queue.submit("ugoira-test", "ugoira-save") { gson.toJson(request()) }
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    @Test fun mp4FallbackUsesTheFrozenGifDestinationAndRecordsTheActualFormat() = scenario {
        val ticket = submit(); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(listOf(request().gif), opened); assertEquals(listOf(false to "frozen.gif"), recorded)
    }
    @Test fun aVideoResultUsesTheVideoDestinationAndItsCorrectMime() = scenario {
        actualVideo = true; assertEquals(State.SUCCEEDED, end(submit()).state)
        assertEquals("video/mp4", opened.single().mime); assertEquals(listOf(true to "frozen.mp4"), recorded)
    }
    @Test fun historyFailureRetriesTheSameCompleteInputAndOutputWithoutReencoding() = scenario {
        failRecord = true; val first = submit(); assertEquals(State.FAILED, end(first).state)
        failRecord = false; val retry = submit(); assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(first.id, retry.id); assertEquals(1, generated.get()); assertEquals(1, opened.size); assertEquals(1, recorded.size)
    }
    @Test fun explicitCancelWhileEncodingCannotPublishOrRecordAndWaitsForStageExit() = scenario {
        entered = CompletableDeferred(); release = CompletableDeferred()
        val ticket = submit(); entered!!.await(); assertTrue(ticket.cancel())
        assertEquals(State.CANCELLED, end(ticket).state); assertTrue(opened.isEmpty()); assertTrue(recorded.isEmpty())
        val next = runtime.store.enqueue("after", "after", Pool.ENCODE, "{}"); assertNotNull(runtime.store.claim(next, Pool.ENCODE, 1))
    }
    @Test fun detachingAnObserverDoesNotCancelTheProcessOwnedSave() = scenario {
        entered = CompletableDeferred(); release = CompletableDeferred()
        val ticket = submit(); entered!!.await()
        val observer = launch { end(ticket) }; observer.cancelAndJoin(); release!!.complete(Unit)
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(1, recorded.size)
    }
    @Test fun malformedFrameFailsEncodingRatherThanProducingASuccessWithMissingFrames() {
        val frame = File(app.cacheDir, "broken-${UUID.randomUUID()}").apply { writeText("invalid bitmap") }
        try {
            try { encodeFramesToGif(listOf(frame), listOf(60), ByteArrayOutputStream()); fail("Broken frame must fail") }
            catch (expected: java.io.IOException) { }
        } finally { frame.delete() }
    }
}
