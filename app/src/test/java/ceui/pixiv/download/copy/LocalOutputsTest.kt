package ceui.pixiv.download.copy

import android.app.Application
import android.net.Uri
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
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
class LocalOutputsTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "local-output-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var outputs: QueuedLocalOutputs
    private lateinit var directory: File
    private lateinit var output: File
    private val opened = AtomicInteger()
    private val recorded = mutableListOf<LocalOutputRequest>()
    private var failRecord = false
    private var skip = false
    private val destination = CopyDestination("cache", segments = listOf("frozen.txt"), mime = "text/plain", policy = "Rename")
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, database)
        directory = File(app.cacheDir, "local-output-${UUID.randomUUID()}").apply { mkdirs() }
        output = File(directory, "published"); createQueue()
    }
    private fun createQueue() {
        queue = ContentPlanQueue(runtime, 1)
        val provider = object : CopyOutput {
            override fun open(destination: CopyDestination): StorageBackend.WriteHandle? {
                opened.incrementAndGet(); if (skip) return null
                return StorageBackend.WriteHandle(Uri.fromFile(output), output.outputStream(), {}, {})
            }
            override fun resume(uri: String, destination: CopyDestination) = open(destination)
            override fun matches(uri: String, sha256: String, bytes: Long) = output.isFile && output.length() == bytes &&
                runBlocking { ArtifactPublisher.digest(output) == sha256 }
            override fun finishRecovered(uri: String, destination: CopyDestination) {}
            override fun available(intent: CopyIntent) = output.isFile
        }
        outputs = QueuedLocalOutputs(app, queue, ContentPhaseRunner(runtime) { 1 }, provider) { request, _ ->
            if (failRecord) throw java.io.IOException("history")
            recorded += request
        }
    }
    @After fun teardown() {
        runBlocking { queue.shutdown(); outputs.shutdown() }; runtime.close(); app.deleteDatabase(database)
        directory.deleteRecursively(); File(app.filesDir, "content_local_outputs").deleteRecursively()
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(12_000, body) }
    private fun text() = LocalOutputRequest(type = QueuedLocalOutputs.CAPTION, subjectId = 123, content = "frozen caption", destination = destination)
    private fun file(file: File) = LocalOutputRequest(type = QueuedLocalOutputs.FILE, subjectId = 0, sourcePath = file.absolutePath,
        sourceBytes = file.length(), sourceModified = file.lastModified(), destination = destination)
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    private suspend fun eventually(test: () -> Boolean) { withTimeout(5_000) { while (!test()) delay(10) } }
    private suspend fun occupy(): ContentQueueStore.Task {
        runtime.initialize(); val id = runtime.store.enqueue("held", "held", Pool.COPY, "{}"); return runtime.store.claim(id, Pool.COPY, 1)!!
    }
    private fun free(lease: ContentQueueStore.Task) { runtime.store.finish(lease.id, lease.attempt, true, null); runtime.changed() }
    @Test fun aGenericTextExportPreservesItsFrozenContentWithoutCaptionOrBackupIdentity() = scenario {
        val request = text().copy(type = QueuedLocalOutputs.TEXT, content = "https://example/123\nhttps://example/456")
        val ticket = outputs.enqueue("links") { request }
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(request.content, output.readText()); assertEquals(QueuedLocalOutputs.TEXT, recorded.single().type)
    }
    @Test fun aGenericImageFileExportPreservesOriginalBytesAndItsSelectedDestination() = scenario {
        val source = File(directory, "image.png").apply { writeBytes(byteArrayOf(1, 2, 3, 4)) }
        val request = file(source).copy(type = QueuedLocalOutputs.FILE,
            destination = destination.copy(segments = listOf("images", "frame01.png"), mime = "image/png"))
        val ticket = outputs.enqueue("frame", source) { request }
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertArrayEquals(source.readBytes(), output.readBytes()); assertEquals("frame01.png", recorded.single().destination.relativePath().filename)
    }
    @Test fun aTextIntentIsDurableBeforeAnyProviderOrPreparationBytes() = scenario {
        val held = occupy(); val ticket = outputs.enqueue("caption") { text() }
        eventually { queue.store.get(ticket.id) != null }; assertEquals(0, opened.get()); assertFalse(output.exists())
        free(held); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals("frozen caption", output.readText()); assertEquals(destination, recorded.single().destination)
    }
    @Test fun localFileDeletionWaitsUntilItsPrivateInputHasBeenCaptured() = scenario {
        val source = File(directory, "source").apply { writeText("original bytes") }; val request = file(source)
        val held = occupy(); val ticket = outputs.enqueue("file", source) { request }
        eventually { queue.store.get(ticket.id) != null }
        CopyInputReferences.requestDelete(source).join(); assertTrue(source.isFile)
        free(held); assertEquals(State.SUCCEEDED, end(ticket).state)
        eventually { !source.exists() }; assertEquals("original bytes", output.readText())
    }
    @Test fun aChangedFileCannotBePublishedAsThePreviouslyFrozenInput() = scenario {
        val source = File(directory, "source").apply { writeText("original") }; val request = file(source)
        val held = occupy(); val ticket = outputs.enqueue("file", source) { request }
        eventually { queue.store.get(ticket.id) != null }; source.writeText("changed length")
        free(held); assertEquals(State.FAILED, end(ticket).state); assertEquals(0, opened.get()); assertTrue(recorded.isEmpty())
    }
    @Test fun emptyInputsFailBeforeOpeningAnOutput() = scenario {
        val source = File(directory, "empty").apply { writeText("") }
        val ticket = outputs.enqueue("empty", source) { file(source) }
        assertEquals(State.FAILED, end(ticket).state); assertEquals(0, opened.get())
    }
    @Test fun coldRetryCanReuseCapturedInputAndAnAlreadyWrittenReceiptWithoutItsOriginalFile() = scenario {
        val source = File(directory, "source").apply { writeText("backup") }; val request = file(source)
        failRecord = true; val first = outputs.enqueue("file", source) { request }; assertEquals(State.FAILED, end(first).state)
        queue.shutdown(); outputs.shutdown(); source.delete(); failRecord = false; createQueue(); queue.ready.await()
        val retry = outputs.enqueue("file") { request }; assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(1, opened.get()); assertEquals("backup", output.readText()); assertEquals(1, recorded.size)
    }
    @Test fun legacyProducerRunsOnlyInsideAnAdmittedCopyStage() = scenario {
        val source = File(directory, "generated"); val calls = AtomicInteger(); val token = UUID.randomUUID().toString()
        val ticket = outputs.enqueue("producer", producer = token to Consumer { target ->
            calls.incrementAndGet()
            assertTrue(runtime.store.tasksPage("phase:", 0, Long.MAX_VALUE, 100).any { it.pool == Pool.COPY && it.state == State.RUNNING })
            target.writeText("{\"generated\":true}")
        }) { LocalOutputRequest(type = QueuedLocalOutputs.BACKUP, subjectId = 0, destination = destination,
            sourcePath = source.absolutePath, producer = token) }
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(1, calls.get()); assertEquals("{\"generated\":true}", output.readText())
    }
    @Test fun aColdUncapturedProducerFailsVisiblyInsteadOfPretendingToReplayACallback() = scenario {
        val request = LocalOutputRequest(type = QueuedLocalOutputs.BACKUP, subjectId = 0, destination = destination,
            sourcePath = File(directory, "missing").absolutePath, producer = "old-process-token")
        val ticket = outputs.enqueue("cold-producer") { request }
        assertEquals(State.FAILED, end(ticket).state); assertTrue(ticket.status.value.error!!.contains("重新导出"))
        assertEquals(0, opened.get())
    }
    @Test fun aTruncatedBackupCannotBePublishedAsASuccessfulBackup() = scenario {
        val file = File(directory, "partial.json").apply { writeText("{\"history\":[") }
        val request = file(file).copy(type = QueuedLocalOutputs.BACKUP)
        val ticket = outputs.enqueue("invalid-backup", file) { request }
        assertEquals(State.FAILED, end(ticket).state); assertEquals(0, opened.get())
    }
    @Test fun anArrayBackupRemainsCompatibleWithOldMuteAndSynonymExports() = scenario {
        val file = File(directory, "array.json").apply { writeText("[{\"id\":123}]") }
        val request = file(file).copy(type = QueuedLocalOutputs.BACKUP)
        val ticket = outputs.enqueue("array-backup", file) { request }
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(file.readText(), output.readText())
    }
    @Test fun pausingBeforePreparationStillAllowsTheSameProcessProducerToResume() = scenario {
        val held = occupy(); val source = File(directory, "generated"); val calls = AtomicInteger(); val token = UUID.randomUUID().toString()
        val ticket = outputs.enqueue("producer", producer = token to Consumer { target -> calls.incrementAndGet(); target.writeText("{}") }) {
            LocalOutputRequest(type = QueuedLocalOutputs.BACKUP, subjectId = 0, destination = destination,
                sourcePath = source.absolutePath, producer = token)
        }
        eventually { queue.store.get(ticket.id) != null }; ticket.pause(); ticket.status.first { it.state == State.PAUSED }
        assertEquals(0, calls.get()); free(held); queue.resume(ticket.id)
        ticket.status.first { it.state == State.SUCCEEDED || it.state == State.FAILED }
        assertEquals(State.SUCCEEDED, ticket.status.value.state); assertEquals(1, calls.get())
    }
    @Test fun skippingAnExistingOutputDoesNotInventAHistoryRecord() = scenario {
        skip = true; val ticket = outputs.enqueue("skip") { text() }
        assertEquals(State.SUCCEEDED, end(ticket).state); assertTrue(outputs.receipt(ticket).skipped); assertTrue(recorded.isEmpty())
    }
    @Test fun cancellingABlockedOutputReleasesItsSourceReferenceWithoutWriting() = scenario {
        val held = occupy(); val source = File(directory, "source").apply { writeText("bytes") }
        val ticket = outputs.enqueue("cancel", source) { file(source) }
        eventually { queue.store.get(ticket.id) != null }; ticket.cancel(); ticket.status.first { it.state == State.CANCELLED }
        CopyInputReferences.requestDelete(source).join(); eventually { !source.exists() }
        assertEquals(0, opened.get()); free(held)
    }
    @Test fun detachingAProgressObserverDoesNotCancelTheQueuedOutput() = scenario {
        val held = occupy(); val ticket = outputs.enqueue("caption") { text() }
        val observer = launch { end(ticket) }; observer.cancelAndJoin(); free(held)
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals("frozen caption", output.readText())
    }
    @Test fun aTransferredCacheReferenceDoesNotDeadlockBehindAWaitingClear() = scenario {
        val source = File(directory, "source").apply { writeText("leased bytes") }; val request = file(source)
        val reference = ceui.pixiv.ui.bulk.UgoiraCacheAccess.acquireRead()
        val cleared = CompletableDeferred<Unit>()
        val writer = launch(start = CoroutineStart.UNDISPATCHED) {
            ceui.pixiv.ui.bulk.UgoiraCacheAccess.maintain(ContentPhaseRunner(runtime) { 1 }) {
                source.delete(); cleared.complete(Unit)
            }
        }
        val ticket = outputs.enqueue("cache-source", source, cacheReference = reference) { request }
        assertEquals(State.SUCCEEDED, end(ticket).state); writer.join()
        assertTrue(cleared.isCompleted); assertEquals("leased bytes", output.readText())
    }
}
