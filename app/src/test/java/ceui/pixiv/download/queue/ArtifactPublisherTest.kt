package ceui.pixiv.download.queue

import android.app.Application
import android.net.Uri
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.copy.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.FilterOutputStream
import java.io.IOException
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

/** Real parent/store/phase integration, with provider failure points and actual filesystem bytes. */
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ArtifactPublisherTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "artifact-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var source: File
    private lateinit var output: File
    private val opened = AtomicInteger()
    private val recorded = AtomicInteger()
    private val aborted = AtomicInteger()
    private val recovered = AtomicInteger()
    private var failRecord = false
    private var failWrite = false
    private var skip = false
    private var writing: CountDownLatch? = null
    private var release: CountDownLatch? = null
    private val destination = CopyDestination("cache", segments = listOf("book.txt"), mime = "text/plain", policy = "Rename")
    private lateinit var publisher: ArtifactPublisher
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, database); queue = ContentPlanQueue(runtime, 1)
        source = File(app.filesDir, "source-${UUID.randomUUID()}").apply { writeText("complete artifact") }
        output = File(app.cacheDir, "output-${UUID.randomUUID()}")
        val provider = object : CopyOutput {
            override fun open(destination: CopyDestination): StorageBackend.WriteHandle? {
                opened.incrementAndGet(); if (skip) return null
                return handle()
            }
            override fun resume(uri: String, destination: CopyDestination) = if (output.isFile) handle() else null
            override fun matches(uri: String, sha256: String, bytes: Long) = output.isFile && output.length() == bytes &&
                runBlocking { ArtifactPublisher.digest(output) == sha256 }
            override fun finishRecovered(uri: String, destination: CopyDestination) { recovered.incrementAndGet() }
            override fun available(intent: CopyIntent) = output.isFile
            private fun handle(): StorageBackend.WriteHandle = StorageBackend.WriteHandle(Uri.fromFile(output),
                object : FilterOutputStream(output.outputStream()) {
                    override fun write(bytes: ByteArray, offset: Int, count: Int) {
                        val rows = queue.store.activePage(0, Long.MAX_VALUE, 20)
                        val checkpoint = Gson().fromJson(rows.single().checkpoint, ArtifactCheckpoint::class.java)
                        assertEquals(Uri.fromFile(output).toString(), checkpoint.receipt)
                        writing?.countDown(); release?.let { check(it.await(5, TimeUnit.SECONDS)) }
                        if (failWrite) throw IOException("provider-write")
                        out.write(bytes, offset, count)
                    }
                }, onFinish = {}, onAbort = { aborted.incrementAndGet() })
        }
        publisher = ArtifactPublisher(ContentPhaseRunner(runtime) { 1 }, provider)
        register()
    }
    private fun register() {
        queue.register(object : ContentPlanHandler {
            override val kind = "artifact"
            override suspend fun reconcile(plan: ContentPlanStore.Plan) = publisher.reconcile(plan, destination) {
                if (failRecord) throw IOException("history-write")
                recorded.incrementAndGet()
            }
            override suspend fun execute(context: ContentPlanContext) {
                publisher.publish(context, source, destination) {
                    if (failRecord) throw IOException("history-write")
                    recorded.incrementAndGet()
                }
            }
        })
    }
    @After fun teardown() { release?.countDown(); runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database); source.delete(); output.delete() }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(12_000, block) }
    private fun submit() = queue.submit("book", "artifact") { "{\"version\":1}" }
    private suspend fun end(ticket: ContentPlanTicket) = withTimeout(8_000) { ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END } }
    private suspend fun eventually(test: () -> Boolean) { withTimeout(5_000) { while (!test()) delay(10) } }
    @Test fun cancellingAfterAnUnacknowledgedPhysicalOutputReconcilesInsteadOfLosingItsHistory() = scenario {
        failRecord = true
        val ticket = submit(); assertEquals(State.FAILED, end(ticket).state)
        failRecord = false; assertTrue(ticket.cancel())
        ticket.status.first { it.state == State.SUCCEEDED }
        assertEquals(1, opened.get()); assertEquals(1, recorded.get()); assertEquals(source.readText(), output.readText())
    }
    @Test fun aChangedOutputNeverPassesReconciliationOrGetsOverwrittenByCancellation() = scenario {
        failRecord = true; val ticket = submit(); assertEquals(State.FAILED, end(ticket).state)
        output.writeText("unrelated bytes"); failRecord = false; ticket.cancel()
        ticket.status.first { it.state == State.CANCELLED }
        assertEquals("unrelated bytes", output.readText()); assertEquals(0, recorded.get()); assertEquals(1, opened.get())
    }
    @Test fun aColdCancelledReceiptIsValidatedWithoutGeneratingOrOpeningAnotherOutput() = scenario {
        val original = submit(); assertEquals(State.SUCCEEDED, end(original).state)
        queue.shutdown()
        runtime.store.writableDatabase.execSQL("UPDATE plans SET state='CANCELLED',attempt=NULL WHERE id=?", arrayOf(original.id))
        queue = ContentPlanQueue(runtime, 1); register(); queue.ready.await()
        eventually { queue.ticket(original.id)?.status?.value?.state == State.SUCCEEDED }
        assertEquals(1, opened.get()); assertEquals(2, recorded.get())
    }
    @Test fun aSaturatedCopyPoolDoesNotOpenAProviderAndAcceptedCancellationNeverWrites() = scenario {
        runtime.initialize(); val id = runtime.store.enqueue("blocker", "blocker", Pool.COPY, "{}"); val lease = runtime.store.claim(id, Pool.COPY, 1)!!
        val ticket = submit(); eventually { runtime.store.tasksPage("phase:", 0, Long.MAX_VALUE, 20).isNotEmpty() }
        assertNotNull(queue.store.get(ticket.id)); assertEquals(0, opened.get())
        ticket.cancel(); assertEquals(State.CANCELLED, end(ticket).state); assertFalse(output.exists())
        runtime.store.cancel(id); runtime.store.finish(id, lease.attempt, false, "test"); runtime.changed()
    }
    @Test fun outputReceiptExistsBeforeAnyBytesAndHistoryFollowsSuccessfulFinish() = scenario {
        val ticket = submit(); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(source.readText(), output.readText()); assertEquals(1, recorded.get()); assertEquals(1, opened.get())
        val checkpoint = Gson().fromJson(queue.store.get(ticket.id).checkpoint, ArtifactCheckpoint::class.java)
        assertTrue(checkpoint.outputCommitted); assertEquals(source.length(), checkpoint.bytes)
    }
    @Test fun aFailedHistoryCommitReusesTheExactPhysicalOutputWithoutOpeningAnotherFile() = scenario {
        failRecord = true; val first = submit(); assertEquals(State.FAILED, end(first).state)
        assertEquals(source.readText(), output.readText()); assertEquals(0, recorded.get())
        failRecord = false; val retry = submit(); assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(first.id, retry.id); assertEquals(1, opened.get()); assertEquals(1, recovered.get()); assertEquals(1, recorded.get())
    }
    @Test fun providerWriteFailureAbortsAndRetainsSourceForExplicitRetry() = scenario {
        failWrite = true; val first = submit(); assertEquals(State.FAILED, end(first).state)
        assertEquals(1, aborted.get()); assertEquals(0, recorded.get()); assertTrue(source.isFile)
        failWrite = false; val retry = submit(); assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(source.readText(), output.readText()); assertEquals(1, opened.get()); assertEquals(1, recorded.get())
    }
    @Test fun aLateCancelCannotTurnAnAlreadyWritingOutputIntoAnUnrecordedSuccess() = scenario {
        writing = CountDownLatch(1); release = CountDownLatch(1)
        val ticket = submit(); withContext(Dispatchers.IO) { check(writing!!.await(5, TimeUnit.SECONDS)) }
        assertFalse(ticket.cancel()); release!!.countDown()
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(1, recorded.get())
    }
    @Test fun skipPolicyCommitsASkippedReceiptWithoutCreatingHistory() = scenario {
        skip = true; val ticket = submit(); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertFalse(output.exists()); assertEquals(0, recorded.get())
        assertTrue(Gson().fromJson(queue.store.get(ticket.id).checkpoint, ArtifactCheckpoint::class.java).skipped)
    }
    @Test fun missingGeneratedInputCannotOpenAnyProvider() = scenario {
        source.delete(); val ticket = submit(); assertEquals(State.FAILED, end(ticket).state)
        assertEquals(0, opened.get()); assertEquals(0, recorded.get())
    }
    @Test fun anInputChangedAfterOpeningCannotBeWrittenUnderTheOldReceipt() = scenario {
        failRecord = true; val first = submit(); assertEquals(State.FAILED, end(first).state)
        val original = output.readText(); source.writeText("other input"); failRecord = false
        assertEquals(State.FAILED, end(submit()).state); assertEquals(original, output.readText()); assertEquals(1, opened.get())
    }
}
