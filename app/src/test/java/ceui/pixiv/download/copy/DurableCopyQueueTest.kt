package ceui.pixiv.download.copy

import android.app.Application
import android.net.Uri
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import java.io.File
import java.io.FileOutputStream
import java.io.FilterOutputStream
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class DurableCopyQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "copies-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: DurableCopyQueue
    private lateinit var root: File
    private lateinit var source: File
    private lateinit var output: FilesOutput
    private val gates = mutableListOf<CountDownLatch>()
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }

    @Before fun setup() {
        root = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        source = File(root, "input.png").apply { writeText("immutable translated pixels") }
        runtime = ContentQueueRuntime(app, database)
        output = FilesOutput(root)
        queue = DurableCopyQueue(app, runtime, output, { 1 })
    }
    @After fun cleanup() {
        gates.forEach { it.countDown() }
        runBlocking { queue.shutdown() }
        runtime.close(); app.deleteDatabase(database)
        root.deleteRecursively()
        File(app.filesDir, "content_queue_sources").deleteRecursively()
    }
    private fun request(file: File = source, name: String = "translated.png") = CopyRequest(
        "translation", 1, 0, file.absolutePath, file.length(), file.lastModified(), "{}",
        CopyDestination("cache", segments = listOf(name), mime = "image/png", policy = "Rename"))
    private fun submit(file: File = source, name: String = "translated.png"): CopyTicket {
        val initial = request(file, name)
        return queue.submit(initial.family, file) { request(file, name) }
    }
    private suspend fun end(ticket: CopyTicket) = ticket.status.first { it.state in setOf(State.SUCCEEDED, State.FAILED, State.CANCELLED) }
    private suspend fun eventually(test: () -> Boolean) = withTimeout(5_000) { while (!test()) delay(10) }

    @Test fun durableAdmissionPrecedesAnyOutputAndQueuedCancellationWritesNothing() = scenario {
        queue.ready.await()
        val other = runtime.store.enqueue("occupied", "occupied", Pool.COPY, "x")
        val held = runtime.store.claim(other, Pool.COPY, 1)
        val ticket = submit()
        eventually { runtime.store.get(ticket.id)?.state == State.QUEUED }
        assertEquals(0, output.opens.get())
        assertTrue(ticket.cancel())
        assertEquals(State.CANCELLED, end(ticket).state)
        assertEquals(State.CANCELLED, runtime.store.get(ticket.id).state)
        assertEquals(0, output.opens.get())
        assertTrue(source.exists())
        runtime.store.finish(other, held.attempt, true, null)
    }

    @Test fun translationCacheEvictionCannotRemoveAQueuedInputBeforeItIsCaptured() = scenario {
        queue.ready.await()
        val other = runtime.store.enqueue("occupied", "occupied", Pool.COPY, "x")
        val held = runtime.store.claim(other, Pool.COPY, 1)
        val ticket = submit()
        eventually { runtime.store.get(ticket.id)?.state == State.QUEUED }
        CopyInputReferences.requestDelete(source).join()
        assertTrue(source.exists())
        runtime.store.finish(other, held.attempt, true, null); runtime.changed()
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals("immutable translated pixels", output.files.single().readText())
        eventually { !source.exists() }
    }

    @Test fun outputWritingCannotBeCancelledOrReleaseCapacityBeforeItExits() = scenario {
        queue.ready.await()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1).also { gates += it }
        output.beforeWrite = { entered.countDown(); check(release.await(5, TimeUnit.SECONDS)) }
        val first = submit()
        withContext(Dispatchers.IO) { check(entered.await(5, TimeUnit.SECONDS)) }
        assertFalse(first.cancel())
        val other = runtime.store.enqueue("occupied", "occupied", Pool.COPY, "x")
        assertNull(runtime.store.claim(other, Pool.COPY, 1))
        release.countDown()
        assertEquals(State.SUCCEEDED, end(first).state)
        val claimed = runtime.store.claim(other, Pool.COPY, 1)
        assertNotNull(claimed)
        runtime.store.finish(other, claimed.attempt, true, null)
    }

    @Test fun failedStorageCommitKeepsSnapshotAndRetryWorksAfterOriginalCacheDisappears() = scenario {
        queue.ready.await()
        output.failFinish.set(1)
        val initial = submit()
        assertEquals(State.FAILED, end(initial).state)
        assertEquals(1, output.aborts.get())
        val record = Gson().fromJson(runtime.store.get(initial.id).payload, CopyIntent::class.java)
        assertTrue(File(app.filesDir, "content_queue_sources/${record.snapshotName}").isFile)
        source.delete()
        val retry = submit()
        assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(2, output.opens.get())
        assertEquals("immutable translated pixels", output.files.last().readText())
        assertTrue(DurableCopyQueue.savedKey(record.request) in queue.savedSources.value)
    }

    @Test fun restartReusesAndFinalizesACompleteUnacknowledgedOutputWithoutOpeningADuplicate() = scenario {
        queue.ready.await()
        queue.shutdown()
        val req = request()
        val id = "copy:" + UUID.randomUUID()
        val name = id.substringAfterLast(':') + ".source"
        File(app.filesDir, "content_queue_sources").mkdirs()
        val captured = File(app.filesDir, "content_queue_sources/$name").apply { source.copyTo(this) }
        val target = File(root, "receipt.png").apply { source.copyTo(this) }
        val intent = CopyIntent(request = req, snapshotName = name, sourceHash = DurableCopyQueue.hash(captured),
            sourceBytes = captured.length(), receiptUri = Uri.fromFile(target).toString())
        runtime.store.admit(id, req.family, "output-test", Pool.COPY, Gson().toJson(intent), State.QUEUED, false)
        val old = runtime.store.claim(id, Pool.COPY, 1)
        runtime.close(); source.delete()
        runtime = ContentQueueRuntime(app, database)
        queue = DurableCopyQueue(app, runtime, output, { 1 })
        queue.ready.await()
        eventually { queue.states.value[id]?.state == State.SUCCEEDED }
        assertEquals(0, output.opens.get())
        assertEquals(1, output.recovered.get())
        assertEquals("immutable translated pixels", target.readText())
        assertFalse(captured.exists())
        assertFalse(runtime.store.checkpoint(id, old.attempt, "stale"))
    }

    @Test fun savedFlagIsPublishedBeforeSuccessfulUiState() = scenario {
        queue.ready.await()
        val ticket = submit()
        val terminal = end(ticket)
        assertEquals(State.SUCCEEDED, terminal.state)
        assertTrue(DurableCopyQueue.savedKey(terminal.request!!) in queue.savedSources.value)
    }

    @Test fun rejectedAdmissionNeverCreatesOutputOrRunsDirectly() = scenario {
        queue.ready.await()
        runtime.store.writableDatabase.execSQL("CREATE TRIGGER reject_copy BEFORE INSERT ON tasks BEGIN SELECT RAISE(ABORT,'rejected'); END")
        val ticket = submit()
        assertEquals(State.FAILED, end(ticket).state)
        assertNull(runtime.store.get(ticket.id))
        assertEquals(0, output.opens.get())
    }

    @Test fun differentInputsForOneFrozenDestinationNeverOpenItAtTheSameTime() = scenario {
        queue.ready.await()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1).also { gates += it }
        output.beforeWrite = { entered.countDown(); check(release.await(5, TimeUnit.SECONDS)) }
        val first = submit()
        withContext(Dispatchers.IO) { check(entered.await(5, TimeUnit.SECONDS)) }
        val secondSource = File(root, "new-input.png").apply { writeText("new translated pixels") }
        val second = submit(secondSource)
        eventually { runtime.store.get(second.id)?.state == State.QUEUED }
        assertEquals(1, output.opens.get())
        release.countDown()
        assertEquals(State.SUCCEEDED, end(first).state)
        assertEquals(State.SUCCEEDED, end(second).state)
        assertEquals(2, output.opens.get())
    }

    private class FilesOutput(private val root: File) : CopyOutput {
        val opens = AtomicInteger(); val aborts = AtomicInteger(); val recovered = AtomicInteger(); val failFinish = AtomicInteger()
        val files = java.util.concurrent.CopyOnWriteArrayList<File>()
        @Volatile var beforeWrite: () -> Unit = {}
        override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
            val file = File(root, "output-${opens.incrementAndGet()}.png"); files += file
            val stream = object : FilterOutputStream(FileOutputStream(file)) {
                private var first = true
                override fun write(bytes: ByteArray, off: Int, length: Int) {
                    if (first) { first = false; beforeWrite() }
                    out.write(bytes, off, length)
                }
            }
            return StorageBackend.WriteHandle(Uri.fromFile(file), stream, onFinish = {
                if (failFinish.getAndUpdate { if (it > 0) it - 1 else 0 } > 0) throw java.io.IOException("commit failed")
            }, onAbort = { aborts.incrementAndGet(); file.delete() })
        }
        override fun resume(uri: String, destination: CopyDestination): StorageBackend.WriteHandle? {
            val file = File(Uri.parse(uri).path!!)
            return if (file.exists()) StorageBackend.WriteHandle(Uri.fromFile(file), FileOutputStream(file),
                onFinish = { recovered.incrementAndGet() }, onAbort = { file.delete() }) else null
        }
        override fun matches(uri: String, sha256: String, bytes: Long): Boolean = File(Uri.parse(uri).path!!).let {
            it.isFile && it.length() == bytes && DurableCopyQueue.hash(it) == sha256
        }
        override fun finishRecovered(uri: String, destination: CopyDestination) { recovered.incrementAndGet() }
        override fun available(intent: CopyIntent): Boolean = intent.receiptUri?.let { File(Uri.parse(it).path!!).length() > 0 } ?: false
    }
}
