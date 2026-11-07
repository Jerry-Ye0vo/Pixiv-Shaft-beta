package ceui.pixiv.snapshot

import android.app.Application
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.IOException
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
class SnapshotPlanSessionTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "snapshot-plan-${UUID.randomUUID()}.db"
    private val id = UUID.randomUUID().toString()
    private val gson = Gson()
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var phases: ContentPhaseRunner
    private lateinit var transfers: SnapshotAssetTransfers
    private val metadata = AtomicInteger()
    private val first = AtomicInteger()
    private val second = AtomicInteger()
    private val indexed = AtomicInteger()
    private var failSecond = false
    private var parallel = false
    private var entered: CompletableDeferred<Unit>? = null
    private var release: CompletableDeferred<Unit>? = null
    private val request get() = SnapshotRequest(snapshotId = id, createdAt = 12345, illustJson = "{\"id\":123,\"type\":\"illust\"}",
        comments = false, original = true, fresh = true, automatic = false)
    private fun directory() = SnapshotRepository.dir(app, id)
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, database); phases = ContentPhaseRunner(runtime) { 1 }
        transfers = SnapshotAssetTransfers(phases) { error("No unexpected source fetch") }
        createQueue()
    }
    private fun createQueue() {
        queue = ContentPlanQueue(runtime, 1)
        queue.register(object : ContentPlanHandler {
            override val kind = "snapshot-test"
            override suspend fun execute(context: ContentPlanContext) {
                val spec = gson.fromJson(context.plan.request, SnapshotRequest::class.java)
                val session = SnapshotPlanSession(context, spec, app, phases, transfers) { _, _ -> indexed.incrementAndGet() }
                if (session.recoverManifest()) return
                session.bean { metadata.incrementAndGet(); gson.fromJson(spec.illustJson, Illust::class.java) }
                if (parallel) coroutineScope {
                    (0 until 20).map { i -> async {
                        val file = File(session.directory, "images/$i")
                        session.resource("url:$i", file) { transfers.writeText(file, "image-$i") }
                    } }.awaitAll()
                } else {
                    val a = File(session.directory, "images/first")
                    session.resource("url:first", a) { first.incrementAndGet(); transfers.writeText(a, "one") }
                    val b = File(session.directory, "images/second")
                    session.resource("url:second", b) {
                        second.incrementAndGet(); entered?.complete(Unit); release?.await()
                        if (failSecond) throw IOException("source unavailable")
                        transfers.writeText(b, "two")
                    }
                }
                session.publish(mapOf("snapshotId" to spec.snapshotId, "createdAt" to spec.createdAt), false) { indexed.incrementAndGet() }
            }
        })
    }
    @After fun teardown() {
        release?.complete(Unit); runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database); directory().deleteRecursively()
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(12_000, body) }
    private fun submit() = queue.submit("snapshot-test:123", "snapshot-test") { gson.toJson(request) }
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    @Test fun retryKeepsTheSameSnapshotIdMetadataAndAlreadyVerifiedAssets() = scenario {
        failSecond = true; val original = submit(); assertEquals(State.FAILED, end(original).state)
        assertTrue(File(directory(), "images/first").isFile); assertFalse(File(directory(), SNAPSHOT_MANIFEST).exists())
        failSecond = false; val retry = submit(); assertEquals(State.SUCCEEDED, end(retry).state)
        assertEquals(original.id, retry.id); assertEquals(1, metadata.get()); assertEquals(1, first.get()); assertEquals(2, second.get())
        assertEquals(id, gson.fromJson(File(directory(), SNAPSHOT_MANIFEST).readText(), Map::class.java)["snapshotId"])
    }
    @Test fun parallelAssetReceiptsCannotOverwriteEachOthersCheckpoints() = scenario {
        parallel = true; val ticket = submit(); assertEquals(State.SUCCEEDED, end(ticket).state)
        val saved = gson.fromJson(queue.store.get(ticket.id).checkpoint, SnapshotCheckpoint::class.java)
        assertEquals(20, saved.resources.size); assertEquals(1, indexed.get())
        assertTrue(saved.resources.values.all { it.bytes > 0 && it.sha256.length == 64 })
    }
    @Test fun aCorruptOwnedResourceIsFetchedAgainInsteadOfTrustingItsFilename() = scenario {
        failSecond = true; assertEquals(State.FAILED, end(submit()).state)
        File(directory(), "images/first").writeText("bad")
        failSecond = false; assertEquals(State.SUCCEEDED, end(submit()).state)
        assertEquals("one", File(directory(), "images/first").readText()); assertEquals(2, first.get()); assertEquals(1, metadata.get())
    }
    @Test fun processQueueRecoveryReusesTheCompletedAssetsWithoutRefetchingTheirMetadata() = scenario {
        failSecond = true; val original = submit(); assertEquals(State.FAILED, end(original).state)
        queue.shutdown(); createQueue(); queue.ready.await(); failSecond = false
        assertEquals(State.SUCCEEDED, end(submit()).state); assertEquals(1, first.get()); assertEquals(1, metadata.get())
    }
    @Test fun aManifestCommittedBeforeTheParentAcknowledgementCanBeReconciledOffline() = scenario {
        val original = submit(); assertEquals(State.SUCCEEDED, end(original).state)
        val row = queue.store.get(original.id)
        queue.shutdown()
        // Simulate an interrupted parent acknowledgement with the same request/checkpoint/output.
        runtime.store.writableDatabase.execSQL("UPDATE plans SET state='QUEUED',attempt=NULL WHERE id=?", arrayOf(original.id))
        createQueue(); queue.ready.await()
        val restored = requireNotNull(queue.latest(row.family)); assertEquals(State.SUCCEEDED, end(restored).state)
        assertEquals(1, first.get()); assertEquals(1, second.get()); assertEquals(1, metadata.get()); assertEquals(2, indexed.get())
    }
    @Test fun cancellingAnAssetBeforeManifestPublicationCannotExposeACompleteSnapshot() = scenario {
        entered = CompletableDeferred(); release = CompletableDeferred(); val ticket = submit(); entered!!.await()
        assertTrue(ticket.cancel()); assertEquals(State.CANCELLED, end(ticket).state)
        assertFalse(File(directory(), SNAPSHOT_MANIFEST).exists()); assertEquals(0, indexed.get())
    }
    @Test fun directoryReceiptsRejectAnIncompleteOrModifiedUgoiraFrameSet() = scenario {
        queue.shutdown(); queue = ContentPlanQueue(runtime, 1)
        queue.register(object : ContentPlanHandler {
            override val kind = "directory"
            override suspend fun execute(context: ContentPlanContext) {
                val session = SnapshotPlanSession(context, request, app, phases, transfers) { _, _ -> }
                val dir = File(session.directory, "ugoira"); dir.mkdirs()
                File(dir, "frame.png").writeText("frame"); File(dir, "delays.txt").writeText("60")
                session.preserveDirectory(dir); assertTrue(session.directoryReady(dir))
                File(dir, "frame.png").writeText("bad"); assertFalse(session.directoryReady(dir))
                context.commit {}
            }
        })
        val ticket = queue.submit("directory", "directory") { "{}" }
        assertEquals(State.SUCCEEDED, end(ticket).state)
    }
    @Test fun resourcePathsCannotEscapeTheOwnedSnapshotDirectory() = scenario {
        queue.shutdown(); queue = ContentPlanQueue(runtime, 1)
        queue.register(object : ContentPlanHandler {
            override val kind = "path"
            override suspend fun execute(context: ContentPlanContext) {
                val session = SnapshotPlanSession(context, request, app, phases, transfers) { _, _ -> }
                session.resource("url", File(session.directory, "../outside")) { fail("Escaped path reached a writer") }
                context.commit {}
            }
        })
        assertEquals(State.FAILED, end(queue.submit("path", "path") { "{}" }).state)
    }
}
