package ceui.pixiv.snapshot

import android.app.Application
import android.net.Uri
import ceui.lisa.activities.Shaft
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.util.UUID
import java.util.zip.ZipFile
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
class SnapshotOperationsTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "snapshot-operations-${UUID.randomUUID()}.db"
    private val id = UUID.randomUUID().toString()
    private val gson = Gson()
    private var previousGson: Gson? = null
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var operations: QueuedSnapshotOperations
    private lateinit var scratch: File
    @Before fun setup() {
        previousGson = Shaft.sGson; Shaft.sGson = gson
        runtime = ContentQueueRuntime(app, database)
        scratch = File(app.cacheDir, "snapshot-op-test-${UUID.randomUUID()}").apply { mkdirs() }
        createQueue()
    }
    private fun createQueue() {
        queue = ContentPlanQueue(runtime, 1)
        operations = QueuedSnapshotOperations(app, queue, ContentPhaseRunner(runtime) { 1 }, recoverLegacy = false)
    }
    @After fun teardown() {
        runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database)
        target().deleteRecursively(); scratch.deleteRecursively()
        SnapshotRepository.root(app).listFiles()?.filter { it.name.contains(id) || it.name.startsWith(".queue-import-") }?.forEach { it.deleteRecursively() }
        File(app.filesDir, "content_snapshot_operations").deleteRecursively()
        SnapshotIndex.onSnapshotDeleted(id); Shaft.sGson = previousGson
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(18_000, body) }
    private fun target() = SnapshotRepository.dir(app, id)
    private fun snapshot(directory: File, title: String = "new", automatic: Boolean = false): File {
        directory.mkdirs(); File(directory, "images").mkdirs(); File(directory, "images/p0.jpg").writeText(title)
        File(directory, SNAPSHOT_ILLUST_JSON).writeText("{\"id\":123,\"type\":\"illust\",\"page_count\":1}")
        File(directory, SNAPSHOT_ASSETS_JSON).writeText("{\"assets\":{}}")
        if (automatic) File(directory, AUTO_SNAPSHOT_MANIFEST).writeText(gson.toJson(AutoSnapshotManifest(snapshotId = id, illustId = 123,
            title = title, coverPath = "images/p0.jpg")))
        else File(directory, SNAPSHOT_MANIFEST).writeText(gson.toJson(SnapshotManifest(snapshotId = id, illustId = 123,
            title = title, coverPath = "images/p0.jpg")))
        return directory
    }
    private fun archive(missing: Boolean = false): File {
        val directory = snapshot(File(scratch, "source-${UUID.randomUUID()}"))
        if (missing) File(directory, "images/p0.jpg").delete()
        return File(scratch, "archive-${UUID.randomUUID()}.shaftsnap").apply {
            outputStream().use { SnapshotArchive.zipDirectory(directory, it) }
        }
    }
    private fun import(file: File) = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.IMPORT,
        snapshotId = null, uri = Uri.fromFile(file).toString()))
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    private suspend fun eventually(test: () -> Boolean) { withTimeout(7_000) { while (!test()) delay(10) } }
    private fun checkpoint(ticket: ContentPlanTicket) = gson.fromJson(queue.store.get(ticket.id).checkpoint, ArtifactCheckpoint::class.java)
    private fun domain(ticket: ContentPlanTicket) = gson.fromJson(checkpoint(ticket).domain, SnapshotOperationState::class.java)
    private fun encodeCount() = runtime.store.tasksPage("phase:", 0, Long.MAX_VALUE, 200).count { it.pool == Pool.ENCODE }

    @Test fun validImportReplacesOnlyAfterValidationAndRetainsTheExistingSnapshotSchema() = scenario {
        snapshot(target(), "previous"); val ticket = import(archive())
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals("new", File(target(), "images/p0.jpg").readText())
        assertEquals(SNAPSHOT_SCHEMA_VERSION, SnapshotRepository.readManifest(app, id)!!.schemaVersion)
        assertEquals(id, gson.fromJson(domain(ticket).manifestJson, SnapshotManifest::class.java).snapshotId)
        assertTrue(runtime.store.tasksPage("phase:", 0, Long.MAX_VALUE, 200).none { it.pool == Pool.NETWORK })
    }
    @Test fun incompleteImportCannotRemoveAnExistingSnapshot() = scenario {
        snapshot(target(), "previous"); val ticket = import(archive(missing = true))
        assertEquals(State.FAILED, end(ticket).state)
        assertEquals("previous", File(target(), "images/p0.jpg").readText())
        assertEquals("previous", SnapshotRepository.readManifest(app, id)!!.title)
    }
    @Test fun invalidZipCannotRemoveAnExistingSnapshot() = scenario {
        snapshot(target(), "previous"); val input = File(scratch, "invalid.zip").apply { writeText("not a ZIP") }
        assertEquals(State.FAILED, end(import(input)).state)
        assertEquals("previous", File(target(), "images/p0.jpg").readText())
    }
    @Test fun committedDirectoryCanBeRecoveredWithoutReopeningTheOriginalDocument() = scenario {
        val input = archive(); val first = import(input); assertEquals(State.SUCCEEDED, end(first).state)
        queue.shutdown(); input.delete()
        runtime.store.writableDatabase.execSQL("UPDATE plans SET state='QUEUED',attempt=NULL WHERE id=?", arrayOf(first.id))
        createQueue(); queue.ready.await()
        assertEquals(State.SUCCEEDED, end(requireNotNull(queue.ticket(first.id))).state)
        assertEquals("new", File(target(), "images/p0.jpg").readText())
    }
    @Test fun replacementWaitsForArchiveReadersWithoutHoldingCopyCapacity() = scenario {
        snapshot(target(), "previous"); val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val reader = launch { ContentResourceLeases.read("snapshot:$id") { entered.complete(Unit); release.await() } }
        entered.await(); val ticket = import(archive())
        eventually { queue.store.get(ticket.id)?.checkpoint?.let { raw ->
            runCatching { gson.fromJson(gson.fromJson(raw, ArtifactCheckpoint::class.java).domain, SnapshotOperationState::class.java).prepared }.getOrDefault(false)
        } == true }
        assertEquals("previous", File(target(), "images/p0.jpg").readText())
        val free = runtime.store.enqueue("free-copy", "free-copy", Pool.COPY, "{}"); val lease = runtime.store.claim(free, Pool.COPY, 1)
        assertNotNull(lease); runtime.store.finish(free, lease!!.attempt, true, null); runtime.changed()
        release.complete(Unit); reader.join()
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals("new", File(target(), "images/p0.jpg").readText())
    }
    @Test fun exportUsesTheSelectedDocumentAndItsStableProviderReceipt() = scenario {
        snapshot(target()); val document = File(scratch, "export.shaftsnap")
        val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.EXPORT, snapshotId = id,
            uri = Uri.fromFile(document).toString(), fileName = "frozen.shaftsnap"))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(Uri.fromFile(document).toString(), checkpoint(ticket).receipt)
        ZipFile(document).use { zip -> assertNotNull(zip.getEntry(SNAPSHOT_MANIFEST)); assertNotNull(zip.getEntry("images/p0.jpg")) }
    }
    @Test fun failedExportCanRetryItsPreservedArchiveWithoutCompressingAgain() = scenario {
        snapshot(target()); val parent = File(scratch, "blocked").apply { writeText("file") }; val document = File(parent, "export.shaftsnap")
        val request = SnapshotOperationRequest(operation = QueuedSnapshotOperations.EXPORT, snapshotId = id,
            uri = Uri.fromFile(document).toString(), fileName = "frozen.shaftsnap")
        val first = operations.enqueue(request); assertEquals(State.FAILED, end(first).state); val encodes = encodeCount()
        parent.delete(); parent.mkdirs(); val retry = operations.enqueue(request)
        assertEquals(State.SUCCEEDED, end(retry).state); assertEquals(encodes, encodeCount()); assertTrue(document.length() > 0)
    }
    @Test fun promotionCommitsFormalMetadataAndRetiresTheAutoMarker() = scenario {
        snapshot(target(), automatic = true)
        val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.PROMOTE, snapshotId = id))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertTrue(File(target(), SNAPSHOT_MANIFEST).isFile)
        assertFalse(File(target(), AUTO_SNAPSHOT_MANIFEST).exists()); assertEquals(id, SnapshotRepository.readManifest(app, id)!!.snapshotId)
    }
    @Test fun incompleteAutoSnapshotCannotBePromoted() = scenario {
        snapshot(target(), automatic = true); File(target(), "images/p0.jpg").delete()
        val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.PROMOTE, snapshotId = id))
        assertEquals(State.FAILED, end(ticket).state); assertFalse(File(target(), SNAPSHOT_MANIFEST).exists())
        assertTrue(File(target(), AUTO_SNAPSHOT_MANIFEST).isFile)
    }
    @Test fun deleteIsDurableAndKeepsItsPreviousExistenceResultAcrossAcknowledgementRecovery() = scenario {
        snapshot(target()); val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.DELETE, snapshotId = id))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertTrue(domain(ticket).existed); assertFalse(target().exists())
        queue.shutdown(); runtime.store.writableDatabase.execSQL("UPDATE plans SET state='QUEUED',attempt=NULL WHERE id=?", arrayOf(ticket.id))
        createQueue(); queue.ready.await(); val restored = requireNotNull(queue.ticket(ticket.id))
        assertEquals(State.SUCCEEDED, end(restored).state); assertTrue(domain(restored).existed)
    }
    @Test fun aLegacyBackupIsRestoredWhenTheOriginalDirectoryIsMissing() = scenario {
        val old = snapshot(File(SnapshotRepository.root(app), ".backup_${id}_123"), "previous")
        val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.RECOVER, snapshotId = id, fileName = old.name))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertFalse(old.exists())
        assertEquals("previous", File(target(), "images/p0.jpg").readText())
    }
    @Test fun anExistingValidSnapshotDoesNotMakeItsOlderBackupDisposable() = scenario {
        snapshot(target(), "current"); val old = snapshot(File(SnapshotRepository.root(app), ".backup_${id}_123"), "previous")
        val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.RECOVER, snapshotId = id, fileName = old.name))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals("current", File(target(), "images/p0.jpg").readText())
        assertEquals("previous", File(old, "images/p0.jpg").readText())
    }
    @Test fun unrecognizedLegacyBackupFailsVisiblyAndIsNeverDeleted() = scenario {
        val old = File(SnapshotRepository.root(app), ".backup_${id}_123").apply { mkdirs() }
        File(old, "unknown").writeText("keep")
        val ticket = operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.RECOVER, snapshotId = id, fileName = old.name))
        assertEquals(State.FAILED, end(ticket).state); assertEquals("keep", File(old, "unknown").readText()); assertFalse(target().exists())
    }
    private suspend fun interruptedImport(paused: Boolean): String {
        queue.ready.await(); queue.shutdown()
        val planId = "plan:" + UUID.randomUUID()
        val request = SnapshotOperationRequest(operation = QueuedSnapshotOperations.IMPORT, snapshotId = null,
            uri = Uri.fromFile(File(scratch, "missing.shaftsnap")).toString())
        queue.store.admit(planId, "interrupted", "snapshot-operation", gson.toJson(request))
        val manifest = SnapshotManifest(snapshotId = id, illustId = 123, title = "new", coverPath = "images/p0.jpg")
        val state = SnapshotOperationState(manifestJson = gson.toJson(manifest), prepared = true,
            files = mapOf("images/p0.jpg" to SnapshotResource("import", 3, "not-old")))
        val checkpoint = ArtifactCheckpoint(domain = gson.toJson(state))
        runtime.store.writableDatabase.execSQL("UPDATE plans SET checkpoint=?,state=? WHERE id=?",
            arrayOf(gson.toJson(checkpoint), if (paused) "PAUSED" else "QUEUED", planId))
        val token = java.security.MessageDigest.getInstance("SHA-256").digest(planId.toByteArray()).joinToString("") { "%02x".format(it) }
        snapshot(File(SnapshotRepository.root(app), ".queue-import-$token.old"), "previous")
        createQueue(); queue.ready.await()
        return planId
    }
    @Test fun missingImportInputStillRestoresTheOldDirectoryBeforeFailing() = scenario {
        val plan = interruptedImport(false)
        assertEquals(State.FAILED, end(requireNotNull(queue.ticket(plan))).state)
        assertEquals("previous", File(target(), "images/p0.jpg").readText())
    }
    @Test fun cancellingARestoredImportRollsBackItsOnlyOldBackup() = scenario {
        val plan = interruptedImport(true); val ticket = requireNotNull(queue.ticket(plan))
        assertTrue(ticket.cancel()); ticket.status.first { it.state == State.CANCELLED }
        assertEquals("previous", File(target(), "images/p0.jpg").readText())
    }
}
