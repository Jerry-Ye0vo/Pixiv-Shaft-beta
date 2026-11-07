package ceui.pixiv.ui.profile

import android.app.Application
import android.net.Uri
import androidx.room.Room
import ceui.lisa.database.AppDatabase
import ceui.lisa.database.DownloadEntity
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.State
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import java.io.File
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ProfileTaggingQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val databaseName = "profile-queue-${UUID.randomUUID()}.db"
    private lateinit var db: AppDatabase
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var worker: QueuedProfileTagging
    private val inputs = mutableListOf<File>()
    private var prediction: suspend (Uri) -> List<PredictedImageTag> = { listOf(PredictedImageTag("dress",.9f,0)) }

    @Before fun setup() {
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        runtime = ContentQueueRuntime(app,databaseName); createQueue()
    }
    private fun createQueue() {
        queue = ContentPlanQueue(runtime,1)
        worker = QueuedProfileTagging(app,queue,ContentPhaseRunner(runtime) { 1 },{ db },{ true }) {
            object : ProfileTagger {
                override suspend fun predict(uri: Uri) = prediction(uri)
                override fun close() {}
            }
        }
    }
    @After fun cleanup() {
        runBlocking { queue.shutdown() }; runtime.close(); db.close(); app.deleteDatabase(databaseName)
        inputs.forEach { it.delete() }
    }
    private fun add(name: String, bytes: String) {
        val file = File(app.filesDir,"${UUID.randomUUID()}-$name").apply { writeText(bytes) }; inputs += file
        db.downloadDao().insert(DownloadEntity().apply {
            fileName = file.name; filePath = Uri.fromFile(file).toString(); illustId = inputs.size.toLong()
        })
    }
    private suspend fun eventually(predicate: () -> Boolean) = withTimeout(8000) { while (!predicate()) delay(10) }

    @Test fun identicalBytesReuseInferenceAndNonImagesNeverEnterTheEncodeStage() = runBlocking {
        add("one.jpg","same image bytes"); add("renamed.png","same image bytes"); add("novel.epub","novel")
        var calls = 0; prediction = { calls++; listOf(PredictedImageTag("dress",.9f,0)) }
        val ticket = worker.start()
        assertEquals(State.SUCCEEDED,withTimeout(8000) { ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END } }.state)
        val result = worker.progress(ticket)!!
        assertEquals(1,calls); assertEquals(1,result.inferred); assertEquals(1,result.reused)
        assertEquals(1,result.skipped); assertEquals(3,result.scanned)
        assertEquals(2,db.profileAnalyticsDao().tagBatch(LocalTaggerModel.VERSION,0,100).size)
    }

    @Test fun pauseAndColdQueueResumeKeepCompletedResultsAndTheLibraryBoundary() = runBlocking {
        add("one.jpg","first"); add("two.jpg","second")
        val enteredSecond = CompletableDeferred<Unit>()
        prediction = { uri ->
            if (uri.toString().contains("two.jpg")) { enteredSecond.complete(Unit); awaitCancellation() }
            listOf(PredictedImageTag("dress",.9f,0))
        }
        val ticket = worker.start()
        withTimeout(8000) { enteredSecond.await() }
        assertTrue(ticket.pause())
        withTimeout(8000) { ticket.status.first { it.state == State.PAUSED } }
        val before = worker.progress(ticket)!!
        assertEquals(1,before.scanned)
        queue.shutdown()
        prediction = { listOf(PredictedImageTag("coat",.9f,0)) }
        createQueue(); queue.ready.await()
        eventually { worker.current() != null }
        val restored = worker.current()!!
        add("new.jpg","added after the original intention")
        worker.resume(restored.id)
        assertEquals(State.SUCCEEDED,withTimeout(8000) { restored.status.first { it.state in setOf(State.SUCCEEDED,State.FAILED) } }.state)
        val after = worker.progress(restored)!!
        assertEquals(2,after.scanned); assertEquals(2,after.inferred)
        assertEquals(2,db.profileAnalyticsDao().tagBatch(LocalTaggerModel.VERSION,0,100).size)
    }
}
