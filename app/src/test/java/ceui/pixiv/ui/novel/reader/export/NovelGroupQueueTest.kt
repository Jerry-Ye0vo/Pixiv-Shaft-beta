package ceui.pixiv.ui.novel.reader.export

import android.app.Application
import android.net.Uri
import ceui.loxia.Novel
import ceui.pixiv.api.model.WebNovel
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.copy.*
import ceui.pixiv.download.model.*
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.State
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
class NovelGroupQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "novel-group-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var exports: QueuedNovelExports
    private lateinit var directory: File
    private val gson = Gson()
    private val opens = AtomicInteger()
    private val provider = object : CopyOutput {
        private fun file(destination: CopyDestination) = File(directory, destination.relativePath().filename)
        override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
            opens.incrementAndGet(); val output = file(destination)
            return StorageBackend.WriteHandle(Uri.fromFile(output), output.outputStream(), {}, {})
        }
        override fun resume(uri: String, destination: CopyDestination) = open(destination)
        override fun matches(uri: String, sha256: String, bytes: Long): Boolean {
            val file = File(Uri.parse(uri).path!!)
            return file.isFile && file.length() == bytes && runBlocking { ArtifactPublisher.digest(file) == sha256 }
        }
        override fun finishRecovered(uri: String, destination: CopyDestination) {}
        override fun available(intent: CopyIntent) = file(intent.request.destination).isFile
    }
    @Before fun setup() {
        directory = File(app.cacheDir, "group-${UUID.randomUUID()}").apply { mkdirs() }
        runtime = ContentQueueRuntime(app, name); start()
    }
    private fun start() {
        queue = ContentPlanQueue(runtime, 1)
        exports = QueuedNovelExports(app, queue, ContentPhaseRunner(runtime) { 1 }, provider) { _, _ -> }
    }
    @After fun teardown() {
        runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(name); directory.deleteRecursively()
        File(app.filesDir, "content_novel_inputs").deleteRecursively()
    }
    private fun scenario(body: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(15_000, body) }
    private suspend fun end(ticket: ContentPlanTicket) = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
    private fun naming() = NovelGroupNaming("{title}_{chapters}.{ext}", true, false, 1000,
        CopyDestination("cache", segments = listOf("placeholder.txt"), mime = "text/plain", policy = "Rename"))
    private fun novel(id: Long) = "{\"id\":$id,\"title\":\"Chapter $id\"}"
    private fun spec(mode: String = "series", count: Int = 1) = NovelGroupRequest(mode = mode,
        series = (1..count).map { NovelGroupSeries(it.toLong(), "Series $it", "{\"id\":$it,\"title\":\"Series $it\"}") },
        format = "Txt", naming = naming(), authorName = "Author", authorId = 42)
    private fun web(novel: Novel): WebNovel = gson.fromJson("{\"id\":\"${novel.id}\",\"text\":\"Body ${novel.id}\"}", WebNovel::class.java)
    private fun service(page: suspend (Long, Int?) -> NovelGroupPage = { id, _ ->
        NovelGroupPage(null, listOf(novel(id * 10), novel(id * 10 + 1)), false) },
        chapter: suspend (Novel) -> WebNovel = { web(it) }, wait: suspend (Long) -> Unit = {}) =
        QueuedNovelGroups(app, queue, ContentPhaseRunner(runtime) { 1 }, exports, page, chapter, wait)

    @Test fun fullSeriesFetchesAndPublishesAtEveryCapacityOne() = scenario {
        val service = service(); val ticket = service.enqueue(spec())
        val status = end(ticket); assertEquals(status.error, State.SUCCEEDED, status.state)
        assertEquals(2, service.state(ticket.id)!!.chapters); assertEquals(1, opens.get())
        val text = directory.listFiles()!!.single().readText()
        assertTrue(text.indexOf("Body 10") < text.indexOf("Body 11"))
        assertEquals(2, runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
            .count { it.payload.contains("novel-chapter") })
    }
    @Test fun requestedFinishPublishesOnlyAlreadyFetchedChapters() = scenario {
        lateinit var ticket: ContentPlanTicket
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val service = service(chapter = { novel -> entered.complete(Unit); release.await(); web(novel) })
        ticket = service.enqueue(spec()); entered.await(); queue.finishByUser(ticket)
        withTimeout(3000) { while (!queue.store.finishRequested(ticket.id)) delay(10) }
        release.complete(Unit)
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(1, service.state(ticket.id)!!.chapters)
        assertTrue(directory.listFiles()!!.single().name.contains("1.txt"))
    }
    @Test fun crossSeriesPerOutputKeepsIndependentSeriesFiles() = scenario {
        val service = service(); val ticket = service.enqueue(spec("per-series", 2))
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(2, service.state(ticket.id)!!.success); assertEquals(2, opens.get())
        assertEquals(2, directory.listFiles()!!.size)
    }
    @Test fun allMergedKeepsSeparatorsAndRealChapterCounts() = scenario {
        val service = service(); val ticket = service.enqueue(spec("all", 2))
        assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(4, service.state(ticket.id)!!.chapters); assertEquals(1, opens.get())
        val output = directory.listFiles()!!.single()
        assertEquals("Author_全系列合集_U42.txt", output.name)
        assertTrue(output.readText().contains("<系列 1/2>")); assertTrue(output.readText().contains("<系列 2/2>"))
    }
    @Test fun failedChapterDoesNotLoseGoodChaptersOrMiscount() = scenario {
        val service = service(chapter = { if (it.id == 10L) throw java.io.IOException("network") else web(it) })
        val ticket = service.enqueue(spec()); assertEquals(State.SUCCEEDED, end(ticket).state)
        assertEquals(1, service.state(ticket.id)!!.skipped); assertEquals(1, service.state(ticket.id)!!.chapters)
    }
    @Test fun noSuccessfulChaptersNeverCreatesAnEmptyOutput() = scenario {
        val service = service(chapter = { throw java.io.IOException("offline") })
        val ticket = service.enqueue(spec()); assertEquals(State.FAILED, end(ticket).state)
        assertEquals(0, opens.get())
    }
    @Test fun coldRecoveryReusesFrozenChapterBodiesAndDoesNotFetchThemAgain() = scenario {
        queue.ready.await(); queue.shutdown()
        val store = ContentPlanStore(runtime.store)
        val request = spec()
        store.admit("restored-group", "frozen-group", "novel-series-group", gson.toJson(request))
        val lease = store.claim("restored-group", 1)
        val checkpoint = NovelGroupState(parts = mutableListOf(NovelGroupPart(detail = request.series[0].detail,
            novels = mutableListOf(novel(10), novel(11)), listed = true, next = 1,
            chapters = mutableListOf(ExportChapter("First", "Frozen body", null, "第1章 First", null)))))
        store.checkpoint(lease.id, lease.attempt, gson.toJson(checkpoint)); runtime.store.recoverAfterProcessDeath()
        start()
        val calls = mutableListOf<Long>()
        service(page = { _, _ -> error("should not list again") }, chapter = { calls += it.id; web(it) })
        queue.ready.await(); val ticket = requireNotNull(queue.ticket("restored-group"))
        assertEquals(State.SUCCEEDED, end(ticket).state); assertEquals(listOf(11L), calls)
        assertTrue(directory.listFiles()!!.single().readText().contains("Frozen body"))
    }
    @Test fun pauseAndResumeRetainCompletedChapterCheckpoint() = scenario {
        val delayEntered = CompletableDeferred<Unit>(); var first = true
        val service = service(wait = { if (first) { first = false; delayEntered.complete(Unit); awaitCancellation() } })
        val ticket = service.enqueue(spec()); delayEntered.await(); ticket.pause()
        assertEquals(State.PAUSED, end(ticket).state)
        assertEquals(1, service.state(ticket.id)!!.parts[0].next)
        queue.resume(ticket.id)
        val status = ticket.status.first { it.state == State.SUCCEEDED || it.state == State.FAILED }
        assertEquals(status.error, State.SUCCEEDED, status.state); assertEquals(1, opens.get())
    }
    @Test fun frozenNamingFixesExtensionAndChapterCountWithoutLiveSettings() {
        val destination = QueuedNovelGroups.destination(naming().copy(template = "Folder/{title}_{chapters}.txt"),
            ItemMeta(id = 1, title = "Book", author = Author(1, "A"), createdAt = java.time.Instant.EPOCH,
                seriesTotal = 7), ExportFormat.Epub)
        assertEquals("Book_7.epub", destination.relativePath().filename)
        assertEquals("application/epub+zip", destination.mime)
    }
}
