package ceui.pixiv.ui.novel.reader.export

import android.app.Application
import android.net.Uri
import ceui.loxia.Novel
import ceui.pixiv.api.model.WebNovel
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.copy.*
import ceui.pixiv.download.queue.*
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
class NovelQueuedExportTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "novel-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: ContentPlanQueue
    private lateinit var output: File
    private val gson = Gson()
    private val recorded = mutableListOf<Long>()
    @Before fun setup() {
        runtime = ContentQueueRuntime(app, database); queue = ContentPlanQueue(runtime, 1)
        output = File(app.cacheDir, "novel-${UUID.randomUUID()}")
        val provider = object : CopyOutput {
            override fun open(destination: CopyDestination) = StorageBackend.WriteHandle(Uri.fromFile(output), output.outputStream(), {}, {})
            override fun resume(uri: String, destination: CopyDestination) = open(destination)
            override fun matches(uri: String, sha256: String, bytes: Long) = output.isFile && output.length() == bytes &&
                runBlocking { ArtifactPublisher.digest(output) == sha256 }
            override fun finishRecovered(uri: String, destination: CopyDestination) {}
            override fun available(intent: CopyIntent) = output.isFile
        }
        QueuedNovelExports(app, queue, ContentPhaseRunner(runtime) { 1 }, provider) { novel, _ -> recorded += novel.id }
    }
    @After fun teardown() {
        runBlocking { queue.shutdown() }; runtime.close(); app.deleteDatabase(database); output.delete()
        File(app.filesDir, "content_novel_inputs").deleteRecursively()
    }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(12_000, block) }
    private fun request(format: ExportFormat, legacy: Boolean = false, merged: ExportMerged? = null): NovelExportRequest =
        NovelExportRequest(format = format.name, novelJson = if (merged == null) "{\"id\":123,\"title\":\"Book\"}" else null,
            novelId = if (merged == null) 123 else 0, webJson = if (merged == null) "{\"id\":\"123\",\"title\":\"Book\",\"text\":\"body\"}" else null,
            tokens = if (merged == null) listOf(ExportToken("chapter", 0, 1, "第一章"), ExportToken("paragraph", 2, 5, "保存正文")) else null,
            merged = merged, destination = CopyDestination("cache", segments = listOf("original", "book.${format.extension}"), mime = format.mimeType, policy = "Rename"),
            header = "Frozen header", legacyTxt = legacy)
    private suspend fun export(spec: NovelExportRequest): ContentPlanTicket {
        val ticket = queue.submit("novel-test", "novel-export") { gson.toJson(spec) }
        val result = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
        assertEquals(result.error, State.SUCCEEDED, result.state)
        return ticket
    }
    @Test fun txtUsesFrozenHeaderAndExistingHistoryIdentity() = scenario {
        val ticket = export(request(ExportFormat.Txt))
        assertTrue(output.readText().startsWith("Frozen header")); assertTrue(output.readText().contains("保存正文"))
        assertEquals(listOf(123L), recorded)
        val spec = gson.fromJson(queue.store.get(ticket.id).request, NovelExportRequest::class.java)
        assertEquals(listOf("original", "book.txt"), spec.destination.segments)
    }
    @Test fun markdownKeepsLinksAndChapterStructureThroughThePersistentParent() = scenario {
        export(request(ExportFormat.Markdown)); val text = output.readText()
        assertTrue(text.contains("## 第一章")); assertTrue(text.contains("保存正文")); assertTrue(text.contains("show.php?id=123"))
    }
    @Test fun epubKeepsItsRequiredStoredFirstMimetypeAndContentEntries() = scenario {
        export(request(ExportFormat.Epub))
        ZipFile(output).use { zip ->
            assertEquals("mimetype", zip.entries().nextElement().name)
            assertEquals(java.util.zip.ZipEntry.STORED, zip.getEntry("mimetype").method)
            assertEquals("application/epub+zip", zip.getInputStream(zip.getEntry("mimetype")).bufferedReader().readText())
            assertTrue(zip.getInputStream(zip.getEntry("OEBPS/content.xhtml")).bufferedReader().readText().contains("保存正文"))
        }
    }
    @Test fun legacyTxtRetainsMarkersAndWritesTheSameNovelHistoryRecord() = scenario {
        export(request(ExportFormat.Txt, legacy = true))
        val text = output.readText(); assertTrue(text.contains("<===== Shaft Novel Start =====>"))
        assertTrue(text.contains("Frozen header")); assertTrue(text.contains("body")); assertTrue(text.contains("<===== Shaft Novel End =====>"))
        assertEquals(listOf(123L), recorded)
    }
    @Test fun mergedTxtRetainsSeriesChapterOrderAndSubtitleWithoutInventingASingleNovelRecord() = scenario {
        val merged = ExportMerged("Collection", "Author", null, null, "novel_series_123", listOf(
            ExportChapter("Long full title", "one", null, "第1章 Short", "Long full title"),
            ExportChapter("Second", "two", null, "第2章 Second", null)))
        export(request(ExportFormat.Txt, merged = merged)); val text = output.readText()
        assertTrue(text.indexOf("one") < text.indexOf("two")); assertTrue(text.contains("Long full title")); assertTrue(recorded.isEmpty())
    }
}
