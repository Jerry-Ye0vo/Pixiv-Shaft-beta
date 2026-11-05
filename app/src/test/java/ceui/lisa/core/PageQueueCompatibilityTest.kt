package ceui.lisa.core

import android.app.Application
import androidx.room.Room
import ceui.lisa.activities.Shaft
import ceui.lisa.database.AppDatabase
import ceui.lisa.database.DownloadEntity
import ceui.lisa.database.DownloadingEntity
import ceui.lisa.database.IllustHistoryEntity
import ceui.pixiv.download.RecordedPageProbe
import com.blankj.utilcode.util.Utils
import com.google.gson.Gson
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers
import java.io.File

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class PageQueueCompatibilityTest {
    private lateinit var db: AppDatabase
    private val gson = Gson()
    private val app get() = RuntimeEnvironment.getApplication()
    @Before fun setup() {
        Utils.init(app)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", db)
    }
    @After fun cleanup() { AppDatabase.destroyInstance(); db.close() }
    private fun item(uuid: String = "pending", url: String = "https://example.invalid/original.jpg") =
        gson.fromJson("""{"uuid":"$uuid","name":"pending.jpg","url":"$url","index":2,
            "illust":{"id":42,"page_count":4,"title":"Artwork"}}""", DownloadItem::class.java)

    @Test fun admissionAndClearPreserveCompletedFileProbesSearchHistoryAndReactiveReads() = runBlocking {
        val dao = db.downloadDao()
        val file = File(app.filesDir, "old-original.jpg").apply { writeText("old image") }
        val done = DownloadEntity().apply {
            fileName = "old-template.jpg"; filePath = file.toURI().toString()
            illustGson = """{"id":42,"title":"Original record"}"""
            page = 2; downloadTime = 100
        }
        dao.insertDownload(done)
        dao.insert(IllustHistoryEntity().apply { illustID = 42; illustJson = done.illustGson; time = 75; type = 0 })
        // A newer stale row must not hide the older readable file of the same page.
        dao.insertDownload(DownloadEntity().apply {
            fileName = "deleted-new-template.jpg"; filePath = File(app.filesDir, "missing.jpg").toURI().toString()
            illustGson = done.illustGson; page = 2; downloadTime = 200
        })
        PageQueuePersistence.admit(db, gson, item())
        dao.deleteAllDownloading()
        assertEquals(2, dao.getAll(100, 0).size)
        assertTrue(dao.hasDownloadRecordByIllustIdIndexed(42))
        assertEquals(done.filePath, dao.getDownloadByFileName(done.fileName).filePath)
        assertEquals(setOf(done.fileName), dao.getDownloadsByFileNames(listOf(done.fileName)).map { it.fileName }.toSet())
        assertEquals(2, dao.getDownloadedPages(42).size)
        assertEquals(2, dao.getDownloadedPageCandidates(42, 2).size)
        assertEquals(2, dao.searchDownloads("Original record").size)
        assertEquals(1, dao.countDoneCardsInWindow("novel", 100))
        assertEquals(2, dao.flowAll(100).first().size)
        assertEquals(1, dao.getAllViewHistory(100, 0).size)
        assertEquals(done.filePath, RecordedPageProbe.findUsableUri(app, 42, 2).toString())
        assertEquals(setOf(2), RecordedPageProbe.usablePagesSince(app, 42, 50))
        assertTrue(RecordedPageProbe.usablePagesSince(app, 42, 150).isEmpty())
        file.delete()
        assertFalse(RecordedPageProbe.hasUsableRecord(app, 42, 2))
    }

    @Test fun legacyUnindexedRowsAndImporterIgnoreSemanticsRemainReadable() {
        val dao = db.downloadDao()
        val old = DownloadEntity().apply {
            fileName = "legacy-template-p2.jpg"; filePath = "content://legacy/document/42"
            illustGson = """{"id":42,"title":"Keep full metadata"}"""
            downloadTime = 100 // id=0 and page=-1: before the historical backfills
        }
        dao.insert(old)
        PageQueuePersistence.admit(db, gson, item())
        dao.deleteAllDownloading()
        assertFalse(dao.hasDownloadRecordByIllustIdIndexed(42))
        assertTrue(dao.hasDownloadRecordByIllustId(42))
        assertEquals(-1, dao.getDownloadByFileName(old.fileName).page)
        dao.insertIgnoreAll(listOf(DownloadEntity().apply {
            fileName = old.fileName; filePath = "content://wrong/replacement"; illustGson = "{}"
        }))
        assertEquals(old.illustGson, dao.getDownloadByFileName(old.fileName).illustGson)
        assertEquals(old.filePath, dao.getDownloadByFileName(old.fileName).filePath)
    }

    @Test fun sameFilenameDifferentResourceFailsWithoutOverwritingTheOlderIntent() {
        val first = item()
        PageQueuePersistence.admit(db, gson, first)
        try {
            PageQueuePersistence.admit(db, gson, item("different", "https://example.invalid/another.jpg"))
            fail("different resource must not overwrite pending filename")
        } catch (_: IllegalStateException) { }
        assertEquals(first.uuid, db.downloadDao().getDownloadingByFileName(first.name).uuid)
    }

    @Test fun staleDeleteAndStateUpdateCannotMutateNewerSameFilenameRecord() {
        val first = item("first")
        PageQueuePersistence.admit(db, gson, first)
        val next = item("next")
        PageQueuePersistence.admit(db, gson, next)
        val dao = db.downloadDao()
        assertEquals(0, dao.deleteDownloadingByIdentity(first.name, first.uuid))
        assertEquals(0, dao.updateDownloadingState(first.name, first.uuid, "{}"))
        assertEquals(next.uuid, dao.getDownloadingByFileName(next.name).uuid)
    }

    @Test fun missingLegacyUuidIsRepairedAndCanBeRemovedAfterRecovery() {
        val dao = db.downloadDao()
        dao.insertDownloading(DownloadingEntity().apply {
            fileName = "old-name.jpg"; uuid = ""
            taskGson = """{"name":"stale-name.jpg","url":"https://example.invalid/a.jpg","index":0,"illust":{"id":42}}"""
        })
        val first = Manager.readRestoredDownloads(dao, gson).single()
        val second = Manager.readRestoredDownloads(dao, gson).single()
        assertEquals("old-name.jpg", first.name)
        assertTrue(first.uuid.isNotEmpty())
        assertEquals(first.uuid, second.uuid)
        assertEquals(1, dao.deleteDownloadingByIdentity(first.name, first.uuid))
    }

    @Test fun identityRepairDoesNotOverwriteAnInterveningReplacement() {
        val old = item("old")
        val oldJson = gson.toJson(old)
        val replacement = item("new")
        PageQueuePersistence.admit(db, gson, replacement)
        assertEquals(0, db.downloadDao().repairDownloadingIdentity(old.name, "", oldJson, "repaired", oldJson))
        assertEquals(replacement.uuid, db.downloadDao().getDownloadingByFileName(old.name).uuid)
    }
}
