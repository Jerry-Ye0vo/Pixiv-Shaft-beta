package ceui.lisa.database

import android.app.Application
import androidx.room.Room
import ceui.lisa.utils.Params
import ceui.pixiv.db.BookmarkKnowledgeEntity
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class DownloadLookupTest {
    private lateinit var db: AppDatabase
    @Before fun setup() { db = Room.inMemoryDatabaseBuilder(RuntimeEnvironment.getApplication(), AppDatabase::class.java).allowMainThreadQueries().build() }
    @After fun close() { db.close() }
    private fun row(name: String, id: Long = 42, time: Long = 100) = DownloadEntity().apply {
        fileName = name; illustId = id; downloadTime = time; page = 2
        filePath = "content://original/$name"; illustGson = """{"id":$id,"title":"preserved"}"""
    }

    @Test fun legacyLookupParsesOnlyTheTopLevelIdAndAcceptsOldJsonWhitespace() {
        val dao = db.downloadDao()
        dao.insert(row("author-id.jpg", 0).apply { illustGson = """{"id":41,"user":{"id":42}}""" })
        assertFalse(dao.hasDownloadRecordByIllustId(42))
        dao.insert(row("old.jpg", 0).apply { illustGson = """{ "id" : 42, "user": { "id": 99 } }""" })
        assertTrue(dao.hasDownloadRecordByIllustId(42))
        assertEquals(0L, dao.getDownloadByFileName("old.jpg").illustId)
    }

    @Test fun novelIdCollisionDoesNotMarkAnIllustrationAsDownloaded() {
        val dao = db.downloadDao()
        dao.insert(row(Params.NOVEL_KEY + "42.txt"))
        assertFalse(dao.hasDownloadRecordByIllustIdIndexed(42))
        dao.insert(row("illustration.jpg"))
        assertTrue(dao.hasDownloadRecordByIllustIdIndexed(42))
    }

    @Test fun keysetPagingKeepsTimestampTiesAndIsNotShiftedByNewHeadRows() {
        val dao = db.downloadDao()
        listOf("a", "b", "c", "d", "e").forEach { dao.insert(row(it)) }
        dao.insert(row("older", time = 90))
        val first = dao.getAll(2, 0)
        assertEquals(listOf("e", "d"), first.map { it.fileName })
        dao.insert(row("new-head", time = 110))
        val second = dao.getDownloadsAfter(first.last().downloadTime, first.last().fileName, 2)
        val third = dao.getDownloadsAfter(second.last().downloadTime, second.last().fileName, 2)
        assertEquals(listOf("e", "d", "c", "b", "a", "older"), (first + second + third).map { it.fileName })
    }

    @Test fun pageCandidatesKeepOldUrisAndUseTheOrderedIndexWithoutTemporarySorting() {
        val dao = db.downloadDao()
        dao.insert(row("old", time = 10)); dao.insert(row("new", time = 20))
        assertEquals(listOf("new", "old"), dao.getDownloadedPageCandidates(42, 2).map { it.fileName })
        assertEquals("content://original/old", dao.getDownloadedPathsByFileNames(listOf("old")).single().filePath)
        db.openHelper.readableDatabase.query("EXPLAIN QUERY PLAN SELECT fileName,filePath,page FROM illust_download_table WHERE illustId=42 AND page=2 ORDER BY downloadTime DESC").use { cursor ->
            val plans = mutableListOf<String>()
            while (cursor.moveToNext()) plans += cursor.getString(3)
            assertFalse(plans.joinToString().contains("TEMP B-TREE"))
        }
    }

    @Test fun confirmedKnowledgeIsAccountScopedAndPendingWritesCannotRecreateADeletedOwnersData() {
        val dao = db.bookmarkKnowledgeDao()
        val user = UserEntity().apply { userID = 1 }
        db.downloadDao().insertUser(user)
        dao.remember(listOf(BookmarkKnowledgeEntity(1, 42, true), BookmarkKnowledgeEntity(2, 42, true)))
        assertEquals(listOf(BookmarkKnowledgeEntity(1, 42, true)), dao.knownAmong(1, listOf(42)))
        assertTrue(dao.knownAmong(2, listOf(42)).isEmpty())
        db.downloadDao().deleteUser(user); dao.deleteOwner(1)
        dao.remember(listOf(BookmarkKnowledgeEntity(1, 42, true)))
        assertTrue(dao.knownAmong(1, listOf(42)).isEmpty())
    }
}
