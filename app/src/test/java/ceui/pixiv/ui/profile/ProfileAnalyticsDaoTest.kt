package ceui.pixiv.ui.profile

import android.app.Application
import androidx.room.Room
import ceui.lisa.database.AppDatabase
import ceui.lisa.database.DownloadEntity
import ceui.pixiv.api.model.Illust
import ceui.pixiv.db.mirror.BookmarkMirrorMapper
import ceui.pixiv.db.mirror.BookmarkShelf
import ceui.pixiv.db.mirror.MirrorContentType
import ceui.pixiv.db.mirror.MirrorRestrict
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
class ProfileAnalyticsDaoTest {
    private lateinit var db: AppDatabase
    private var previousGson: com.google.gson.Gson? = null
    @Before fun setup() {
        previousGson = ceui.lisa.activities.Shaft.sGson
        ceui.lisa.activities.Shaft.sGson = com.google.gson.Gson()
        db = Room.inMemoryDatabaseBuilder(RuntimeEnvironment.getApplication(), AppDatabase::class.java).allowMainThreadQueries().build()
    }
    @After fun close() { db.close(); ceui.lisa.activities.Shaft.sGson = previousGson }

    @Test fun scanUsesStableRowIdsAndExcludesNewRowsBeyondTheStartingCeiling() {
        for (id in 1L..3L) db.downloadDao().insert(DownloadEntity().apply {
            fileName = "$id.jpg"; illustId = id; filePath = "content://old/$id"; illustGson = """{"id":$id}"""
        })
        val dao = db.profileAnalyticsDao(); val ceiling = dao.downloadCeiling()
        val first = dao.downloadBatch(0, ceiling, 2)
        db.downloadDao().insert(DownloadEntity().apply { fileName = "new.jpg"; illustId = 9 })
        assertEquals(listOf(1L,2L,3L), (first + dao.downloadBatch(first.last().sequence, ceiling, 2)).map { it.illustId })
        assertEquals(3, dao.downloadCount(ceiling))
        assertEquals("content://old/1", first.first().filePath)
    }

    @Test fun bookmarkScanIsPartitionedByOwnerTypeAndVisibility() {
        val shelves = listOf(BookmarkShelf(1, MirrorContentType.ILLUST, MirrorRestrict.PUBLIC),
            BookmarkShelf(2, MirrorContentType.ILLUST, MirrorRestrict.PUBLIC),
            BookmarkShelf(1, MirrorContentType.NOVEL, MirrorRestrict.PRIVATE))
        for ((n, shelf) in shelves.withIndex()) {
            val mapped = if (shelf.contentType == MirrorContentType.NOVEL)
                BookmarkMirrorMapper.fromNovel(shelf, ceui.loxia.Novel(id = (n + 10).toLong()), 0, 1, 100)
                else BookmarkMirrorMapper.fromIllust(shelf, Illust(id = (n + 10).toLong()), 0, 1, 100)
            db.bookmarkMirrorDao().insertRows(listOf(mapped.row))
        }
        assertEquals(listOf(10L), db.profileAnalyticsDao().bookmarkBatch(shelves[0].key, 0, 100).map { it.targetId })
    }

    @Test fun cachedPredictionsKeepModelVersionsSeparateAndCanReuseIdenticalContent() {
        val dao = db.profileAnalyticsDao()
        dao.rememberTags(LocalImageTags("content://old/1", "v1", "stamp", "hash", "[]", 30, 1))
        assertNotNull(dao.findHash("hash", "v1"))
        assertNull(dao.findHash("hash", "v2"))
        assertNull(dao.findTags("content://old/1", "v2"))
        dao.rememberTags(LocalImageTags("content://old/1", "v1", "new", "new-hash", "[]", 40, 2))
        assertNull(dao.findHash("hash", "v1"))
        assertEquals("new", dao.findTags("content://old/1", "v1")!!.sourceStamp)
    }
}
