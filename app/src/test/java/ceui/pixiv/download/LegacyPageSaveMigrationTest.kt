package ceui.pixiv.download

import android.app.Application
import androidx.room.Room
import ceui.lisa.activities.Shaft
import ceui.lisa.core.DownloadItem
import ceui.lisa.database.AppDatabase
import ceui.lisa.database.DownloadEntity
import ceui.lisa.database.DownloadingEntity
import ceui.lisa.utils.Settings
import com.blankj.utilcode.util.Utils
import com.google.gson.Gson
import java.io.File
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class LegacyPageSaveMigrationTest {
    private val context get() = RuntimeEnvironment.getApplication()
    private val gson = Gson()
    private lateinit var database: AppDatabase
    private lateinit var journal: ImagePageSaveJournal
    private val savedAt = 1_700_000_000_000L

    @Before fun setUp() {
        Utils.init(context)
        Shaft.sGson = gson
        Shaft.sSettings = Settings().apply { downloadLimitType = 2 }
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", context)
        database = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", database)
        journal = ImagePageSaveJournal(File(context.noBackupFilesDir, "image-page-saves.properties"))
        journal.write(emptyList())
    }

    @After fun tearDown() {
        journal.write(emptyList())
        AppDatabase.destroyInstance()
        database.close()
    }

    private fun entry(page: Int, error: String? = null) = ImagePageSaveJournal.Entry(
        """{"id":12345,"type":"illust","page_count":20,"title":"Test",
            "image_urls":{"large":"https://example.com/12345_p0.jpg"}}""".trimIndent(),
        page, "https://example.com/12345_p$page.png", savedAt, error,
    )

    private fun rows(): List<DownloadItem> = database.downloadDao()
        .getDownloadingBatch(0, Long.MAX_VALUE, 100).use { cursor ->
            buildList { while (cursor.moveToNext()) add(gson.fromJson(cursor.getString(1), DownloadItem::class.java)) }
        }

    @Test fun `pending and failed saves become only their selected pages in the durable queue`() {
        journal.write(listOf(entry(3), entry(9, "timeout")))
        assertEquals(2, LegacyPageSaveMigration.migrate(context, database, gson))
        assertEquals(listOf(3, 9), rows().map { it.index })
        assertEquals(listOf("https://example.com/12345_p3.png", "https://example.com/12345_p9.png"), rows().map { it.url })
        assertTrue(journal.read().isEmpty())
        // Imports unfinished intentions, never invents completed-page records.
        assertTrue(database.downloadDao().getAll(100, 0).isEmpty())
    }

    @Test fun `crash between database commit and journal acknowledgement cannot duplicate tasks`() {
        val original = listOf(entry(3), entry(9))
        journal.write(original)
        LegacyPageSaveMigration.migrate(context, database, gson)
        val uuids = rows().map { it.uuid }
        journal.write(original) // Simulate a retained old snapshot after the database commit.
        assertEquals(0, LegacyPageSaveMigration.migrate(context, database, gson))
        assertEquals(uuids, rows().map { it.uuid })
        assertTrue(journal.read().isEmpty())
    }

    @Test fun `existing queue item keeps its identity`() {
        journal.write(listOf(entry(3)))
        LegacyPageSaveMigration.migrate(context, database, gson)
        val task = rows().single().apply { uuid = "existing-live-task" }
        database.downloadDao().insertDownloading(DownloadingEntity().apply {
            fileName = task.name
            uuid = task.uuid
            taskGson = gson.toJson(task)
        })
        journal.write(listOf(entry(3)))
        assertEquals(0, LegacyPageSaveMigration.migrate(context, database, gson))
        assertEquals("existing-live-task", rows().single().uuid)
    }

    @Test fun `database failure rolls back every insert and retains all old intentions`() {
        journal.write(listOf(entry(3), entry(9)))
        database.openHelper.writableDatabase.execSQL("""CREATE TRIGGER fail_import BEFORE INSERT
            ON illust_downloading_table WHEN NEW.taskGson LIKE '%"index":9%'
            BEGIN SELECT RAISE(ABORT, 'test storage failure'); END""")
        try {
            LegacyPageSaveMigration.migrate(context, database, gson)
            fail("Expected failed transaction")
        } catch (_: android.database.SQLException) { }
        assertTrue(rows().isEmpty())
        assertEquals(2, journal.read().size)
        database.openHelper.writableDatabase.execSQL("DROP TRIGGER fail_import")
        assertEquals(2, LegacyPageSaveMigration.migrate(context, database, gson))
    }

    @Test fun `invalid artwork page leaves journal unchanged`() {
        journal.write(listOf(entry(3), entry(20)))
        try {
            LegacyPageSaveMigration.migrate(context, database, gson)
            fail("Expected invalid page")
        } catch (_: IllegalArgumentException) { }
        assertTrue(rows().isEmpty())
        assertEquals(listOf(3, 20), journal.read().map { it.page })
    }

    @Test fun `already written page is acknowledged only if the recorded file is still usable`() {
        val file = File(context.filesDir, "migration-completed.png").apply { writeText("image") }
        database.downloadDao().insertDownload(DownloadEntity().apply {
            fileName = "completed.png"
            illustGson = entry(3).illustJson
            filePath = file.toURI().toString()
            downloadTime = savedAt + 1
            page = 3
        })
        journal.write(listOf(entry(3)))
        assertEquals(0, LegacyPageSaveMigration.migrate(context, database, gson))
        assertTrue(rows().isEmpty())
        file.delete()
        journal.write(listOf(entry(3)))
        assertEquals(1, LegacyPageSaveMigration.migrate(context, database, gson))
        assertEquals(3, rows().single().index)
    }
}
