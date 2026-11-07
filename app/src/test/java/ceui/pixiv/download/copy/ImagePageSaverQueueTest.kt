package ceui.pixiv.download.copy

import android.app.Application
import android.net.Uri
import androidx.room.Room
import ceui.lisa.activities.Shaft
import ceui.lisa.database.AppDatabase
import ceui.lisa.utils.Settings
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.ImagePageSaver
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.queue.ContentQueueRuntime
import com.blankj.utilcode.util.Utils
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ImagePageSaverQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val main = StandardTestDispatcher()
    private val name = "saver-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var queue: DurableCopyQueue
    private lateinit var db: AppDatabase
    private lateinit var root: File
    @Before fun setup() {
        Dispatchers.setMain(main)
        Utils.init(app)
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        Shaft.sGson = Gson(); Shaft.sSettings = Settings()
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).allowMainThreadQueries().build()
        ReflectionHelpers.setStaticField(AppDatabase::class.java, "INSTANCE", db)
        root = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        runtime = ContentQueueRuntime(app, name)
        queue = DurableCopyQueue(app, runtime, object : CopyOutput {
            override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
                assertTrue(destination.segments.last().endsWith("_translated.png"))
                val file = File(root, destination.segments.last())
                return StorageBackend.WriteHandle(Uri.fromFile(file), FileOutputStream(file))
            }
            override fun resume(uri: String, destination: CopyDestination): StorageBackend.WriteHandle? = null
            override fun matches(uri: String, sha256: String, bytes: Long) = false
            override fun finishRecovered(uri: String, destination: CopyDestination) = Unit
            override fun available(intent: CopyIntent) = true
        }, { 1 })
    }
    @After fun cleanup() {
        runBlocking { queue.shutdown() }
        runtime.close(); app.deleteDatabase(name)
        AppDatabase.destroyInstance(); db.close()
        root.deleteRecursively(); File(app.filesDir, "content_queue_sources").deleteRecursively()
        Dispatchers.resetMain()
    }
    @Test fun translatedSaveUsesDurableCopyAndGreenCheckWithoutCreatingAnOriginalDownloadRecord(): Unit = runBlocking {
        queue.ready.await()
        val file = File(root, "rendered.png").apply { writeText("translated pixels") }
        val artwork = Illust(id = 91, title = "example", page_count = 1)
        val saver = ImagePageSaver(app, queue) { illust, page, source ->
            CopyRequest("translation", illust.id, page, source.absolutePath, source.length(), source.lastModified(), "{}",
                CopyDestination("cache", segments = listOf("91_translated.png"), mime = "image/png", policy = "Rename"))
        }
        try {
            main.scheduler.runCurrent()
            assertTrue(saver.save(artwork, 0, ImagePageSaver.Target.Translated(file)))
            assertFalse(saver.save(artwork, 0, ImagePageSaver.Target.Translated(file)))
            withTimeout(5_000) {
                while (!saver.isTranslationSaved(file) || saver.states.value.containsKey(ImagePageSaver.PageKey(91, 0))) {
                    main.scheduler.runCurrent(); delay(10)
                }
            }
            assertEquals("translated pixels", File(root, "91_translated.png").readText())
            db.openHelper.readableDatabase.query("SELECT COUNT(*) FROM illust_download_table").use {
                assertTrue(it.moveToFirst()); assertEquals(0, it.getInt(0))
            }
        } finally {
            ReflectionHelpers.getField<CoroutineScope>(saver, "scope").cancel()
            main.scheduler.runCurrent()
        }
    }
}
