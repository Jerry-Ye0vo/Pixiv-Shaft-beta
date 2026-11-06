package ceui.lisa.core

import android.app.Application
import android.net.Uri
import ceui.lisa.download.DownloadFileFactory
import ceui.pixiv.download.StageStore
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import java.io.File
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ManagerOutputCommitTest {
    private fun verifyCommit(fails: Boolean) {
        val dir = File(RuntimeEnvironment.getApplication().cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        val key = StageStore.keyForUrl("https://example.invalid/image.jpg")
        val stage = StageStore.partFile(dir, key).apply { writeText("complete image") }
        val expected = IllegalStateException("provider failed to publish")
        val factory = object : DownloadFileFactory {
            override fun query(): Uri? = null
            override fun insert(): Uri? = null
            override fun getFileUri(): Uri = Uri.parse("content://test/image")
            override fun finishWrite() { if (fails) throw expected }
        }
        var published = false
        var failure: Throwable? = null
        var finallyCount = 0
        val io = Executors.newSingleThreadExecutor()
        try {
            val task = DownloadTask.create({ it.onNext("done") }, {
                Manager.finishPageOutput(factory, dir, key)
                published = true // stands for done-table insertion and active-row removal
            }, { failure = it }, { finallyCount++ })
            task.start(io)
            io.submit {}.get(5, TimeUnit.SECONDS)
            assertEquals(!fails, published)
            assertEquals(fails, stage.exists())
            if (fails) { assertSame(expected, failure); assertEquals("complete image", stage.readText()) }
            else assertNull(failure)
            assertEquals(1, finallyCount)
        } finally { io.shutdownNow(); dir.deleteRecursively() }
    }
    @Test fun failedVisibilityCommitPreservesStageAndDeliversFailureBeforePublication() = verifyCommit(true)
    @Test fun successfulVisibilityCommitClearsStageBeforePublication() = verifyCommit(false)
}
