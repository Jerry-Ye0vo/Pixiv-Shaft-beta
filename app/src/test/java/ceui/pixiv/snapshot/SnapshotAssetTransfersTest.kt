package ceui.pixiv.snapshot

import android.app.Application
import ceui.pixiv.download.queue.ContentPhaseRunner
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.imageloader.OriginalFileLease
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
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
class SnapshotAssetTransfersTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "snapshot-assets-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var transfers: SnapshotAssetTransfers
    private lateinit var root: File
    private lateinit var original: File
    private lateinit var preview: File
    private lateinit var target: File
    private val acquired = AtomicInteger()
    private val released = AtomicInteger()
    private val urls = mutableListOf<String>()
    @Before fun setup() {
        root = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        original = File(root, "original.jpg").apply { writeText("full quality") }
        preview = File(root, "preview.jpg").apply { writeText("small preview") }
        target = File(root, "snapshot/images/p0.jpg")
        runtime = ContentQueueRuntime(app, database); runtime.initialize()
        transfers = SnapshotAssetTransfers(ContentPhaseRunner(runtime) { 1 }) { url ->
            acquired.incrementAndGet(); urls += url
            OriginalFileLease(original, Closeable { released.incrementAndGet() })
        }
    }
    @After fun teardown() { runtime.close(); app.deleteDatabase(database); root.deleteRecursively() }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }
    private fun rows() = runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
    private suspend fun eventually(block: () -> Boolean) { withTimeout(5_000) { while (!block()) delay(10) } }
    private fun occupy(): ceui.pixiv.download.queue.ContentQueueStore.Task {
        val id = runtime.store.enqueue("other-copy", "other-copy", Pool.COPY, "x")
        return requireNotNull(runtime.store.claim(id, Pool.COPY, 1))
    }
    private fun release(held: ceui.pixiv.download.queue.ContentQueueStore.Task) {
        runtime.store.finish(held.id, held.attempt, true, null); runtime.changed()
    }

    @Test fun anOriginalLeaseProtectsTheSourceThroughCopyQueueWaitingAndCommit() = scenario {
        val held = occupy()
        val job = launch { transfers.copyUrl("original-url", target, original = true) }
        eventually { rows().singleOrNull()?.state == State.QUEUED }
        assertEquals(1, acquired.get()); assertEquals(0, released.get()); assertFalse(target.exists())
        release(held); job.join()
        assertEquals("full quality", target.readText()); assertEquals(1, released.get())
        assertEquals(State.SUCCEEDED, rows().single().state)
    }

    @Test fun aReadyPreviewCopiesTheRequestedQualityWithoutPromotingItToAnOriginal() = scenario {
        transfers.copyUrl("preview-url", target, cached = preview)
        assertEquals("small preview", target.readText()); assertEquals(0, acquired.get())
        assertTrue(rows().all { it.pool == Pool.COPY && it.state == State.SUCCEEDED })
    }

    @Test fun anOriginalRequestNeverUsesAnUnrelatedLowerQualityReadyFile() = scenario {
        transfers.copyUrl("exact-original-url?variant=1", target, original = true, cached = preview)
        assertEquals(listOf("exact-original-url?variant=1"), urls)
        assertEquals("full quality", target.readText()); assertEquals(1, released.get())
    }

    @Test fun anEvictedPreviewFallsBackThroughTheQueuedSourceAndLeavesNoTemporaryOutput() = scenario {
        preview.delete()
        transfers.copyUrl("exact-preview-url", target, cached = preview)
        assertEquals(listOf("exact-preview-url"), urls)
        assertEquals("full quality", target.readText())
        assertEquals(1, released.get())
        assertTrue(target.parentFile!!.listFiles()!!.none { it.name.contains(".queue-") })
        assertEquals(listOf(State.FAILED, State.SUCCEEDED), rows().map { it.state })
    }

    @Test fun cancellingAWaitingCopyReleasesItsSourceReferenceAndWritesNothing() = scenario {
        val held = occupy()
        val job = launch { transfers.copyUrl("original-url", target, original = true) }
        eventually { rows().singleOrNull()?.state == State.QUEUED }
        job.cancelAndJoin()
        assertEquals(1, released.get()); assertFalse(target.exists())
        assertEquals(State.CANCELLED, rows().single().state)
        release(held)
    }

    @Test fun failedAtomicCommitKeepsTheExistingTargetAndReportsFailureAfterClosingTheSource() = scenario {
        target.mkdirs(); File(target, "keep").writeText("old snapshot")
        try { transfers.copyUrl("original-url", target); fail("commit must fail") }
        catch (_: IOException) { }
        assertEquals("old snapshot", File(target, "keep").readText())
        assertEquals(1, released.get())
        assertEquals(State.FAILED, rows().single().state)
        assertTrue(target.parentFile!!.listFiles()!!.none { it.name.contains(".queue-") })
    }

    @Test fun manifestsAlsoWaitForSharedCopyAuthorizationAndCancelledWritesStayInvisible() = scenario {
        val held = occupy()
        val manifest = File(root, "snapshot/manifest.json")
        val job = launch { transfers.writeText(manifest, "{\"version\":1}") }
        eventually { rows().singleOrNull()?.state == State.QUEUED }
        assertFalse(manifest.exists()); job.cancelAndJoin(); assertFalse(manifest.exists())
        release(held)
        transfers.writeText(manifest, "{\"version\":1}")
        assertEquals("{\"version\":1}", manifest.readText())
        assertEquals(State.SUCCEEDED, rows().last().state)
    }
}
