package ceui.lisa.update

import android.app.Application
import ceui.pixiv.download.queue.ContentPhaseRunner
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.io.File
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.SocketPolicy
import okio.Buffer
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
class ApkSharedQueueTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val database = "apk-shared-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var server: MockWebServer
    private lateinit var root: File
    private lateinit var manager: ApkDownloadManager
    private val managers = mutableListOf<ApkDownloadManager>()
    private val records = MemoryRecords()
    private val client = OkHttpClient.Builder().readTimeout(30, TimeUnit.SECONDS).build()
    private val requests = CopyOnWriteArrayList<RecordedRequest>()
    private val bytes = ByteArray(3 * 1024 * 1024) { (it % 251).toByte() }
    @Volatile private var bodyPrefixOnly = false
    @Volatile private var invalidRange = false
    @Volatile private var changedValidator = false
    @Volatile private var stalledFirstProbe = false

    private class MemoryRecords : ApkRecordStorage {
        @Volatile var raw: String? = null
        @Volatile var reject = false
        @Volatile var rejectCompleted = false
        override fun read() = raw
        override fun write(value: String): Boolean {
            if (reject || (rejectCompleted && value.contains("\"status\":\"COMPLETED\""))) return false
            raw = value; return true
        }
        override fun clear() { raw = null }
    }
    @Before fun setup() {
        root = File(app.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        runtime = ContentQueueRuntime(app, database); runtime.initialize()
        server = MockWebServer().apply {
            dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    requests += request
                    if (stalledFirstProbe && request.path!!.contains("first")) return MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)
                    val range = Regex("bytes=(\\d+)-(\\d+)").matchEntire(request.getHeader("Range") ?: "")
                        ?: return MockResponse().setResponseCode(400)
                    val start = range.groupValues[1].toInt()
                    val end = range.groupValues[2].toInt().coerceAtMost(bytes.lastIndex)
                    val size = end - start + 1
                    val count = if (bodyPrefixOnly && size > 1) minOf(size, 1024) else size
                    return MockResponse().setResponseCode(206)
                        .setBody(Buffer().write(bytes, start, count))
                        .setHeader("Content-Length", size)
                        .setHeader("Content-Range", "bytes ${if (invalidRange && size > 1) start + 1 else start}-$end/${bytes.size}")
                        .setHeader("ETag", if (changedValidator) "\"new-apk\"" else "\"apk-v1\"")
                }
            }
            start()
        }
        manager = newManager()
    }
    private fun newManager(): ApkDownloadManager = ApkDownloadManager(app).also {
        it.environment = ApkDownloadEnvironment(ContentPhaseRunner(runtime) { 1 }, records, root,
            { timeout -> check(timeout > 0) { "A restored APK chunk cannot use an infinite timeout" }; client })
        managers += it
    }
    @After fun teardown() {
        runBlocking { managers.forEach { it.shutdown() } }
        client.dispatcher.cancelAll(); server.shutdown(); client.connectionPool.evictAll(); client.dispatcher.executorService.shutdown()
        runtime.close(); app.deleteDatabase(database); root.deleteRecursively()
    }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(20_000, block) }
    private suspend fun eventually(check: () -> Boolean) { withTimeout(8_000) { while (!check()) delay(10) } }
    private fun release(name: String = "test") = GitHubRelease("v99.0.0-$name", "v99.0.0-$name", "", null, null,
        listOf(GitHubAsset("test-github.apk", bytes.size.toLong(), server.url("/$name.apk").toString(), "application/vnd.android.package-archive")))
    private fun phases() = runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)
    private suspend fun end(instance: ApkDownloadManager = manager) = withTimeout(15_000) {
        instance.state.first { it.phase in setOf(ApkDownloadManager.Phase.COMPLETED, ApkDownloadManager.Phase.FAILED) }
    }
    private fun occupy(pool: Pool): ceui.pixiv.download.queue.ContentQueueStore.Task {
        val id = runtime.store.enqueue("occupied-$pool", "occupied-$pool", pool, "x")
        return requireNotNull(runtime.store.claim(id, pool, 1))
    }
    private fun free(task: ceui.pixiv.download.queue.ContentQueueStore.Task) {
        runtime.store.finish(task.id, task.attempt, true, null); runtime.changed()
    }

    @Test fun unifiedPauseAndResumeKeepTheAcceptedVersionAndDoNotStartBeforeCapacity() = scenario {
        val held = occupy(Pool.NETWORK)
        manager.start(release()); manager.awaitCommands()
        eventually { phases().any { it.state == State.QUEUED } }
        manager.pause(); manager.awaitCommands()
        assertEquals(ApkDownloadManager.Phase.PAUSED, manager.state.value.phase)
        assertNotNull(records.raw); assertEquals(0, requests.size)
        free(held); delay(50); assertEquals(0, requests.size)
        manager.resume(); manager.awaitCommands()
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end().phase)
        assertArrayEquals(bytes, manager.apkFile()!!.readBytes())
    }

    @Test fun unifiedCancelRetiresOnlyThisApkAndPreservesAnotherOwnersPermit() = scenario {
        val held = occupy(Pool.NETWORK)
        manager.start(release()); manager.awaitCommands()
        eventually { phases().any { it.state == State.QUEUED } }
        manager.cancel(); manager.awaitCommands()
        assertEquals(ApkDownloadManager.Phase.IDLE, manager.state.value.phase)
        assertNull(records.raw); assertEquals(0, requests.size)
        assertEquals(State.RUNNING, runtime.store.get(held.id).state)
        free(held)
    }

    @Test fun persistedParentAndSharedClaimsPrecedeEveryProbeShardAndSuccessfulMerge() = scenario {
        val held = occupy(Pool.NETWORK)
        manager.start(release()); manager.awaitCommands()
        eventually { phases().singleOrNull()?.state == State.QUEUED }
        assertNotNull(records.raw); assertEquals(0, requests.size)
        assertEquals(0, manager.state.value.activeThreads)
        free(held)
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end().phase)
        assertArrayEquals(bytes, manager.apkFile()!!.readBytes())
        assertEquals(4, requests.size) // one range probe plus three independently authorized shards
        assertEquals(5, phases().size)
        assertTrue(phases().all { it.state == State.SUCCEEDED })
        assertTrue(root.listFiles()!!.none { it.name.contains(".part") || it.name.endsWith(".merging") })
    }

    @Test fun rejectedParentPersistenceStartsNoProbeOrDirectFallback() = scenario {
        val previous = File(root, "shaft-update.apk.part0").apply { writeText("old admitted bytes") }
        records.reject = true
        manager.start(release())
        assertEquals(ApkDownloadManager.Phase.FAILED, end().phase)
        assertTrue(phases().isEmpty()); assertTrue(requests.isEmpty()); assertFalse(manager.hasApk())
        assertEquals("old admitted bytes", previous.readText())
    }

    @Test fun anAcceptedIntentWithNoBytesSurvivesColdStartupWithoutStartingNetwork() = scenario {
        val held = occupy(Pool.NETWORK)
        manager.start(release()); manager.awaitCommands()
        eventually { phases().singleOrNull()?.state == State.QUEUED }
        manager.shutdown(); free(held)
        runtime.close(); runtime = ContentQueueRuntime(app, database); runtime.initialize()
        val restored = newManager()
        restored.restoreForTesting(); restored.awaitCommands()
        assertEquals(ApkDownloadManager.Phase.PAUSED, restored.state.value.phase)
        assertFalse(restored.state.value.resumable)
        assertNotNull(records.raw); assertTrue(requests.isEmpty())
        restored.start(release())
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end(restored).phase)
    }

    @Test fun assemblyWaitsForSharedCopyAndDoesNotExposeAPartialFinalApk() = scenario {
        val held = occupy(Pool.COPY)
        manager.start(release())
        eventually { manager.state.value.phase == ApkDownloadManager.Phase.MERGING }
        eventually { phases().any { it.pool == Pool.COPY && it.state == State.QUEUED } }
        assertFalse(manager.apkFile()!!.exists())
        free(held)
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end().phase)
        assertArrayEquals(bytes, manager.apkFile()!!.readBytes())
    }

    @Test fun invalidRangeNeverBecomesAnInstalledCandidate() = scenario {
        invalidRange = true
        manager.start(release())
        val result = end()
        assertEquals(ApkDownloadManager.Phase.FAILED, result.phase)
        assertEquals(ApkDownloadManager.Reason.NOT_RESUMABLE, result.reason)
        assertFalse(manager.apkFile()!!.exists())
    }

    @Test fun restartClosesAStalledProbeBeforeStartingTheNextVersion() = scenario {
        stalledFirstProbe = true
        manager.start(release("first"))
        eventually { requests.any { it.path!!.contains("first") } }
        withTimeout(4_000) { manager.restart(release("second")); eventually { manager.state.value.tag == "v99.0.0-second" } }
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end().phase)
        assertArrayEquals(bytes, manager.apkFile()!!.readBytes())
        assertTrue(phases().any { it.state == State.CANCELLED })
    }

    @Test fun restartJoinsOldShardWritersBeforeReusingPartPaths() = scenario {
        bodyPrefixOnly = true
        manager.start(release("first"))
        eventually { root.listFiles()!!.any { it.name.contains(".part") && it.length() > 0 } }
        bodyPrefixOnly = false
        withTimeout(4_000) { manager.restart(release("second")); eventually { manager.state.value.tag == "v99.0.0-second" } }
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end().phase)
        assertArrayEquals(bytes, manager.apkFile()!!.readBytes())
    }

    @Test fun oldV2PartialRecordsRestorePausedAndResumeOnlyAfterUserStart() = scenario {
        bodyPrefixOnly = true
        val release = release()
        manager.start(release)
        eventually { root.listFiles()!!.any { it.name.contains(".part") && it.length() > 0 } }
        manager.shutdown()
        val before = requests.size
        val segments = com.google.gson.JsonParser.parseString(requireNotNull(records.raw))
            .asJsonObject.getAsJsonArray("segments")
        val resumableOffsets = segments.mapIndexedNotNull { index, element ->
            val chunk = element.asJsonObject
            val start = chunk.get("start").asLong
            val end = chunk.get("end").asLong
            val length = File(root, "shaft-update.apk.part$index").length()
            if (length > 0 && length < end - start + 1) start + length else null
        }.toSet()
        assertTrue("Fixture must retain at least one incomplete prefix", resumableOffsets.isNotEmpty())
        runtime.close(); runtime = ContentQueueRuntime(app, database); runtime.initialize()
        val restored = newManager()
        restored.restoreForTesting(); restored.awaitCommands()
        assertEquals(ApkDownloadManager.Phase.PAUSED, restored.state.value.phase)
        assertEquals(before, requests.size)
        bodyPrefixOnly = false
        restored.start(release)
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end(restored).phase)
        assertArrayEquals(bytes, restored.apkFile()!!.readBytes())
        assertTrue("Resume must reuse an actual retained prefix with its validator", requests.drop(before).any {
            val offset = it.getHeader("Range")?.substringAfter("bytes=")?.substringBefore('-')?.toLongOrNull()
            offset in resumableOffsets && it.getHeader("If-Range") == "\"apk-v1\""
        })
    }

    @Test fun changedValidatorRejectsOldPartialRecordsWithoutAppendingNewBytes() = scenario {
        bodyPrefixOnly = true
        val release = release()
        manager.start(release)
        eventually { root.listFiles()!!.any { it.name.contains(".part") && it.length() > 0 } }
        manager.shutdown()
        runtime.close(); runtime = ContentQueueRuntime(app, database); runtime.initialize()
        val restored = newManager()
        changedValidator = true; bodyPrefixOnly = false
        restored.start(release)
        assertEquals(ApkDownloadManager.Reason.NOT_RESUMABLE, end(restored).reason)
        assertFalse(restored.apkFile()!!.exists())
    }

    @Test fun aFailedParentCompletionReceiptRetainsPartsForOfflineRecoveryOfTheFinalMerge() = scenario {
        records.rejectCompleted = true
        manager.start(release())
        assertEquals(ApkDownloadManager.Phase.FAILED, end().phase)
        assertTrue(root.listFiles()!!.any { it.name.contains(".part") })
        val before = requests.size
        manager.shutdown(); runtime.close(); runtime = ContentQueueRuntime(app, database); runtime.initialize()
        records.rejectCompleted = false
        val restored = newManager()
        restored.restoreForTesting()
        assertEquals(ApkDownloadManager.Phase.COMPLETED, end(restored).phase)
        assertEquals(before, requests.size)
        assertArrayEquals(bytes, restored.apkFile()!!.readBytes())
    }

    @Test fun failedAtomicMergeKeepsExistingOutputAndResumableParts() = scenario {
        val held = occupy(Pool.COPY)
        manager.start(release())
        eventually { manager.state.value.phase == ApkDownloadManager.Phase.MERGING }
        manager.apkFile()!!.mkdirs(); File(manager.apkFile(), "old").writeText("keep")
        free(held)
        assertEquals(ApkDownloadManager.Phase.FAILED, end().phase)
        assertEquals("keep", File(manager.apkFile(), "old").readText())
        assertTrue(root.listFiles()!!.any { it.name.contains(".part") })
        assertFalse(File(root, "shaft-update.apk.merging").exists())
    }
}
