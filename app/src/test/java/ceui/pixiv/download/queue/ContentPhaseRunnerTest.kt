package ceui.pixiv.download.queue

import android.app.Application
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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
class ContentPhaseRunnerTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "phases-${UUID.randomUUID()}.db"
    private lateinit var runtime: ContentQueueRuntime
    private lateinit var runner: ContentPhaseRunner
    private val gates = mutableListOf<CountDownLatch>()
    @Before fun setup() { runtime = ContentQueueRuntime(app, name); runner = ContentPhaseRunner(runtime) { 1 }; runtime.initialize() }
    @After fun teardown() { gates.forEach { it.countDown() }; runtime.close(); app.deleteDatabase(name) }
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }
    private suspend fun eventually(block: () -> Boolean) { withTimeout(5_000) { while (!block()) delay(10) } }
    private fun rows() = runtime.store.tasksPage(ContentPhaseRunner.PREFIX, 0, Long.MAX_VALUE, 100)

    @Test fun allAdaptersShareTheSameNetworkCapacityAndWaitingCancellationDoesNoWork() = scenario {
        val other = runtime.store.enqueue("page", "page", Pool.NETWORK, "x")
        val held = runtime.store.claim(other, Pool.NETWORK, 1)
        val calls = AtomicInteger()
        val job = launch { runner.run("ugoira", "zip", "zip", Pool.NETWORK) { calls.incrementAndGet() } }
        eventually { rows().singleOrNull()?.state == State.QUEUED }
        assertEquals(0, calls.get())
        job.cancelAndJoin()
        assertEquals(State.CANCELLED, rows().single().state)
        assertEquals(0, calls.get())
        runtime.store.finish(other, held.attempt, true, null)
    }

    @Test fun cancellationKeepsTheSlotUntilAStubbornPhysicalBodyAndItsCleanupExit() = scenario {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1).also { gates += it }
        val cleaning = CompletableDeferred<Unit>()
        val finishCleanup = CompletableDeferred<Unit>()
        val job = launch {
            runner.run("ugoira", "zip", "zip", Pool.NETWORK) {
                try { entered.countDown(); check(release.await(5, TimeUnit.SECONDS)); currentCoroutineContext().ensureActive() }
                finally { withContext(NonCancellable) { cleaning.complete(Unit); withTimeout(5_000) { finishCleanup.await() } } }
            }
        }
        withContext(Dispatchers.IO) { check(entered.await(5, TimeUnit.SECONDS)) }
        val other = runtime.store.enqueue("other", "other", Pool.NETWORK, "x")
        job.cancel()
        assertNull(runtime.store.claim(other, Pool.NETWORK, 1))
        release.countDown(); cleaning.await()
        assertNull(runtime.store.claim(other, Pool.NETWORK, 1))
        finishCleanup.complete(Unit); job.join()
        assertEquals(State.CANCELLED, rows().single().state)
        val lease = runtime.store.claim(other, Pool.NETWORK, 1)
        assertNotNull(lease); runtime.store.finish(other, lease.attempt, true, null)
    }

    @Test fun capacityOnePipelineReleasesNetworkBeforeWaitingForCopyAndEncode() = scenario {
        val phases = mutableListOf<String>()
        runner.run("ugoira", "zip", "zip", Pool.NETWORK) { phases += "zip" }
        runner.run("ugoira", "frames", "frames", Pool.COPY) { phases += "frames" }
        runner.run("ugoira", "video", "video", Pool.ENCODE) { phases += "video" }
        assertEquals(listOf("zip", "frames", "video"), phases)
        assertTrue(rows().all { it.state == State.SUCCEEDED && it.attempt == null })
    }

    @Test fun resourceIdentitySerializesDifferentAdaptersEvenAcrossPools() = scenario {
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        val first = launch { runner.run("play", "video", "same-output", Pool.ENCODE) { entered.complete(Unit); release.await() } }
        entered.await()
        val calls = AtomicInteger()
        val second = launch { runner.run("save", "copy", "same-output", Pool.COPY) { calls.incrementAndGet() } }
        eventually { rows().size == 2 }
        assertEquals(0, calls.get())
        release.complete(Unit); first.join(); second.join()
        assertEquals(1, calls.get())
    }

    @Test fun nestedStageFailsWithoutWaitingForItsOwnCapacity() = scenario {
        try {
            runner.run("parent", "outer", "a", Pool.NETWORK) {
                runner.run("parent", "inner", "b", Pool.NETWORK) { fail("nested execution") }
            }
            fail("nested claim must be rejected")
        } catch (_: IllegalStateException) { }
        assertEquals(State.FAILED, rows().single().state)
    }

    @Test fun anotherAdapterDoesNotRecoverALiveStageAndColdStartupRetiresOnlyOldChildren() = scenario {
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        val first = launch { runner.run("play", "zip", "zip", Pool.NETWORK) { entered.complete(Unit); release.await() } }
        entered.await()
        ContentPhaseRunner(runtime) { 1 }.run("save", "copy", "copy", Pool.COPY) { }
        assertEquals(State.RUNNING, rows().first().state)
        release.complete(Unit); first.join()
        val abandoned = ContentPhaseRunner.PREFIX + "old-process"
        runtime.store.admit(abandoned, abandoned, "old", Pool.NETWORK, "{}", State.QUEUED, false)
        runtime.store.claim(abandoned, Pool.NETWORK, 1)
        val original = runtime.store.enqueue("page", "page", Pool.COPY, "x")
        runtime.store.claim(original, Pool.COPY, 1)
        runtime.close()
        runtime = ContentQueueRuntime(app, name); runtime.initialize()
        assertEquals(State.CANCELLED, runtime.store.get(abandoned).state)
        assertEquals(State.QUEUED, runtime.store.get(original).state)
    }
}
