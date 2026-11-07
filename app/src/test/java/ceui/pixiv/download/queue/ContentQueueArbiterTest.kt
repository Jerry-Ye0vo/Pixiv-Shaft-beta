package ceui.pixiv.download.queue

import android.app.Application
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.util.UUID
import kotlinx.coroutines.*
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ContentQueueArbiterTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "arbiter-${UUID.randomUUID()}.db"
    private var now = 0L
    private lateinit var runtime: ContentQueueRuntime
    @Before fun setup() { runtime = ContentQueueRuntime(app, name, { now }); runtime.initialize() }
    @After fun teardown() { runtime.close(); app.deleteDatabase(name) }
    private fun enqueue(id: String, pool: Pool = Pool.NETWORK, resource: String = id): String {
        runtime.store.admit(id, "output:$id", resource, pool, "{}", State.QUEUED, false); return id
    }
    private fun claim(id: String, priority: Int = ContentQueueRuntime.SAVE, capacity: Int = 1) =
        runtime.claim(id, runtime.store.get(id).pool, capacity, priority)
    private fun release(lease: ContentQueueStore.Task) {
        assertTrue(runtime.store.finish(lease.id, lease.attempt, true, null)); runtime.changed()
    }
    @Test fun currentViewBeatsNewSavedWorkAcrossDifferentAdapters() {
        val busy = runtime.store.claim(enqueue("busy"), Pool.NETWORK, 1)
        enqueue("save"); enqueue("view")
        assertNull(claim("save")); assertNull(claim("view", ContentQueueRuntime.VISIBLE))
        release(busy)
        assertNull(claim("save"))
        assertNotNull(claim("view", ContentQueueRuntime.VISIBLE))
    }
    @Test fun oldBackgroundWorkAgesAheadOfAContinuousStreamOfNewViews() {
        val busy = runtime.store.claim(enqueue("busy"), Pool.NETWORK, 1)
        enqueue("background"); assertNull(claim("background", ContentQueueRuntime.BACKGROUND))
        now = 4000
        enqueue("view"); assertNull(claim("view", ContentQueueRuntime.VISIBLE)); release(busy)
        assertNull(claim("view", ContentQueueRuntime.VISIBLE))
        assertNotNull(claim("background", ContentQueueRuntime.BACKGROUND))
    }
    @Test fun equalPriorityUsesDurableAdmissionOrderRatherThanWorkerWakeRace() {
        val busy = runtime.store.claim(enqueue("busy"), Pool.NETWORK, 1)
        enqueue("first"); enqueue("second")
        assertNull(claim("second")); assertNull(claim("first")); release(busy)
        assertNull(claim("second")); val first = requireNotNull(claim("first")); release(first)
        assertNotNull(claim("second"))
    }
    @Test fun aResourceBlockedWinnerDoesNotBlockIndependentWork() {
        val busy = runtime.store.claim(enqueue("busy", resource = "shared"), Pool.NETWORK, 2)
        enqueue("same", resource = "shared"); enqueue("different")
        assertNull(claim("same", ContentQueueRuntime.VISIBLE, 2))
        assertNotNull(claim("different", capacity = 2)); release(busy)
    }
    @Test fun physicalPoolsHaveIndependentFairQueuesAndEnforceTheirSharedLimits() {
        val network = requireNotNull(claim(enqueue("network")))
        val copy = requireNotNull(claim(enqueue("copy", Pool.COPY)))
        val encode = requireNotNull(claim(enqueue("encode", Pool.ENCODE)))
        assertNull(claim(enqueue("network2"))); assertNull(claim(enqueue("copy2", Pool.COPY)))
        assertNull(claim(enqueue("encode2", Pool.ENCODE)))
        release(network); release(copy); release(encode)
        assertNotNull(claim("network2")); assertNotNull(claim("copy2")); assertNotNull(claim("encode2"))
    }
    @Test fun withdrawnOrPausedWaitersCannotReservePriorityOverEligibleWork() {
        val busy = runtime.store.claim(enqueue("busy"), Pool.NETWORK, 1)
        enqueue("paused"); enqueue("withdrawn"); enqueue("save")
        assertNull(claim("paused", ContentQueueRuntime.VISIBLE))
        assertNull(claim("withdrawn", ContentQueueRuntime.VISIBLE)); assertNull(claim("save"))
        runtime.store.pause("paused"); runtime.withdraw("withdrawn"); release(busy)
        assertNotNull(claim("save"))
    }
    @Test fun aSharedSourceCanBePromotedWhenAViewJoinsTheSavedSource() {
        val busy = runtime.store.claim(enqueue("busy"), Pool.NETWORK, 1)
        enqueue("older"); enqueue("shared")
        assertNull(claim("older")); assertNull(claim("shared"))
        runtime.prioritize("shared", ContentQueueRuntime.VISIBLE); release(busy)
        assertNull(claim("older")); assertNotNull(claim("shared", ContentQueueRuntime.VISIBLE))
    }
    @Test fun aStoppingWorkerKeepsCapacityUntilItsActualAcknowledgement() {
        val old = requireNotNull(claim(enqueue("old")))
        runtime.store.cancel(old.id)
        enqueue("view"); assertNull(claim("view", ContentQueueRuntime.VISIBLE))
        assertTrue(runtime.store.finish(old.id, old.attempt, false, null)); runtime.changed()
        assertNotNull(claim("view", ContentQueueRuntime.VISIBLE))
    }
    @Test fun compactingInternalStagesPreservesLegacyTombstonesAndEveryLivePermit() {
        val live = runtime.store.claim(enqueue("phase:live"), Pool.NETWORK, 1)
        for (i in 0..9) {
            val id = enqueue("phase:old:$i", Pool.COPY)
            val lease = runtime.store.claim(id, Pool.COPY, 1)
            runtime.store.finish(id, lease.attempt, true, null)
        }
        enqueue("page:legacy", Pool.COPY); runtime.store.cancel("page:legacy")
        runtime.store.compactStages(3)
        assertEquals(State.RUNNING, runtime.store.get(live.id).state)
        assertEquals(State.CANCELLED, runtime.store.get("page:legacy").state)
        assertNull(runtime.store.get("phase:old:0")); assertNotNull(runtime.store.get("phase:old:9"))
    }
}
