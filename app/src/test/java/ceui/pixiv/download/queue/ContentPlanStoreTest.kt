package ceui.pixiv.download.queue

import android.app.Application
import android.database.sqlite.SQLiteDatabase
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.util.UUID
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class ContentPlanStoreTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val name = "parent-store-${UUID.randomUUID()}.db"
    private var helper: ContentQueueStore? = null
    @After fun teardown() { helper?.close(); app.deleteDatabase(name) }
    private fun open(): Pair<ContentQueueStore, ContentPlanStore> {
        val db = ContentQueueStore(app, name).also { helper = it }
        return db to ContentPlanStore(db)
    }
    @Test fun versionOnePhysicalTasksUpgradeWithoutChangingLegacyIdentityPayloadOrStates() {
        val path = app.getDatabasePath(name).apply { parentFile!!.mkdirs() }
        val expected = listOf(State.QUEUED, State.PAUSED, State.FAILED, State.SUCCEEDED, State.CANCELLED)
        SQLiteDatabase.openOrCreateDatabase(path, null).use { db ->
            db.execSQL("CREATE TABLE tasks (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, output_key TEXT NOT NULL, resource_key TEXT NOT NULL, pool TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, attempt TEXT, error TEXT)")
            db.execSQL("CREATE INDEX tasks_ready ON tasks(pool,state,seq)")
            db.execSQL("CREATE INDEX tasks_output ON tasks(output_key,state)")
            db.execSQL("CREATE INDEX tasks_resource ON tasks(resource_key,state)")
            expected.forEachIndexed { i, state ->
                db.execSQL("INSERT INTO tasks(id,output_key,resource_key,pool,payload,state,error) VALUES(?,?,?,?,?,?,?)",
                    arrayOf("page:$i", "output:$i", "exact-url:$i", "COPY", "{\"queueRecordVersion\":1,\"id\":$i}", state.name, "preserved:$i"))
            }
            db.version = 1
        }
        val (db, parents) = open()
        assertEquals(2, db.readableDatabase.version)
        expected.forEachIndexed { i, state ->
            val row = db.get("page:$i")
            assertEquals(state, row.state); assertEquals("exact-url:$i", row.resourceKey)
            assertEquals("{\"queueRecordVersion\":1,\"id\":$i}", row.payload)
            assertEquals("preserved:$i", row.error); assertEquals((i + 1).toLong(), row.sequence)
        }
        val plan = parents.admit("model:1", "model-target", "model", "{\"version\":1}")
        assertEquals(State.QUEUED, plan.state)
        assertNotNull(db.claim("page:0", Pool.COPY, 1))
    }

    @Test fun aLiveParentNeverConsumesAnyPhysicalPoolCapacity() {
        val (db, plans) = open()
        plans.admit("parent", "model", "model", "{}")
        assertNotNull(plans.claim("parent", 1))
        Pool.values().forEach { pool ->
            val id = db.enqueue(pool.name, pool.name, pool, "x")
            assertNotNull(db.claim(id, pool, 1))
        }
        plans.admit("second", "different-model", "model", "{}")
        assertNull(plans.claim("second", 1))
    }

    @Test fun admissionDeduplicatesOnlyAnUnfinishedFamilyAndKeepsItsFrozenRequest() {
        val (_, plans) = open()
        val first = plans.admit("first", "same-target", "model", "old-url")
        val duplicate = plans.admit("new-id", "same-target", "model", "new-url")
        assertEquals(first.id, duplicate.id); assertEquals("old-url", duplicate.request)
        plans.cancel(first.id)
        assertEquals("new-id", plans.admit("new-id", "same-target", "model", "new-url").id)
    }

    @Test fun stoppedParentsKeepTheirAttemptUntilPhysicalAcknowledgementAndRejectLateCheckpoints() {
        val (_, plans) = open()
        plans.admit("first", "same-target", "model", "frozen")
        val old = plans.claim("first", 1)
        assertTrue(plans.checkpoint(old.id, old.attempt, "owned-input"))
        plans.pause(old.id)
        assertEquals(State.PAUSE_REQUESTED, plans.get(old.id).state)
        plans.admit("second", "other-target", "model", "other")
        assertNull(plans.claim("second", 1))
        assertTrue(plans.finish(old.id, old.attempt, false, null))
        assertEquals(State.PAUSED, plans.get(old.id).state)
        assertTrue(plans.resume(old.id))
        val current = plans.claim(old.id, 1)
        assertNotEquals(old.attempt, current.attempt)
        assertEquals("owned-input", current.checkpoint)
        assertFalse(plans.checkpoint(old.id, old.attempt, "stale"))
        assertFalse(plans.finish(old.id, old.attempt, true, null))
        assertEquals("frozen", plans.get(old.id).request)
    }

    @Test fun openingAnotherHandleDoesNotRevokeLiveParentsButProcessRecoveryRespectsStopAndCommit() {
        val (db, plans) = open()
        listOf("run", "pause", "cancel", "commit").forEach { id ->
            plans.admit(id, id, "model", "request:$id")
            val row = plans.claim(id, 4)
            plans.checkpoint(id, row.attempt, "checkpoint:$id")
            when (id) { "pause" -> plans.pause(id); "cancel" -> plans.cancel(id); "commit" -> plans.markCommitted(id, row.attempt) }
        }
        ContentQueueStore(app, name).use { other ->
            assertEquals(State.RUNNING, ContentPlanStore(other).get("run").state)
            assertNotNull(ContentPlanStore(other).get("run").attempt)
        }
        db.recoverAfterProcessDeath()
        assertEquals(State.QUEUED, plans.get("run").state)
        assertEquals(State.PAUSED, plans.get("pause").state)
        assertEquals(State.CANCELLED, plans.get("cancel").state)
        assertEquals(State.SUCCEEDED, plans.get("commit").state)
        listOf("run", "pause", "cancel", "commit").forEach { id ->
            assertNull(plans.get(id).attempt); assertEquals("checkpoint:$id", plans.get(id).checkpoint)
        }
    }
}
