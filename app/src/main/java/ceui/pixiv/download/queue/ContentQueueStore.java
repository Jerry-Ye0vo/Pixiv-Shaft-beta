package ceui.pixiv.download.queue;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import androidx.annotation.WorkerThread;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;

/** Durable admission core. Call on IO; workers run only after claim() commits.
 * A process owner must call recoverAfterProcessDeath() once before starting its workers.
 * Opening another store handle must not revoke live workers in the same process.
 */
@WorkerThread
public final class ContentQueueStore extends SQLiteOpenHelper {
    public enum State { QUEUED, RUNNING, PAUSE_REQUESTED, YIELD_REQUESTED, CANCEL_REQUESTED,
        FAILING, COMMITTED, PAUSED, FAILED, SUCCEEDED, CANCELLED }
    public enum Pool { NETWORK, COPY, ENCODE }
    public static final class Task {
        public final String id, outputKey, resourceKey, payload, attempt, error;
        public final Pool pool;
        public final State state;
        public final long sequence;
        Task(Cursor c) {
            sequence = c.getLong(c.getColumnIndexOrThrow("seq"));
            id = c.getString(c.getColumnIndexOrThrow("id"));
            outputKey = c.getString(c.getColumnIndexOrThrow("output_key"));
            resourceKey = c.getString(c.getColumnIndexOrThrow("resource_key"));
            payload = c.getString(c.getColumnIndexOrThrow("payload"));
            attempt = c.getString(c.getColumnIndexOrThrow("attempt"));
            error = c.getString(c.getColumnIndexOrThrow("error"));
            pool = Pool.valueOf(c.getString(c.getColumnIndexOrThrow("pool")));
            state = State.valueOf(c.getString(c.getColumnIndexOrThrow("state")));
        }
    }
    public ContentQueueStore(Context context) { this(context, "content_queue.db"); }
    public ContentQueueStore(Context context, String databaseName) {
        super(context.getApplicationContext(), databaseName, null, 3);
    }
    @Override public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE tasks (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, output_key TEXT NOT NULL, resource_key TEXT NOT NULL, pool TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, attempt TEXT, error TEXT)");
        db.execSQL("CREATE INDEX tasks_ready ON tasks(pool,state,seq)");
        db.execSQL("CREATE INDEX tasks_output ON tasks(output_key,state)");
        db.execSQL("CREATE INDEX tasks_resource ON tasks(resource_key,state)");
        ContentPlanStore.createSchema(db);
        ContentPlanStore.createControls(db);
    }
    @Override public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        if (newVersion != 3 || oldVersion < 1 || oldVersion > 2)
            throw new IllegalStateException("Explicit content queue migration required");
        if (oldVersion < 2) ContentPlanStore.createSchema(db);
        ContentPlanStore.createControls(db);
    }
    private interface Transaction<T> { T run(SQLiteDatabase db); }
    private <T> T transaction(Transaction<T> action) {
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try {
            T value = action.run(db);
            db.setTransactionSuccessful();
            return value;
        } finally { db.endTransaction(); }
    }
    /** Returns only after durable commit. Different destinations/variants need distinct output keys. */
    public String enqueue(String outputKey, String resourceKey, Pool pool, String payload) {
        if (outputKey == null || outputKey.isEmpty() || resourceKey == null || resourceKey.isEmpty()
                || pool == null || payload == null) throw new IllegalArgumentException("Missing task identity");
        return transaction(db -> {
            try (Cursor c = db.rawQuery("SELECT id FROM tasks WHERE output_key=? AND state NOT IN ('SUCCEEDED','CANCELLED','CANCEL_REQUESTED') ORDER BY seq LIMIT 1", new String[]{outputKey})) {
                if (c.moveToFirst()) return c.getString(0);
            }
            String id = UUID.randomUUID().toString();
            ContentValues v = new ContentValues();
            v.put("id", id); v.put("output_key", outputKey); v.put("resource_key", resourceKey);
            v.put("pool", pool.name()); v.put("payload", payload); v.put("state", State.QUEUED.name());
            db.insertOrThrow("tasks", null, v);
            return id;
        });
    }
    private static final String LIVE = "('RUNNING','PAUSE_REQUESTED','YIELD_REQUESTED','CANCEL_REQUESTED','FAILING','COMMITTED')";
    /** Atomically reserve capacity and a resource. A cancellation request still owns its slot. */
    public Task claim(Pool pool, int capacity) {
        return claim(null, pool, capacity);
    }

    /** The owner selects eligibility (manual/network scope); the database authorizes execution. */
    public Task claim(String requestedId, Pool pool, int capacity) {
        if (capacity < 1) return null;
        return transaction(db -> {
            try (Cursor c = db.rawQuery("SELECT count(*) FROM tasks WHERE pool=? AND state IN " + LIVE, new String[]{pool.name()})) {
                c.moveToFirst(); if (c.getInt(0) >= capacity) return null;
            }
            String id;
            String requested = requestedId == null ? "" : " AND t.id=?";
            String[] args = requestedId == null ? new String[]{pool.name()} : new String[]{pool.name(), requestedId};
            try (Cursor c = db.rawQuery("SELECT id FROM tasks t WHERE pool=? AND state='QUEUED'" + requested + " AND NOT EXISTS (SELECT 1 FROM tasks live WHERE (live.resource_key=t.resource_key OR live.output_key=t.output_key) AND live.state IN " + LIVE + ") ORDER BY seq LIMIT 1", args)) {
                if (!c.moveToFirst()) return null;
                id = c.getString(0);
            }
            ContentValues v = new ContentValues();
            v.put("state", State.RUNNING.name()); v.put("attempt", UUID.randomUUID().toString()); v.putNull("error");
            db.update("tasks", v, "id=?", new String[]{id});
            return read(db, id);
        });
    }
    public Task get(String id) { return read(getReadableDatabase(), id); }
    /** Readiness check for the process arbiter; the subsequent claim remains the atomic authority. */
    public boolean resourceAvailable(String id) {
        try (Cursor c = getReadableDatabase().rawQuery("SELECT 1 FROM tasks t WHERE t.id=? AND t.state='QUEUED' AND NOT EXISTS (SELECT 1 FROM tasks live WHERE (live.resource_key=t.resource_key OR live.output_key=t.output_key) AND live.state IN " + LIVE + ")", new String[]{id})) {
            return c.moveToFirst();
        }
    }
    private static Task read(SQLiteDatabase db, String id) {
        try (Cursor c = db.rawQuery("SELECT * FROM tasks WHERE id=?", new String[]{id})) {
            return c.moveToFirst() ? new Task(c) : null;
        }
    }
    public void pause(String id) { requestStop(id, false); }
    public void cancel(String id) { requestStop(id, true); }
    private void requestStop(String id, boolean cancel) {
        transaction(db -> {
            Task task = read(db, id);
            if (task == null || terminal(task.state) || task.state == State.COMMITTED) return null;
            boolean live = live(task.state);
            // Cancellation dominates a later pause request.
            if (task.state == State.CANCEL_REQUESTED) return null;
            ContentValues v = new ContentValues();
            v.put("state", (live ? (cancel ? State.CANCEL_REQUESTED : State.PAUSE_REQUESTED)
                    : (cancel ? State.CANCELLED : State.PAUSED)).name());
            db.update("tasks", v, "id=?", new String[]{id});
            return null;
        });
    }
    /** Resume never creates a worker, and waits for a stopping worker to acknowledge termination. */
    public boolean resume(String id) {
        return transaction(db -> {
            ContentValues v = new ContentValues(); v.put("state", State.QUEUED.name()); v.putNull("error");
            return db.update("tasks", v, "id=? AND state IN ('PAUSED','FAILED')", new String[]{id}) == 1;
        });
    }
    /** Worker calls after it has stopped using all resources. Stale attempts cannot mutate state.
     * A committed output wins over a late pause/cancel request, avoiding a duplicate save on resume.
     */
    public boolean finish(String id, String attempt, boolean committed, String error) {
        if (attempt == null) return false;
        return transaction(db -> {
            Task task = read(db, id);
            if (task == null || !attempt.equals(task.attempt)
                    || !live(task.state)) return false;
            State state = committed || task.state == State.COMMITTED ? State.SUCCEEDED
                    : task.state == State.CANCEL_REQUESTED ? State.CANCELLED
                    : task.state == State.PAUSE_REQUESTED ? State.PAUSED
                    : task.state == State.YIELD_REQUESTED ? State.QUEUED : State.FAILED;
            ContentValues v = new ContentValues(); v.put("state", state.name()); v.putNull("attempt");
            if (state == State.FAILED) v.put("error", error); else v.putNull("error");
            db.update("tasks", v, "id=?", new String[]{id});
            return true;
        });
    }
    /** Call only when no previous-process worker can still run; output reconciliation precedes replay. */
    public void recoverAfterProcessDeath() {
        transaction(db -> {
            db.execSQL("UPDATE tasks SET state=CASE state WHEN 'PAUSE_REQUESTED' THEN 'PAUSED' WHEN 'CANCEL_REQUESTED' THEN 'CANCELLED' WHEN 'COMMITTED' THEN 'SUCCEEDED' WHEN 'FAILING' THEN 'FAILED' ELSE 'QUEUED' END, attempt=NULL WHERE state IN " + LIVE);
            ContentPlanStore.recover(db);
            return null;
        });
    }

    private static boolean terminal(State state) { return state == State.SUCCEEDED || state == State.CANCELLED; }
    private static boolean live(State state) {
        return state == State.RUNNING || state == State.PAUSE_REQUESTED || state == State.YIELD_REQUESTED
                || state == State.CANCEL_REQUESTED || state == State.FAILING || state == State.COMMITTED;
    }

    /** Stable-ID handoff. Imports never revive a terminal ID or supersede a newer output intent.
     * Explicit new submissions may replace waiting intents, but cannot revoke a live worker.
     */
    public Task admit(String id, String outputKey, String resourceKey, Pool pool, String payload,
                      State initial, boolean importing) {
        if (id == null || id.isEmpty() || outputKey == null || outputKey.isEmpty() || resourceKey == null || resourceKey.isEmpty() || pool == null || payload == null
                || !(initial == State.QUEUED || initial == State.PAUSED || initial == State.FAILED)) {
            throw new IllegalArgumentException("Invalid stable admission");
        }
        return transaction(db -> {
            Task existing = read(db, id);
            if (existing != null && importing) {
                if (!existing.outputKey.equals(outputKey) || !existing.resourceKey.equals(resourceKey)) {
                    throw new IllegalStateException("Conflicting imported task ID: " + id);
                }
                return existing;
            }
            if (existing != null && (!existing.outputKey.equals(outputKey)
                    || !existing.resourceKey.equals(resourceKey) || live(existing.state))) {
                throw new IllegalStateException("Task identity is already owned: " + id);
            }
            State admittedState = initial;
            try (Cursor c = db.rawQuery("SELECT * FROM tasks WHERE output_key=? AND id<>?", new String[]{outputKey, id})) {
                while (c.moveToNext()) {
                    Task other = new Task(c);
                    if (importing) admittedState = State.CANCELLED;
                    else if (live(other.state)) throw new IllegalStateException("Output has a live owner: " + other.id);
                }
            }
            if (!importing) {
                ContentValues cancelled = new ContentValues(); cancelled.put("state", State.CANCELLED.name());
                db.update("tasks", cancelled, "output_key=? AND id<>? AND state IN ('QUEUED','PAUSED','FAILED')", new String[]{outputKey, id});
            }
            ContentValues v = new ContentValues(); v.put("output_key", outputKey); v.put("resource_key", resourceKey);
            v.put("pool", pool.name()); v.put("payload", payload); v.put("state", admittedState.name());
            v.putNull("attempt"); v.putNull("error");
            if (existing == null) { v.put("id", id); db.insertOrThrow("tasks", null, v); }
            else db.update("tasks", v, "id=?", new String[]{id});
            return read(db, id);
        });
    }

    /** Upgrade a stopped page from the old combined NETWORK stage to shared transfer + COPY.
     * Stable identity and terminal tombstones are retained; live attempts can never be rebound.
     */
    public void rebindWaiting(String id, String expectedOutput, String oldResource, String resource, Pool pool) {
        transaction(db -> {
            Task task = read(db, id);
            if (task == null || task.resourceKey.equals(resource)) return null;
            if (!task.outputKey.equals(expectedOutput) || !task.resourceKey.equals(oldResource) || live(task.state)) {
                throw new IllegalStateException("Cannot rebind owned page: " + id);
            }
            ContentValues v = new ContentValues(); v.put("resource_key", resource); v.put("pool", pool.name());
            db.update("tasks", v, "id=?", new String[]{id});
            return null;
        });
    }

    /** Record a command without releasing a running worker's permit. */
    public void updateRequest(String id, String payload, boolean paused, boolean failed) {
        transaction(db -> {
            Task task = read(db, id);
            if (task == null || terminal(task.state) || task.state == State.COMMITTED
                    || task.state == State.CANCEL_REQUESTED) return null;
            State state = task.state;
            if (failed) state = live(state) ? State.FAILING : State.FAILED;
            else if (paused) state = live(state) ? State.PAUSE_REQUESTED : State.PAUSED;
            else if (state == State.PAUSE_REQUESTED || state == State.FAILING) state = State.YIELD_REQUESTED;
            else if (state == State.PAUSED || state == State.FAILED) state = State.QUEUED;
            ContentValues v = new ContentValues(); v.put("payload", payload); v.put("state", state.name());
            db.update("tasks", v, "id=?", new String[]{id});
            return null;
        });
    }

    /** Network parking stops the current attempt without turning it into a user pause. */
    public void yieldAttempt(String id) {
        transaction(db -> {
            Task task = read(db, id);
            if (task != null && (task.state == State.RUNNING || task.state == State.FAILING)) {
                ContentValues v = new ContentValues(); v.put("state", State.YIELD_REQUESTED.name());
                db.update("tasks", v, "id=?", new String[]{id});
            }
            return null;
        });
    }

    /** Output is committed, but the worker keeps its permit through final cleanup. */
    public boolean markCommitted(String id, String attempt) {
        return transaction(db -> {
            Task task = read(db, id);
            if (task == null || attempt == null || !attempt.equals(task.attempt) || !live(task.state)) return false;
            ContentValues v = new ContentValues(); v.put("state", State.COMMITTED.name()); v.putNull("error");
            return db.update("tasks", v, "id=?", new String[]{id}) == 1;
        });
    }

    public long highWaterMark() {
        try (Cursor c = getReadableDatabase().rawQuery("SELECT IFNULL(MAX(seq),0) FROM tasks", null)) {
            c.moveToFirst(); return c.getLong(0);
        }
    }

    public List<Task> activePage(String prefix, long after, long through, int limit) {
        List<Task> tasks = new ArrayList<>();
        try (Cursor c = getReadableDatabase().rawQuery("SELECT * FROM tasks WHERE id LIKE ? AND seq>? AND seq<=? AND state NOT IN ('SUCCEEDED','CANCELLED') ORDER BY seq LIMIT ?",
                new String[]{prefix + "%", String.valueOf(after), String.valueOf(through), String.valueOf(limit)})) {
            while (c.moveToNext()) tasks.add(new Task(c));
        }
        return tasks;
    }

    /** Keyset scan for typed adapters, including terminal receipts when requested. */
    public List<Task> tasksPage(String prefix, long after, long through, int limit) {
        List<Task> tasks = new ArrayList<>();
        try (Cursor c = getReadableDatabase().rawQuery("SELECT * FROM tasks WHERE id LIKE ? AND seq>? AND seq<=? ORDER BY seq LIMIT ?",
                new String[]{prefix + "%", String.valueOf(after), String.valueOf(through), String.valueOf(limit)})) {
            while (c.moveToNext()) tasks.add(new Task(c));
        }
        return tasks;
    }

    /** Bounded warm projection; full completed history remains queryable through tasksPage. */
    public List<Task> recentTasks(String prefix, Pool pool, State state, int limit) {
        List<Task> tasks = new ArrayList<>();
        try (Cursor c = getReadableDatabase().rawQuery("SELECT * FROM tasks WHERE pool=? AND state=? AND id LIKE ? ORDER BY seq DESC LIMIT ?",
                new String[]{pool.name(), state.name(), prefix + "%", String.valueOf(limit)})) {
            while (c.moveToNext()) tasks.add(new Task(c));
        }
        return tasks;
    }

    /** Checkpoints belong to the current physical attempt; late callbacks cannot replace new input/receipts. */
    public boolean checkpoint(String id, String attempt, String payload) {
        if (attempt == null || payload == null) return false;
        return transaction(db -> {
            Task task = read(db, id);
            if (task == null || !attempt.equals(task.attempt) || !live(task.state)) return false;
            ContentValues v = new ContentValues(); v.put("payload", payload);
            return db.update("tasks", v, "id=?", new String[]{id}) == 1;
        });
    }

    /** Keep a small tombstone so an interrupted legacy import cannot resurrect completed work. */
    public void compactStages(int keep) {
        if (keep < 1) throw new IllegalArgumentException("Must retain recent diagnostics");
        getWritableDatabase().execSQL("DELETE FROM tasks WHERE id LIKE 'phase:%' AND state IN ('SUCCEEDED','FAILED','CANCELLED') AND seq NOT IN (SELECT seq FROM tasks WHERE id LIKE 'phase:%' ORDER BY seq DESC LIMIT ?)", new Object[]{keep});
    }

    public void compactTerminal(String id) {
        ContentValues v = new ContentValues(); v.put("payload", "");
        getWritableDatabase().update("tasks", v, "id=? AND state IN ('SUCCEEDED','CANCELLED')", new String[]{id});
    }

    public void cancelOrRemember(String id, String outputKey, String resourceKey) {
        transaction(db -> {
            Task task = read(db, id);
            if (task == null) {
                // Withdrawal during import/admission also needs a tombstone.
                ContentValues old = new ContentValues(); old.put("state", State.CANCELLED.name());
                db.update("tasks", old, "output_key=? AND state IN ('QUEUED','PAUSED','FAILED')", new String[]{outputKey});
                ContentValues v = new ContentValues(); v.put("id", id); v.put("output_key", outputKey);
                v.put("resource_key", resourceKey); v.put("pool", Pool.NETWORK.name()); v.put("payload", "");
                v.put("state", State.CANCELLED.name()); db.insertOrThrow("tasks", null, v);
            } else if (!terminal(task.state) && task.state != State.COMMITTED) {
                ContentValues v = new ContentValues();
                v.put("state", (live(task.state) ? State.CANCEL_REQUESTED : State.CANCELLED).name());
                db.update("tasks", v, "id=?", new String[]{id});
            }
            return null;
        });
    }

    public void cancelScope(String prefix) {
        transaction(db -> {
            db.execSQL("UPDATE tasks SET state=CASE WHEN state IN " + LIVE + " THEN 'CANCEL_REQUESTED' ELSE 'CANCELLED' END WHERE id LIKE ? AND state NOT IN ('SUCCEEDED','CANCELLED','COMMITTED')", new Object[]{prefix + "%"});
            return null;
        });
    }
}
