package ceui.pixiv.download.queue;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import androidx.annotation.WorkerThread;
import java.util.UUID;

/** Durable admission core. Call on IO; workers run only after claim() commits.
 * A process owner must call recoverAfterProcessDeath() once before starting its workers.
 * Opening another store handle must not revoke live workers in the same process.
 */
@WorkerThread
public final class ContentQueueStore extends SQLiteOpenHelper {
    public enum State { QUEUED, RUNNING, PAUSE_REQUESTED, CANCEL_REQUESTED, PAUSED, FAILED, SUCCEEDED, CANCELLED }
    public enum Pool { NETWORK, COPY, ENCODE }
    public static final class Task {
        public final String id, outputKey, resourceKey, payload, attempt, error;
        public final Pool pool;
        public final State state;
        Task(Cursor c) {
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
        super(context.getApplicationContext(), databaseName, null, 1);
    }
    @Override public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE tasks (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, output_key TEXT NOT NULL, resource_key TEXT NOT NULL, pool TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, attempt TEXT, error TEXT)");
        db.execSQL("CREATE INDEX tasks_ready ON tasks(pool,state,seq)");
        db.execSQL("CREATE INDEX tasks_output ON tasks(output_key,state)");
        db.execSQL("CREATE INDEX tasks_resource ON tasks(resource_key,state)");
    }
    @Override public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        throw new IllegalStateException("Explicit content queue migration required");
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
    private static final String LIVE = "('RUNNING','PAUSE_REQUESTED','CANCEL_REQUESTED')";
    /** Atomically reserve capacity and a resource. A cancellation request still owns its slot. */
    public Task claim(Pool pool, int capacity) {
        if (capacity < 1) return null;
        return transaction(db -> {
            try (Cursor c = db.rawQuery("SELECT count(*) FROM tasks WHERE pool=? AND state IN " + LIVE, new String[]{pool.name()})) {
                c.moveToFirst(); if (c.getInt(0) >= capacity) return null;
            }
            String id;
            try (Cursor c = db.rawQuery("SELECT id FROM tasks t WHERE pool=? AND state='QUEUED' AND NOT EXISTS (SELECT 1 FROM tasks live WHERE (live.resource_key=t.resource_key OR live.output_key=t.output_key) AND live.state IN " + LIVE + ") ORDER BY seq LIMIT 1", new String[]{pool.name()})) {
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
            if (task == null || task.state == State.SUCCEEDED || task.state == State.CANCELLED) return null;
            boolean live = task.state == State.RUNNING || task.state == State.PAUSE_REQUESTED || task.state == State.CANCEL_REQUESTED;
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
                    || !(task.state == State.RUNNING || task.state == State.PAUSE_REQUESTED || task.state == State.CANCEL_REQUESTED)) return false;
            State state = committed ? State.SUCCEEDED : task.state == State.CANCEL_REQUESTED ? State.CANCELLED
                    : task.state == State.PAUSE_REQUESTED ? State.PAUSED : State.FAILED;
            ContentValues v = new ContentValues(); v.put("state", state.name()); v.putNull("attempt");
            if (state == State.FAILED) v.put("error", error); else v.putNull("error");
            db.update("tasks", v, "id=?", new String[]{id});
            return true;
        });
    }
    /** Call only when no previous-process worker can still run; output reconciliation precedes replay. */
    public void recoverAfterProcessDeath() {
        transaction(db -> {
            db.execSQL("UPDATE tasks SET state=CASE state WHEN 'RUNNING' THEN 'QUEUED' WHEN 'PAUSE_REQUESTED' THEN 'PAUSED' WHEN 'CANCEL_REQUESTED' THEN 'CANCELLED' END, attempt=NULL WHERE state IN " + LIVE);
            return null;
        });
    }
}
