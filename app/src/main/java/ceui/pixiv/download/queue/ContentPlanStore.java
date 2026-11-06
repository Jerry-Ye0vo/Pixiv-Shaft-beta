package ceui.pixiv.download.queue;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import androidx.annotation.WorkerThread;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import ceui.pixiv.download.queue.ContentQueueStore.State;

/** Durable parent intentions. A coordinating parent never consumes a physical transfer/copy slot. */
@WorkerThread
public final class ContentPlanStore {
    static void createControls(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE plan_controls (plan_id TEXT PRIMARY KEY NOT NULL, finish_requested INTEGER NOT NULL DEFAULT 0, selected_format TEXT)");
        db.execSQL("CREATE TABLE plan_children (child_family TEXT PRIMARY KEY NOT NULL, parent_id TEXT NOT NULL)");
    }
    private final ContentQueueStore owner;
    private static final String LIVE = "('RUNNING','PAUSE_REQUESTED','YIELD_REQUESTED','CANCEL_REQUESTED','COMMITTED')";
    public ContentPlanStore(ContentQueueStore owner) { this.owner = owner; }

    static void createSchema(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE plans (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, family TEXT NOT NULL, kind TEXT NOT NULL, request TEXT NOT NULL, checkpoint TEXT NOT NULL, state TEXT NOT NULL, attempt TEXT, error TEXT)");
        db.execSQL("CREATE INDEX plans_ready ON plans(state,seq)");
        db.execSQL("CREATE INDEX plans_family ON plans(family,state,seq)");
        db.execSQL("CREATE INDEX plans_kind ON plans(kind,state,seq)");
    }
    static void recover(SQLiteDatabase db) {
        db.execSQL("UPDATE plans SET state=CASE state WHEN 'PAUSE_REQUESTED' THEN 'PAUSED' WHEN 'CANCEL_REQUESTED' THEN 'CANCELLED' WHEN 'COMMITTED' THEN 'SUCCEEDED' ELSE 'QUEUED' END, attempt=NULL WHERE state IN " + LIVE);
    }
    public static final class Plan {
        public final long sequence;
        public final String id, family, kind, request, checkpoint, attempt, error;
        public final State state;
        Plan(Cursor c) {
            sequence = c.getLong(c.getColumnIndexOrThrow("seq"));
            id = c.getString(c.getColumnIndexOrThrow("id")); family = c.getString(c.getColumnIndexOrThrow("family"));
            kind = c.getString(c.getColumnIndexOrThrow("kind")); request = c.getString(c.getColumnIndexOrThrow("request"));
            checkpoint = c.getString(c.getColumnIndexOrThrow("checkpoint")); attempt = c.getString(c.getColumnIndexOrThrow("attempt"));
            error = c.getString(c.getColumnIndexOrThrow("error")); state = State.valueOf(c.getString(c.getColumnIndexOrThrow("state")));
        }
    }
    private interface Transaction<T> { T run(SQLiteDatabase db); }
    private <T> T transaction(Transaction<T> action) {
        SQLiteDatabase db = owner.getWritableDatabase(); db.beginTransaction();
        try { T result = action.run(db); db.setTransactionSuccessful(); return result; }
        finally { db.endTransaction(); }
    }
    private static Plan read(SQLiteDatabase db, String id) {
        try (Cursor c = db.rawQuery("SELECT * FROM plans WHERE id=?", new String[]{id})) {
            return c.moveToFirst() ? new Plan(c) : null;
        }
    }
    private static boolean live(State state) {
        return state == State.RUNNING || state == State.PAUSE_REQUESTED || state == State.YIELD_REQUESTED || state == State.CANCEL_REQUESTED || state == State.COMMITTED;
    }
    public Plan get(String id) { return read(owner.getReadableDatabase(), id); }
    public Plan succeeded(String family, String kind) {
        try (Cursor c = owner.getReadableDatabase().rawQuery(
                "SELECT * FROM plans WHERE family=? AND kind=? AND state='SUCCEEDED' ORDER BY seq DESC LIMIT 1", new String[]{family, kind})) {
            return c.moveToFirst() ? new Plan(c) : null;
        }
    }
    public Plan latest(String family, String kind) {
        try (Cursor c = owner.getReadableDatabase().rawQuery(
                "SELECT * FROM plans WHERE family=? AND kind=? ORDER BY seq DESC LIMIT 1", new String[]{family, kind})) {
            return c.moveToFirst() ? new Plan(c) : null;
        }
    }
    public void requestFinish(String id) {
        transaction(db -> {
            ContentValues key = new ContentValues(); key.put("plan_id", id);
            db.insertWithOnConflict("plan_controls", null, key, SQLiteDatabase.CONFLICT_IGNORE);
            ContentValues value = new ContentValues(); value.put("finish_requested", 1);
            db.update("plan_controls", value, "plan_id=?", new String[]{id}); return null;
        });
    }
    public void selectFormat(String id, String format) {
        transaction(db -> {
            ContentValues key = new ContentValues(); key.put("plan_id", id);
            db.insertWithOnConflict("plan_controls", null, key, SQLiteDatabase.CONFLICT_IGNORE);
            ContentValues value = new ContentValues(); value.put("selected_format", format);
            db.update("plan_controls", value, "plan_id=?", new String[]{id}); return null;
        });
    }
    public String selectedFormat(String id) {
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT selected_format FROM plan_controls WHERE plan_id=?", new String[]{id})) {
            return c.moveToFirst() && !c.isNull(0) ? c.getString(0) : null;
        }
    }
    public void ownChild(String parentId, String childFamily) {
        ContentValues value = new ContentValues(); value.put("child_family", childFamily); value.put("parent_id", parentId);
        owner.getWritableDatabase().insertWithOnConflict("plan_children", null, value, SQLiteDatabase.CONFLICT_IGNORE);
    }
    public State parentState(String family) {
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT p.state FROM plans p JOIN plan_children c ON p.id=c.parent_id WHERE c.child_family=?", new String[]{family})) {
            return c.moveToFirst() ? State.valueOf(c.getString(0)) : null;
        }
    }
    public List<Plan> children(String parentId) {
        List<Plan> rows = new ArrayList<>();
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT p.* FROM plans p JOIN plan_children c ON p.family=c.child_family WHERE c.parent_id=? ORDER BY p.seq", new String[]{parentId})) {
            while (c.moveToNext()) rows.add(new Plan(c));
        }
        return rows;
    }
    public boolean finishRequested(String id) {
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT finish_requested FROM plan_controls WHERE plan_id=?", new String[]{id})) {
            return c.moveToFirst() && c.getInt(0) != 0;
        }
    }
    /** Immutable request and mutable checkpoint are separate, so a retry can identify the same intent. */
    public Plan admit(String id, String family, String kind, String request) {
        return admit(id, family, kind, request, false);
    }
    public Plan admit(String id, String family, String kind, String request, boolean successor) {
        return admit(id, family, kind, request, successor, State.QUEUED);
    }
    public Plan admit(String id, String family, String kind, String request, boolean successor, State initial) {
        if (initial != State.QUEUED && initial != State.PAUSED && initial != State.FAILED)
            throw new IllegalArgumentException("Invalid initial parent state");
        if (id == null || id.isEmpty() || family == null || family.isEmpty() || kind == null || kind.isEmpty() || request == null)
            throw new IllegalArgumentException("Missing parent identity");
        return transaction(db -> {
            if (!successor) try (Cursor c = db.rawQuery("SELECT * FROM plans WHERE family=? AND state NOT IN ('SUCCEEDED','CANCELLED','CANCEL_REQUESTED') ORDER BY seq DESC LIMIT 1", new String[]{family})) {
                if (c.moveToFirst()) return new Plan(c);
            }
            ContentValues v = new ContentValues(); v.put("id", id); v.put("family", family); v.put("kind", kind);
            v.put("request", request); v.put("checkpoint", "{}"); v.put("state", initial.name());
            db.insertOrThrow("plans", null, v); return read(db, id);
        });
    }
    /** Soft parent planning width is distinct from NETWORK/COPY/ENCODE authorization. */
    public Plan claim(String id, int capacity) {
        if (capacity < 1) return null;
        return transaction(db -> {
            Plan plan = read(db, id);
            if (plan == null || plan.state != State.QUEUED) return null;
            State parent = parentState(plan.family);
            if (parent != null && parent != State.RUNNING) return null;
            String lane = plan.kind.endsWith("-group") ? "substr(kind,-6)='-group'" : "substr(kind,-6)<>'-group'";
            try (Cursor c = db.rawQuery("SELECT count(*) FROM plans WHERE " + lane + " AND state IN " + LIVE, null)) {
                c.moveToFirst(); if (c.getInt(0) >= capacity) return null;
            }
            try (Cursor c = db.rawQuery("SELECT 1 FROM plans WHERE family=? AND id<>? AND state IN " + LIVE + " LIMIT 1", new String[]{plan.family, id})) {
                if (c.moveToFirst()) return null;
            }
            ContentValues v = new ContentValues(); v.put("state", State.RUNNING.name());
            v.put("attempt", UUID.randomUUID().toString()); v.putNull("error");
            db.update("plans", v, "id=?", new String[]{id}); return read(db, id);
        });
    }
    private void stop(String id, boolean cancel) {
        transaction(db -> {
            Plan plan = read(db, id);
            if (plan == null || plan.state == State.SUCCEEDED || plan.state == State.CANCELLED || plan.state == State.COMMITTED) return null;
            if (plan.state == State.CANCEL_REQUESTED) return null;
            ContentValues v = new ContentValues();
            v.put("state", (live(plan.state) ? cancel ? State.CANCEL_REQUESTED : State.PAUSE_REQUESTED
                    : cancel ? State.CANCELLED : State.PAUSED).name());
            db.update("plans", v, "id=?", new String[]{id}); return null;
        });
    }
    public void pause(String id) { stop(id, false); }
    public void cancel(String id) { stop(id, true); }
    public void yieldForPolicy(String id) {
        ContentValues v = new ContentValues(); v.put("state", State.YIELD_REQUESTED.name());
        owner.getWritableDatabase().update("plans", v, "id=? AND state='RUNNING'", new String[]{id});
    }
    public boolean resume(String id) {
        ContentValues v = new ContentValues(); v.put("state", State.QUEUED.name()); v.putNull("error");
        return owner.getWritableDatabase().update("plans", v, "id=? AND state IN ('PAUSED','FAILED')", new String[]{id}) == 1;
    }
    public boolean checkpoint(String id, String attempt, String checkpoint) {
        if (attempt == null || checkpoint == null) return false;
        return transaction(db -> {
            Plan plan = read(db, id);
            if (plan == null || !attempt.equals(plan.attempt) || !live(plan.state)) return false;
            ContentValues v = new ContentValues(); v.put("checkpoint", checkpoint);
            return db.update("plans", v, "id=?", new String[]{id}) == 1;
        });
    }
    public boolean markCommitted(String id, String attempt) {
        if (attempt == null) return false;
        return transaction(db -> {
            Plan plan = read(db, id);
            if (plan == null || !attempt.equals(plan.attempt) || !live(plan.state)) return false;
            ContentValues v = new ContentValues(); v.put("state", State.COMMITTED.name()); v.putNull("error");
            return db.update("plans", v, "id=?", new String[]{id}) == 1;
        });
    }
    public boolean finish(String id, String attempt, boolean committed, String error) {
        if (attempt == null) return false;
        return transaction(db -> {
            Plan plan = read(db, id);
            if (plan == null || !attempt.equals(plan.attempt) || !live(plan.state)) return false;
            State state = committed || plan.state == State.COMMITTED ? State.SUCCEEDED
                    : plan.state == State.CANCEL_REQUESTED ? State.CANCELLED
                    : plan.state == State.PAUSE_REQUESTED ? State.PAUSED
                    : plan.state == State.YIELD_REQUESTED ? State.QUEUED : State.FAILED;
            ContentValues v = new ContentValues(); v.put("state", state.name()); v.putNull("attempt");
            if (state == State.FAILED) v.put("error", error); else v.putNull("error");
            return db.update("plans", v, "id=?", new String[]{id}) == 1;
        });
    }
    public long highWaterMark() {
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT COALESCE(MAX(seq),0) FROM plans", null)) { c.moveToFirst(); return c.getLong(0); }
    }
    public List<Plan> activePage(long after, long through, int limit) {
        List<Plan> result = new ArrayList<>();
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT * FROM plans WHERE seq>? AND seq<=? AND state NOT IN ('SUCCEEDED','CANCELLED') ORDER BY seq LIMIT ?",
                new String[]{String.valueOf(after), String.valueOf(through), String.valueOf(limit)})) {
            while (c.moveToNext()) result.add(new Plan(c));
        }
        return result;
    }
    public List<Plan> recent(String kind, State state, int limit) {
        List<Plan> result = new ArrayList<>();
        try (Cursor c = owner.getReadableDatabase().rawQuery("SELECT * FROM plans WHERE kind=? AND state=? ORDER BY seq DESC LIMIT ?", new String[]{kind, state.name(), String.valueOf(limit)})) {
            while (c.moveToNext()) result.add(new Plan(c));
        }
        return result;
    }
}
