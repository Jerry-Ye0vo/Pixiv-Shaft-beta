package ceui.lisa.core;

import android.content.Context;
import ceui.lisa.database.AppDatabase;
import ceui.lisa.database.DownloadDao;
import ceui.pixiv.download.queue.ContentQueueStore;
import ceui.pixiv.download.queue.ContentQueueStore.Task;
import ceui.pixiv.download.queue.ContentQueueStore.State;
import ceui.pixiv.download.queue.ContentQueueStore.Pool;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import okio.ByteString;

/** Manager's process-lifetime owner. Only this adapter admits/claims page workers.
 * The old pending table is a compatibility journal, never a second execution source.
 * All methods run on IO; no caller holds the Manager monitor while entering this owner.
 */
final class PageQueueOwner implements java.io.Closeable {
    static final String PREFIX = "page:";
    final ContentQueueStore store;
    private final ceui.pixiv.download.queue.ContentQueueRuntime runtime;
    private int importEpoch;

    PageQueueOwner(Context context) { this(ceui.pixiv.download.queue.ContentQueueRuntime.get(context)); }
    PageQueueOwner(Context context, String name) { this(new ceui.pixiv.download.queue.ContentQueueRuntime(context, name)); }
    private PageQueueOwner(ceui.pixiv.download.queue.ContentQueueRuntime runtime) {
        this.runtime = runtime; store = runtime.getStore();
    }
    private void initialize() { runtime.initialize(); }
    private static boolean sharedImage(DownloadItem item) {
        return !item.getIllust().isGif();
    }
    private static Pool pool(DownloadItem item) { return sharedImage(item) ? Pool.COPY : Pool.NETWORK; }
    private void migratePage(DownloadItem item) {
        if (sharedImage(item)) store.rebindWaiting(id(item), output(item),
                "pixiv-bytes:" + ByteString.encodeUtf8(item.getUrl()).sha256().hex(), resource(item), Pool.COPY);
    }
    static String id(DownloadItem item) { return PREFIX + item.getUuid(); }
    private static String output(DownloadItem item) {
        return "page-output:" + ByteString.encodeUtf8(PageQueuePersistence.key(item)).sha256().hex();
    }
    private static String resource(DownloadItem item) {
        return (sharedImage(item) ? "page-copy:" : "pixiv-bytes:") + ByteString.encodeUtf8(item.getUrl()).sha256().hex();
    }
    private static State initial(DownloadItem item) {
        return item.isFailed() ? State.FAILED : item.isPaused() ? State.PAUSED : State.QUEUED;
    }
    private static String payload(Gson gson, DownloadItem item) {
        return PageQueuePersistence.snapshot(gson, item, item.isPaused(), item.getState());
    }

    synchronized void admit(AppDatabase database, Gson gson, DownloadItem item) {
        initialize();
        // Legacy first: if the process stops between commits, restart imports this durable intent.
        // A core failure never authorizes a worker; no fallback to direct execution.
        PageQueuePersistence.admit(database, gson, item);
        migratePage(item);
        store.admit(id(item), output(item), resource(item), pool(item), payload(gson, item), initial(item), false);
    }

    private void importOne(DownloadDao dao, Gson gson, DownloadItem item) {
        migratePage(item);
        Task task = store.admit(id(item), output(item), resource(item), pool(item),
                payload(gson, item), initial(item), true);
        if (task.state == State.SUCCEEDED || task.state == State.CANCELLED) {
            dao.deleteDownloadingByIdentity(item.getName(), item.getUuid());
            store.compactTerminal(task.id);
        }
    }

    List<DownloadItem> restore(AppDatabase database, Gson gson) {
        initialize();
        final int epoch;
        synchronized (this) { epoch = importEpoch; }
        List<DownloadItem> old = Manager.readRestoredDownloads(database.downloadDao(), gson);
        for (DownloadItem item : old) {
            synchronized (this) {
                if (epoch != importEpoch) return new ArrayList<>();
                importOne(database.downloadDao(), gson, item);
            }
        }
        List<DownloadItem> result = new ArrayList<>();
        Gson restoring = Manager.restoringGson(gson);
        long after = 0, through = store.highWaterMark();
        while (true) {
            List<Task> page = store.activePage(PREFIX, after, through, 20);
            if (page.isEmpty()) return result;
            for (Task task : page) {
                after = task.sequence;
                if (task.state == State.COMMITTED || task.state == State.CANCEL_REQUESTED) continue;
                DownloadItem item = restoring.fromJson(task.payload, DownloadItem.class);
                if (item == null || item.getIllust() == null || !id(item).equals(task.id)) {
                    throw new IllegalStateException("Unreadable core page intent: " + task.id);
                }
                migratePage(item);
                applyState(item, task.state);
                result.add(item);
            }
        }
    }

    static void applyState(DownloadItem item, State state) {
        item.setPaused(state == State.PAUSED || state == State.PAUSE_REQUESTED);
        item.setState(state == State.FAILED || state == State.FAILING ? DownloadItem.DownloadState.FAILED
                : state == State.RUNNING ? DownloadItem.DownloadState.DOWNLOADING : DownloadItem.DownloadState.INIT);
    }

    synchronized void update(AppDatabase database, Gson gson, DownloadItem item, boolean paused, int state) {
        initialize();
        String json = PageQueuePersistence.snapshot(gson, item, paused, state);
        store.updateRequest(id(item), json, paused, state == DownloadItem.DownloadState.FAILED);
        database.downloadDao().updateDownloadingState(item.getName(), item.getUuid(), json);
    }

    synchronized Task claim(DownloadItem item, int capacity) {
        initialize();
        Task task = store.get(id(item));
        return task == null ? null : runtime.claim(id(item), task.pool, capacity);
    }

    synchronized void park(DownloadItem item) { initialize(); store.yieldAttempt(id(item)); }

    synchronized void cancel(AppDatabase database, DownloadItem item) {
        initialize();
        store.cancelOrRemember(id(item), output(item), resource(item));
        database.downloadDao().deleteDownloadingByIdentity(item.getName(), item.getUuid());
        store.compactTerminal(id(item));
    }

    synchronized void clear(AppDatabase database, Gson gson) {
        initialize(); importEpoch++;
        // Include records not yet seen by the asynchronous startup reader before clearing its source.
        for (DownloadItem item : Manager.readRestoredDownloads(database.downloadDao(), gson)) {
            importOne(database.downloadDao(), gson, item);
        }
        store.cancelScope(PREFIX);
        database.downloadDao().deleteAllDownloading();
    }

    synchronized void committed(Task lease) {
        if (lease != null && !store.markCommitted(lease.id, lease.attempt)) {
            throw new IllegalStateException("Stale page completion: " + lease.id);
        }
    }

    synchronized boolean finish(AppDatabase database, Gson gson, DownloadItem item, Task lease,
                                boolean committed, boolean stopped) {
        if (lease == null) return true; // no worker was authorized (e.g. admission/preparation withdrawn)
        if (stopped && !committed) store.yieldAttempt(lease.id); // preserves an explicit pause/cancel
        if (!store.finish(lease.id, lease.attempt, committed, item.isFailed() ? "Page worker failed" : null)) return false;
        Task task = store.get(lease.id);
        if (task.state == State.SUCCEEDED || task.state == State.CANCELLED) {
            database.downloadDao().deleteDownloadingByIdentity(item.getName(), item.getUuid());
            store.compactTerminal(task.id);
        } else {
            DownloadItem snapshot = gson.fromJson(task.payload, DownloadItem.class);
            applyState(snapshot, task.state);
            database.downloadDao().updateDownloadingState(item.getName(), item.getUuid(), payload(gson, snapshot));
        }
        runtime.changed();
        return true;
    }

    @Override public void close() { runtime.close(); }
}
