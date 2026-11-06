package ceui.lisa.core;


import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.os.Handler;
import android.os.Looper;

import androidx.localbroadcastmanager.content.LocalBroadcastManager;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import ceui.lisa.R;
import ceui.lisa.activities.Shaft;
import ceui.lisa.database.AppDatabase;
import ceui.lisa.database.DownloadDao;
import ceui.lisa.database.DownloadEntity;
import ceui.lisa.download.DownloadFileFactory;
import ceui.lisa.download.DownloadProgress;
import ceui.lisa.download.MediaStoreUtil;
import ceui.lisa.helper.Android10DownloadFactory22;
import ceui.lisa.helper.SAFactory;
import ceui.lisa.interfaces.Callback;
import ceui.lisa.model.Holder;
import ceui.lisa.utils.Common;
import ceui.lisa.utils.Dev;
import ceui.lisa.utils.DownloadLimitTypeUtil;
import ceui.lisa.utils.Params;
import ceui.lisa.utils.PixivOperate;
import ceui.pixiv.download.DownloadsRegistry;
import ceui.pixiv.api.model.Illust;
import ceui.pixiv.download.RecordedPageProbe;
import ceui.pixiv.download.StageStore;
import ceui.pixiv.download.StorageSpaceGuard;
import ceui.pixiv.download.aria2.Aria2Dispatcher;
import ceui.pixiv.download.toast.DownloadToastKind;
import ceui.pixiv.download.toast.DownloadToasts;
import ceui.pixiv.imageloader.ImageLoaderV3;
import ceui.pixiv.progress.ProgressTracker;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.ByteString;
import timber.log.Timber;

public class Manager {

    private final Context mContext = Shaft.getContext();
    private final PageQueueOwner pageQueue = new PageQueueOwner(mContext);
    private final Map<String, ceui.pixiv.download.queue.ContentQueueStore.Task> leases = new ConcurrentHashMap<>();
    private final Set<String> blockedClaims = ConcurrentHashMap.newKeySet();
    private final Set<String> finalizing = ConcurrentHashMap.newKeySet();
    private java.util.function.Consumer<DownloadItem> workerStarter = item -> downloadOne(mContext, item);
    /**
     * 下载列表。用 {@link CopyOnWriteArrayList}：写入（add/remove/clear）仍走
     * {@code synchronized (this)} 保证复合操作原子（去重 + add），但任何线程的遍历
     * （{@link #activeCount}/{@link #getFirstReady}/外部 {@link #getContent()}/
     * {@link #contentSnapshot()}）天然拿到一致快照，不会再撞
     * ConcurrentModificationException。列表规模是「进行中的 page 数」量级，
     * 写时拷贝的代价可忽略。
     */
    private List<DownloadItem> content = new CopyOnWriteArrayList<>();
    /**
     * 下载 IO 线程池（替代原先的 RxJava {@code Schedulers.io()}）：无界缓存池，空闲线程 60s
     * 回收，线程名 {@code shaft-dl-io-N}。restore / addTask 的 DB 写、传输 Body、传输完成后的
     * DB + finishWrite 收尾全在这里跑；UI mutation 一律 {@link #MAIN} post 回主线程。
     */
    private static final ExecutorService IO = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger seq = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "shaft-dl-io-" + seq.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });
    /** Ordered persistence commands; never wait for Room while holding the UI-facing monitor. */
    private final ExecutorService queueWrites = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "shaft-page-queue-store");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<String, Object> pendingPersistence = new ConcurrentHashMap<>();
    private final Set<String> persistenceFailures = ConcurrentHashMap.newKeySet();
    /**
     * 主线程投递（替代 {@code AndroidSchedulers.mainThread().scheduleDirect}）。永远 post、
     * 从不同步执行——哪怕当前已在主线程——保持 Rx 的排队语义（多处逻辑依赖"content.remove
     * 先入队、广播第二个 Runnable"这种 FIFO 顺序）。
     */
    private static final class MainHolder {
        // 惰性：JVM 单测里 Looper 未 mock，Manager 的静态工具方法（openStageStream 等）不能因此初始化失败。
        static final Handler MAIN = new Handler(Looper.getMainLooper());
    }

    private static void postMain(Runnable r) {
        MainHolder.MAIN.post(r);
    }
    /**
     * 多任务并发下载的运行句柄表（key = item.uuid），值是 {@link DownloadTask}（替代 Rx
     * {@code Disposable}）。旧设计是单一 handle（串行），现在支持 1-5 并发：每个正在传的
     * page 各占一个 entry。stopOne(uuid) 只 cancel 那一个；stopAll() cancel 全部。
     */
    private final Map<String, DownloadTask> handles = new ConcurrentHashMap<>();
    /**
     * 正在写的 stage key（= sha256(url)）集合。断点续传把 {@code .part} 按 url 命名，
     * url-key 不像 uuid 那样天然唯一（同一张图可能被重复入队），必须挡住两个传输
     * 同时 append 同一个 {@code .part}。{@code startDownloadChain} 抢占，onFinally 释放。
     */
    private final java.util.Set<String> activeStageKeys = ConcurrentHashMap.newKeySet();
    /**
     * "已经置 DOWNLOADING、但 [handles] 还没登记"的 uuid 集合。
     *
     * {@link #pumpAvailableSlots} 里 {@code setState(DOWNLOADING)} 是同步的，而句柄要绕
     * IO 线程 + 主线程一跳才落到 [handles]（实测 19–161ms）。这段窗口里
     * {@link #resurrectIfStranded} 只看 [handles] 会把正在派发的 item 误判成 stranded
     * → 翻 INIT → 再派发一次 → 同 uuid 两条传输抢同一个 stage 文件 / targetUri。
     *
     * 生命周期：
     *   - {@link #pumpAvailableSlots} 置 DOWNLOADING 时 add；
     *   - 句柄登记（{@link #registerHandle}）时 remove —— 窗口到此为止；
     *   - 任务终结（{@link #complete}）时 remove —— 覆盖"派发失败、根本没建句柄"的路径；
     *   - 暂停不清除：等待准备阶段退出或登记句柄，防止快速继续重复派发。
     */
    private final java.util.Set<String> dispatching = ConcurrentHashMap.newKeySet();
    private boolean isRunning = false;
    /**
     * 全局泵关着（[isRunning] == false）时，用户在详情页 FAB 点名放行的作品 id（{@link #startIllust}）。
     *
     * 泵关着 = 「仅通过 Wi-Fi 下载」在蜂窝上（入列未自动启动 / {@link #parkForNetwork} 熄火）、
     * 模式 2、冷启动恢复：content 里其它作品的 INIT 项也都没暂停，只是没人 pump。这时若直接
     * {@code isRunning = true; pumpAvailableSlots()}，getFirstReady 会按 content 顺序先挑走
     * **别的**等待项，且每条完成后的 onFinally 再 pump 一轮 —— 整队都在蜂窝上跑起来。
     * 放进这里的作品单独享有派发资格，跑完（没有待派发 / 在传的页）由 pump 收回。
     */
    private final java.util.Set<Long> releasedIllusts = ConcurrentHashMap.newKeySet();
    private final java.util.Set<String> releasedPages = ConcurrentHashMap.newKeySet();

    /** 登记传输句柄，同时把 uuid 从 [dispatching] 摘掉（"正在派发"窗口到此为止）。 */
    private synchronized void registerHandle(String uuid, DownloadTask d) {
        handles.put(uuid, d);
        dispatching.remove(uuid);
        boolean allowed = false;
        for (DownloadItem item : content) {
            if (uuid.equals(item.getUuid())) { allowed = canContinueAttempt(item); break; }
        }
        if (!allowed) d.cancel();
    }

    private boolean canContinueAttempt(DownloadItem item) {
        return content.contains(item) && !item.isPaused()
                && (isRunning || releasedPages.contains(item.getUuid())
                    || releasedIllusts.contains(item.getIllust().getId()));
    }

    /** Called after the worker and its storage cleanup have stopped touching resources. */
    private void workerTerminated(DownloadItem item) {
        final ceui.pixiv.download.queue.ContentQueueStore.Task lease;
        final boolean committed, stopped;
        synchronized (this) {
            DownloadTask ended = handles.remove(item.getUuid());
            lease = leases.get(item.getUuid());
            if (lease == null) {
                if (ended != null && ended.isCancelled() && !item.isSuccess() && !item.isFailed()) {
                    item.setState(DownloadItem.DownloadState.INIT);
                }
                dispatching.remove(item.getUuid());
                postMain(this::pumpAvailableSlots);
                return;
            }
            if (!finalizing.add(item.getUuid())) return;
            dispatching.add(item.getUuid()); // cleanup ended; durable acknowledgement still owns capacity
            committed = item.isSuccess();
            stopped = !committed && !item.isFailed();
        }
        queueWrites.execute(() -> {
            try {
                if (!pageQueue.finish(AppDatabase.getAppDatabase(mContext), Shaft.sGson,
                        item, lease, committed, stopped)) {
                    throw new IllegalStateException("Stale page worker acknowledgement");
                }
                synchronized (this) {
                    leases.remove(item.getUuid(), lease);
                    dispatching.remove(item.getUuid());
                    finalizing.remove(item.getUuid());
                    blockedClaims.clear();
                    if (stopped && !item.isFailed() && !item.isSuccess()) item.setState(DownloadItem.DownloadState.INIT);
                }
                PageQueueDiagnostics.event("worker-acknowledged", item);
                ManagerReactive.invalidate();
                postMain(this::pumpAvailableSlots);
            } catch (Throwable t) {
                // Keep the reservation if acknowledgement fails. Restart recovery is safe after worker exit.
                reportPersistenceError("worker acknowledgement", t);
            }
        });
    }

    /**
     * 下载专用 OkHttpClient —— **强制 HTTP/1.1**。
     *
     * 全局 {@code Shaft.getOkHttpClient()} 在非直连模式下默认 [H2, H1.1]，pixiv CDN
     * 选 H2 后所有并发下载流复用一条 TCP 连接 + H2 多路复用，**服务器端 stream
     * priority 严格串行**：stream 1 的 body 推完才发 stream 2/3 的 response headers，
     * `client.newCall(req).execute()` 在 2/3 上分别卡到 600ms+ / 几秒（实测 4.4s），
     * 用户视角"3 个 item 都标 DOWNLOADING 但只有第一个进度在跳"。
     *
     * 改用 H1.1：每个 sync 请求拿独立 TCP 连接（OkHttp ConnectionPool 默认
     * maxIdleConnections=5 够 5 并发用），各自独立 flow control，真正并行。
     *
     * `newBuilder()` 继承全局 client 的 DNS / SSL 等配置，移除图片显示用的 URL 进度
     * 拦截器：Manager 自己在 pumpBytes 上报进度，不能再把独立请求的进度混进显示任务。
     */
    private volatile OkHttpClient mDownloadOkHttpClient;
    private OkHttpClient getDownloadOkHttpClient() {
        OkHttpClient cached = mDownloadOkHttpClient;
        if (cached != null) return cached;
        synchronized (this) {
            if (mDownloadOkHttpClient == null) {
                OkHttpClient base = ((Shaft) Shaft.getContext()).getOkHttpClient();
                mDownloadOkHttpClient = buildDownloadOkHttpClient(
                        base, ((Shaft) Shaft.getContext()).getDownloadProgress());
            }
            return mDownloadOkHttpClient;
        }
    }

    /**
     * 下载 client 的读超时（毫秒）—— 断流文案「已断流N/10s」的分母。
     *
     * 读的是**下载这条 client**（由全局 client `newBuilder()` 派生，超时同源）：非直连 =
     * OkHttp 默认 10s，直连 = {@code Shaft.buildOkHttpClient} 显式设的 30s。不硬编码。
     *
     * 收在这里读的原因：OkHttp 的 {@code readTimeoutMillis} 是 Kotlin 属性、JVM 访问器是**方法**、
     * 字段本身 private —— Java 侧只能写 {@code readTimeoutMillis()}。UI 层不必碰这个 API。
     */
    @SuppressWarnings("deprecation")
    public long getDownloadReadTimeoutMillis() {
        return getDownloadOkHttpClient().readTimeoutMillis();
    }

    /**
     * 这次失败该不该「读超时静默重连一次」。
     *
     * 三条同时满足才重连：
     *   1. 原因是**读超时**（对端沉默），不是别的错 —— 见
     *      {@code NetworkFailureClassifier.isReadTimeoutFailure}；
     *   2. 用户把阈值**调小过**（当前值 ≠ 该模式默认值）。停在默认时读超时照旧算失败、行为与历史
     *      一致；只有主动调小的人是在用「更早断流」换「更快重连」，这一下读超时对他才是预期内的；
     *   3. 这条 item 还没用过这次额度（每条只给一次，避免对端真挂了时无限重连）。
     *
     * 副作用：判定通过时会把额度标记为已用，所以调用点只需看返回值。
     */
    private boolean shouldSilentlyRetryAfterReadTimeout(DownloadItem item, Throwable error) {
        if (item.isReadTimeoutRetryUsed()) return false;
        if (!ceui.lisa.http.NetworkFailureClassifierKt.isReadTimeoutFailure(error)) return false;
        if (Shaft.sSettings == null || !Shaft.sSettings.isImageReadTimeoutLowered()) return false;
        item.setReadTimeoutRetryUsed(true);
        return true;
    }

    static OkHttpClient buildDownloadOkHttpClient(OkHttpClient base, ProgressTracker displayProgress) {
        OkHttpClient.Builder builder = base.newBuilder()
                .protocols(java.util.Collections.singletonList(okhttp3.Protocol.HTTP_1_1));
        builder.networkInterceptors().remove(displayProgress.getInterceptor());
        return builder.build();
    }

    private Manager() {
        uuid = "";
        currentIllustID = 0;
        IO.execute(() -> PageQueueDiagnostics.previousExits(mContext));
    }

    /**
     * 后台分批读取全部未完成任务，不裁剪持久化队列。Cursor 每次只取一条 JSON，
     * 同作品多 P 共用一个 Illust，避免整库 JSON 和重复作品对象同时驻留内存。
     */
    private static final int RESTORE_BATCH_SIZE = 20;
    private boolean restoreStarted;
    private int restoreGeneration;
    // Guarded by this. Keep output identities even if live tasks finish/get deleted during recovery.
    private java.util.Set<String> restoreLiveKeys;

    // 字节级限速已彻底删除（debug 调试可视化的 500KB/s 节流）。
    // 唯一保留的是 BulkObjectFetcher.RATE_LIMIT_MS（页间 API 间隔，防 pixiv 429）。

    public synchronized void restore() {
        if (restoreStarted) return;
        restoreStarted = true;
        final int generation = restoreGeneration;
        restoreLiveKeys = content.stream().map(PageQueuePersistence::key).collect(Collectors.toSet());
        // Serialize legacy journal writes with enqueue/clear, then read the snapshot on independent IO.
        queueWrites.execute(() -> {
            try {
                ceui.pixiv.download.LegacyPageSaveMigration.migrate(mContext,
                        AppDatabase.getAppDatabase(mContext), Shaft.sGson);
            } catch (Exception migrationError) {
                reportPersistenceError("legacy-import", migrationError);
            }
            IO.execute(() -> {
                boolean failed = false;
                try {
                    AppDatabase database = AppDatabase.getAppDatabase(mContext);
                    List<DownloadItem> restored = pageQueue.restore(database, Shaft.sGson);
                    if (restored.isEmpty()) return;
                    synchronized (this) {
                        if (generation != restoreGeneration) return; // User cleared the queue.
                        restored.removeIf(item -> restoreLiveKeys.contains(PageQueuePersistence.key(item)));
                        if (DownloadLimitTypeUtil.enqueueAsPaused()) {
                            // 「不自动下载」恢复出来的项同样没有自动唤醒源，与入列口径一致地呈现为
                            // 用户暂停；仅 Wi-Fi 的网络等待仍是 INIT（回 Wi-Fi 会唤醒）。
                            for (DownloadItem item : restored) {
                                item.setPaused(true);
                            }
                        }
                        restored.addAll(content); // Preserve work added while recovery was running.
                        content = new CopyOnWriteArrayList<>(restored);
                    }
                    ManagerReactive.invalidate();
                    postMain(() ->
                            DownloadToasts.show(DownloadToastKind.RECORD_RESTORED, "下载记录恢复成功"));
                } catch (Throwable t) {
                    failed = true;
                    Common.showLog("Manager restore failed: " + t.getMessage());
                } finally {
                    synchronized (this) {
                        if (generation == restoreGeneration) {
                            restoreLiveKeys = null;
                            if (failed) restoreStarted = false;
                        }
                    }
                }
            });
        });
    }

    static Gson restoringGson(Gson gson) {
        // Match the complete metadata, not just the ID: an artwork may have been edited
        // between two downloads, with different pages or naming-template inputs.
        // Retain a compact digest rather than every artwork's additional JSON object tree.
        Map<ByteString, Illust> artworks = new HashMap<>();
        return gson.newBuilder().registerTypeAdapter(Illust.class,
                (JsonDeserializer<Illust>) (json, type, context) ->
                        artworks.computeIfAbsent(ByteString.encodeUtf8(json.toString()).sha256(),
                                key -> gson.fromJson(json, Illust.class))).create();
    }

    static List<DownloadItem> readRestoredDownloads(DownloadDao dao, Gson gson) {
        Gson restoringGson = restoringGson(gson);
        List<DownloadItem> restored = new ArrayList<>();
        long after = 0;
        long through = dao.getDownloadingHighWaterMark();
        while (true) {
            try (Cursor cursor = dao.getDownloadingBatch(after, through, RESTORE_BATCH_SIZE)) {
                if (!cursor.moveToFirst()) return restored;
                do {
                    after = cursor.getLong(0);
                    try {
                        String originalJson = cursor.getString(1);
                        DownloadItem item = restoringGson.fromJson(originalJson, DownloadItem.class);
                        if (item != null && item.getIllust() != null && item.getUrl() != null
                                && !item.getUrl().isEmpty()) {
                            PageQueuePersistence.restoreIdentity(dao, gson, item,
                                    cursor.getString(2), cursor.getString(3), originalJson);
                            // Unversioned records keep the historical normalization policy.
                            // Versioned records contain explicit user commands, not transient worker state.
                            if (item.getQueueRecordVersion() == 0) {
                                item.setState(DownloadItem.DownloadState.INIT);
                                item.setPaused(false);
                            } else if (!item.isFailed()) {
                                item.setState(DownloadItem.DownloadState.INIT);
                            }
                            restored.add(item);
                        }
                    } catch (Exception e) {
                        Common.showLog("Manager restore parse error: " + e.getMessage());
                    }
                } while (cursor.moveToNext());
            }
        }
    }

    public static Manager get() {
        return Manager.SingletonHolder.INSTANCE;
    }

    /** GA4 下载失败埋点（按页、带进程级配额，见 Analytics）。 */
    private static ceui.pixiv.analytics.Analytics analytics() {
        return ceui.pixiv.services.ServiceProviderKt.appServices(Shaft.getContext()).getAnalytics();
    }

    private static class SingletonHolder {
        private static final Manager INSTANCE = new Manager();
    }

    /**
     * 直接下载入列（详情页下载按钮 / 收藏后自动下载 / 单页下载）与批量队列补页共用入口。
     *
     * 是否立即开始由 {@link DownloadLimitTypeUtil#startTaskWhenCreate()} 决定；
     * 「不自动下载」时入列即置暂停态（{@link DownloadLimitTypeUtil#enqueueAsPaused()}）——
     * 等待态只属于「仅通过 Wi-Fi 下载」的网络暂缓。批量队列补页（silent）刻意不套这条：
     * 补页只发生在用户点了队列的「继续 / 重试失败」之后，那次点击本身就是手动启动；若这批页
     * 被置 paused，getFirstReady 永远挑不到它们 → 90s 停滞检测 → clearOne → 重拉，假失败循环。
     */
    public void addTask(DownloadItem bean) {
        addTask(bean, null);
    }

    /** onCommitted runs on main only after durable admission, never on persistence failure. */
    public void addTask(DownloadItem bean, Runnable onCommitted) {
        // 纵深防御:null item 会在 safeAdd 里 null.getUuid() NPE(见 gif 走 buildDownloadItem
        // 返 null 的历史坑)。调用方本应先过滤,这里再兜一层,任何 null 直接忽略不崩。
        if (bean == null) {
            return;
        }
        synchronized (this) {
            if (content == null) {
                content = new CopyOnWriteArrayList<>();
            }

            boolean isTaskExist = false;
            DownloadItem canonical = bean;
            for (DownloadItem item : content) {
                if (PageQueuePersistence.key(item).equals(PageQueuePersistence.key(bean))) {
                    isTaskExist = true;
                    canonical = item;
                }
            }

            if (!isTaskExist) {
                if (!bean.isSilent() && DownloadLimitTypeUtil.enqueueAsPaused()) {
                    bean.setPaused(true);
                }
                // Reserve in memory for the bulk producer; pendingPersistence prevents execution
                // until the background transaction commits successfully.
                safeAdd(bean);
            }
            notifyAdmissions(java.util.Collections.singletonList(canonical), onCommitted);
            if(DownloadLimitTypeUtil.startTaskWhenCreate()){
                // 见 triggerPump 的 javadoc —— addTask 是连续调用 hot path
                // （fillSlots 一轮要 addTask N 次），绝不能走 startAll。
                triggerPump();
            }
        }
        ManagerReactive.invalidate();
    }

    /**
     * 给 [ManagerReactive.contentFlow] 用：返回 [content] 当前的浅拷贝快照。
     * synchronized 跟 addTask / safeAdd / clearAll 等写入路径互斥，避免拷贝
     * 时撞上 ConcurrentModificationException。
     */
    public List<DownloadItem> contentSnapshot() {
        synchronized (this) {
            return new ArrayList<>(content);
        }
    }

    /** Called under this monitor. Memory reservation is not execution authorization. */
    private void safeAdd(DownloadItem item) {
        content.add(item);
        PageQueueDiagnostics.event("submit", item);
        if (restoreLiveKeys != null) restoreLiveKeys.add(PageQueuePersistence.key(item));
        scheduleAdmission(item);
    }

    private void scheduleAdmission(DownloadItem item) {
        final Object ticket = new Object();
        if (pendingPersistence.putIfAbsent(item.getUuid(), ticket) != null) return;
        queueWrites.execute(() -> {
            try {
                synchronized (this) {
                    if (pendingPersistence.get(item.getUuid()) != ticket || !content.contains(item)) return;
                }
                pageQueue.admit(AppDatabase.getAppDatabase(mContext), Shaft.sGson, item);
                PageQueueDiagnostics.event("admitted", item);
                synchronized (this) {
                    if (pendingPersistence.get(item.getUuid()) == ticket) persistenceFailures.remove(item.getUuid());
                }
            } catch (Throwable t) {
                synchronized (this) {
                    if (pendingPersistence.get(item.getUuid()) == ticket && content.contains(item)) {
                        // Make retry eligible atomically with publishing failure, not a later finally.
                        pendingPersistence.remove(item.getUuid(), ticket);
                        persistenceFailures.add(item.getUuid());
                        item.setState(DownloadItem.DownloadState.FAILED);
                    }
                }
                reportPersistenceError("enqueue", t);
            } finally {
                pendingPersistence.remove(item.getUuid(), ticket);
                ManagerReactive.invalidate();
                postMain(() -> {
                    if (DownloadLimitTypeUtil.startTaskWhenCreate()) triggerPump();
                    else pumpAvailableSlots(); // a manual request may have arrived while persisting
                });
            }
        });
    }

    private void retryAdmissionIfNeeded(DownloadItem item) {
        if (persistenceFailures.contains(item.getUuid())) scheduleAdmission(item);
    }

    private void notifyAdmissions(List<DownloadItem> items, Runnable onCommitted) {
        if (onCommitted == null || items.isEmpty()) return;
        queueWrites.execute(() -> {
            synchronized (this) {
                for (DownloadItem item : items) {
                    if (!content.contains(item) || pendingPersistence.containsKey(item.getUuid())
                            || persistenceFailures.contains(item.getUuid())) return;
                }
            }
            postMain(onCommitted);
        });
    }

    private void persistState(DownloadItem item) {
        final boolean paused = item.isPaused();
        final int state = item.getState();
        queueWrites.execute(() -> {
            try {
                pageQueue.update(AppDatabase.getAppDatabase(mContext), Shaft.sGson, item, paused, state);
                blockedClaims.remove(item.getUuid());
                postMain(this::pumpAvailableSlots);
            } catch (Throwable t) { reportPersistenceError("state", t); }
        });
    }

    private void deletePersisted(DownloadItem item) {
        queueWrites.execute(() -> {
            try {
                pageQueue.cancel(AppDatabase.getAppDatabase(mContext), item);
            } catch (Throwable t) { reportPersistenceError("remove", t); }
        });
    }

    private void reportPersistenceError(String operation, Throwable error) {
        Timber.tag("ContentQueue").e(error, "Page queue persistence failed: %s", operation);
        postMain(() -> DownloadToasts.show(DownloadToastKind.DOWNLOAD_FAILED,
                mContext.getString(R.string.save_image_failed, String.valueOf(error.getMessage()))));
    }

    /**
     * 标记任务完成。可在任意线程调用。
     * 注意：content 列表修改不在此方法中，由调用方在主线程统一处理。
     */
    private void complete(DownloadItem item, boolean isDownloadSuccess) {
        synchronized (this) {
            if (!leases.containsKey(item.getUuid())) dispatching.remove(item.getUuid());
            releasedPages.remove(item.getUuid());
            if (isDownloadSuccess) {
                item.setState(DownloadItem.DownloadState.SUCCESS);
                setCallback(uuid, null);
            } else {
                item.setNonius(0);
                item.setState(DownloadItem.DownloadState.FAILED);
                persistState(item);
            }
        }
        if (isDownloadSuccess) {
            queueWrites.execute(() -> {
                try {
                    pageQueue.committed(leases.get(item.getUuid()));
                    AppDatabase.getAppDatabase(mContext).downloadDao()
                            .deleteDownloadingByIdentity(item.getName(), item.getUuid());
                } catch (Throwable t) { reportPersistenceError("complete", t); }
            });
        }
        // Preparation-only exits have no DownloadTask.onFinally callback.
        if (!handles.containsKey(item.getUuid())) workerTerminated(item);
        ManagerReactive.invalidate();
    }

    public void addTasks(List<DownloadItem> list) {
        addTasks(list, null);
    }

    public void addTasks(List<DownloadItem> list, Runnable onCommitted) {
        if (Common.isEmpty(list)) return;
        final List<DownloadItem> submitted = new ArrayList<>(list);
        final int generation;
        synchronized (this) { generation = restoreGeneration; }
        IO.execute(() -> {
            synchronized (this) {
                if (generation != restoreGeneration) return; // cleared before the producer ran
                Map<String, DownloadItem> existing = new HashMap<>();
                for (DownloadItem item : content) existing.put(PageQueuePersistence.key(item), item);
                List<DownloadItem> admitted = new ArrayList<>();
                for (DownloadItem item : submitted) {
                    if (item == null) continue;
                    DownloadItem canonical = existing.get(PageQueuePersistence.key(item));
                    if (canonical == null) {
                        if (!item.isSilent() && DownloadLimitTypeUtil.enqueueAsPaused()) item.setPaused(true);
                        safeAdd(item); // same durable admission gate as single-page saves
                        existing.put(PageQueuePersistence.key(item), item);
                        canonical = item;
                    }
                    admitted.add(canonical);
                }
                notifyAdmissions(admitted, onCommitted);
            }
            ManagerReactive.invalidate();
        });
    }

    public synchronized void startAll() {
        Common.showLog("[DL-RACE] startAll (用户态 resume / 冷启动残留路径，走 resurrect)");
        // 用 snapshot 迭代：item.setPaused/state 是各自字段写入,跟 content 结构无关;
        // 直接 for-each live content 跟并发的 addTasks/safeAdd/remove 抢 modCount,CME。
        for (DownloadItem item : contentSnapshot()) {
            if (item != null && !item.isFailed()) {
                item.setPaused(false);
                resurrectIfStranded(item);
                retryAdmissionIfNeeded(item);
                persistState(item);
            }
        }
        isRunning = true;
        // 之前 isRunning=true 时会 short-circuit return，假设单线程串行用 onFinally
        // 自动驱动下一条；并发模式下需要每次都 pumpAvailableSlots() 来填满空闲槽位。
        pumpAvailableSlots();
        ManagerReactive.invalidate();
    }

    /**
     * 内部 dispatch trigger —— 等价于 {@code isRunning = true; pumpAvailableSlots();}，
     * 但 **不走 startAll 的 [resurrectIfStranded] 循环**。
     *
     * 用于"刚 addTask 一批 / fillSlots 末尾想 trigger pump"等场景：这时 content 里
     * 可能有刚 [pumpAvailableSlots] 抢占式 setState(DOWNLOADING) 但 [startDownloadChain]
     * 里 [handles].put(uuid) 还在 IO → 主线程 异步排队中的 in-flight item。
     * 走 startAll 会让 resurrectIfStranded 看到"DOWNLOADING && !handles.containsKey"
     * 误判 stranded → setState(INIT) + setNonius(0) → pump 又把它当 INIT 挑出来再
     * dispatch 一次，导致：
     *   - 同 uuid 两条传输任务抢同一个 stage 文件 / targetUri（实测出过两次 read-start）
     *   - UI 进度一会儿前进一会儿回到 0% 闪烁
     *
     * 用户态 "全部继续" 按钮 / 冷启动 restore 后的残留清理仍然走 [startAll]——
     * 那些场景 handles 是干净的（stopAll dispose 完或进程刚启动）不会撞到 race。
     */
    public synchronized void triggerPump() {
        Common.showLog("[DL-RACE] triggerPump (addTask / QueueDownloadManager.didAdd 路径，无 resurrect)");
        isRunning = true;
        pumpAvailableSlots();
    }

    /** Release only the selected page; all starts pass through the bounded pump. */
    public synchronized void startOne(String uuid) {
        for (DownloadItem item : content) {
            if (uuid.equals(item.getUuid())) {
                item.setPaused(false);
                resurrectIfStranded(item);
                retryAdmissionIfNeeded(item);
                persistState(item);
                releasedPages.add(uuid);
                pumpAvailableSlots();
                ManagerReactive.invalidate();
                return;
            }
        }
        ManagerReactive.invalidate();
    }

    /**
     * 单卡「开始」要不要顺带让全局泵填满其它等待项。纯函数，便于单测钉住口径。
     *
     * <p>只有全局泵本来就开着才顺带：模式 0 / 模式 1 且在 Wi-Fi 时，自动路径本来就该把
     * 并发槽位填满，单卡开始只是其中一次触发。泵关着（仅 Wi-Fi 在蜂窝上被
     * {@link #parkForNetwork} 熄火、冷启动恢复、模式 2 入列）时返回 false —— 否则
     * 「只想先下一个」会变成「整队都下」。
     */
    static boolean fanOutOthersOnStartOne(boolean pumpRunning) {
        return pumpRunning;
    }

    /**
     * 恢复/启动指定作品的所有未完成下载条目（用于详情页 FAB 从暂停态就地继续）。
     *
     * @return 队列里是否有该作品的条目；false = 还没入列（异步入列尚未落地），什么都没做。
     */
    public synchronized boolean startIllust(long illustId) {
        boolean found = false;
        for (DownloadItem downloadItem : contentSnapshot()) {
            if (downloadItem != null && downloadItem.getIllust() != null && downloadItem.getIllust().getId() == illustId) {
                downloadItem.setPaused(false);
                resurrectIfStranded(downloadItem);
                retryAdmissionIfNeeded(downloadItem);
                persistState(downloadItem);
                found = true;
            }
        }

        if (found) {
            Common.showLog("已开始作品 " + illustId);
            synchronized (this) {
                // 泵开着（模式 0 / Wi-Fi 下）照常排队填槽；泵关着只放行这一个作品，见 [releasedIllusts]。
                if (!isRunning) releasedIllusts.add(illustId);
                pumpAvailableSlots();
            }
            ManagerReactive.invalidate();
        }
        return found;
    }

    /**
     * 该作品未暂停的等待页会不会被派发出去：全局泵开着，或用户在详情页单独放行过它
     * （[releasedIllusts]）。false = 只能等用户点继续（或回到 Wi-Fi 自动接续）。
     * 详情页 FAB 据此区分「排队中」和「等你点继续」。
     */
    public synchronized boolean willDispatch(long illustId) {
        return isRunning || releasedIllusts.contains(illustId);
    }

    /** Includes manual page requests waiting for a slot, without releasing sibling pages. */
    public synchronized boolean willDispatchPage(long illustId, int page) {
        if (willDispatch(illustId)) return true;
        for (DownloadItem item : content) {
            if (item.getIllust() != null && item.getIllust().getId() == illustId
                    && item.getIndex() == page && !item.isPaused()
                    && releasedPages.contains(item.getUuid())) return true;
        }
        return false;
    }

    /**
     * 把"看似在跑但实际已经没有 disposable 在背后撑着"的 item 翻回 INIT，让
     * [pumpAvailableSlots] 重新挑选派发。两种来源：
     *
     * 暂停后的 worker/准备阶段仍保留名额。执行器确认退出时将取消项归为 INIT；
     * 此处只修复确实没有句柄和准备阶段的 DOWNLOADING 残留，不提前重派仍在退出的任务。
     *
     * 冷启动 restore 带回的残留**不走这里**：readRestoredDownloads 已经把恢复出来的
     * item 状态统一归成 INIT（见那里的注释），恢复完就能直接被 pump 挑走，不需要先猜
     * 哪些"看着在跑、其实没有句柄"。
     *
     * FAILED 也一起翻 INIT，让 retry 自然走 pump 路径。
     *
     * 两类 item 绝不能动：
     *   - 真在跑的 page（[handles] 里有 uuid）；
     *   - 正在派发的 page（[dispatching] 里有 uuid）—— 它刚被置 DOWNLOADING，句柄还在
     *     IO → 主线程的路上；这时翻 INIT 会让 pump 再派发一次，同 uuid 两条传输抢同一个
     *     stage 文件 / targetUri（实测出过同 uuid 两次 read-start）。
     */
    private void resurrectIfStranded(DownloadItem item) {
        int s = item.getState();
        if (s == DownloadItem.DownloadState.FAILED) {
            // FAILED→INIT 是预期 retry 路径，常态会触发；DEBUG 级别看就行
            Common.showLog("[DL-RACE] resurrect FAILED→INIT uuid=" + item.getUuid()
                    + " nonius=" + item.getNonius());
            item.setState(DownloadItem.DownloadState.INIT);
            // 用户手动重试 = 一次全新的尝试：把「读超时静默重连」的额度也还给他。否则第二次重试
            // 再撞上读超时就直接失败了 —— 那条额度在上一次失败时已经被用掉。
            item.setReadTimeoutRetryUsed(false);
            return;
        }
        if (s == DownloadItem.DownloadState.DOWNLOADING
                && !handles.containsKey(item.getUuid())
                && !dispatching.contains(item.getUuid())) {
            // 只翻状态。**不再** setNonius(0) / setCurrentSize(0) —— 来这里的两种场景：
            //   - 用户 pause → resume：stage 文件仍在 cacheDir，下次 downloadOne 用
            //     stageFile.length() 做 Range 头续传，第一个 chunk 进度直接回到原值
            //     （比如 47%）。归零会让 UI 看到一瞬 "47% → 0% → 47%" 回弹（state
            //     翻 INIT 时 progress bar GONE 不可见，但 pumpAvailableSlots 立刻翻回
            //     DOWNLOADING，那一帧暴露的就是被归零的脏值，第一个 chunk callback
            //     才会更新成真实续传位置）。
            //   - 冷启动 restore 后 stranded：同理，stage 文件跨进程仍在。
            // 481b06e0 当初归零是想"UI 不留进度残影"，但 ActiveListV3Fragment 在 INIT
            // 状态 progress.visibility = GONE，根本不会显示残影；归零反而把脏值
            // 暴露在 re-dispatch 后那一帧。
            Common.showLog("[DL-RACE] resurrect DOWNLOADING→INIT uuid=" + item.getUuid()
                    + " nonius=" + item.getNonius() + " currentSize=" + item.getCurrentSize()
                    + " (preserving progress for stage-resume)");
            item.setState(DownloadItem.DownloadState.INIT);
        }
    }

    public synchronized void stopAll() {
        stopAll(true);
    }

    private void stopAll(boolean recordState) {
        for (DownloadItem item : contentSnapshot()) {
            if (item != null && !item.isFailed()) {
                item.setPaused(true);
                if (recordState) persistState(item);
            }
        }
        isRunning = false;
        releasedIllusts.clear();
        releasedPages.clear();
        // cancel 全部正在传输的下载（snapshot 防 CME）
        for (DownloadTask d : new ArrayList<>(handles.values())) {
            try { d.cancel(); } catch (Exception ignored) {}
        }
        // Stopping workers and preparation still own their slots until termination acknowledgement.
        Common.showLog("[DL-RACE] stopAll (已经停止)");
        ManagerReactive.invalidate();
    }

    /**
     * 只对 FAILED 态的任务下发启动：把 FAILED 翻回 INIT，并触发 pumpAvailableSlots。
     * 其余非 FAILED 态（DOWNLOADING、INIT、PAUSED）的任务一律不碰。
     */
    public synchronized void retryAllFailed() {
        Common.showLog("[DL-RACE] retryAllFailed (只对 FAILED 态下发启动)");
        for (DownloadItem item : contentSnapshot()) {
            if (item != null && item.isFailed()) {
                item.setPaused(false);
                resurrectIfStranded(item);
                retryAdmissionIfNeeded(item);
                persistState(item);
            }
        }
        isRunning = true;
        pumpAvailableSlots();
        ManagerReactive.invalidate();
    }

    /**
     * 网络不可用时的「挂起」：把下载退回**等待态**，等网络恢复自动接续。
     *
     * 与 [stopAll] 的差别是语义，不是实现细节：
     *   - [stopAll] 是**用户暂停** —— 置 paused=true，之后必须用户手动「继续」；
     *   - 这里是**网络等待** —— 只取消在传的传输、把状态翻回 INIT，**不碰 paused**。
     *
     * 用于「仅通过 Wi-Fi 下载」离开 Wi-Fi 时（见 NetWorkStateReceiver）：用户设的是
     * 「只在 Wi-Fi 下下」，不是「暂停」。网络回来时 NetWorkStateReceiver 的
     * [triggerPump] 会把这些等待项自动接续 —— stage 文件仍在 cacheDir，走 Range 续传，
     * 不会从头重下（与 [resurrectIfStranded] 的续传口径一致，故也不 setNonius(0)）。
     *
     * 用户手动暂停过的项不动：paused=true 时 [DownloadItem.getState] 返回 PAUSED，
     * 天然被下面的判断跳过。
     *
     * 别把「不自动下载」也塞进这条路：它没有任何自动唤醒源（回到 Wi-Fi 也不会替它启动），
     * 入列时已按暂停态呈现（见 [DownloadLimitTypeUtil.enqueueAsPaused]），与本方法无关。
     */
    public synchronized void parkForNetwork() {
        // 先熄火再取消：cancel 触发的 onFinally 会调 pumpAvailableSlots，
        // isRunning=false 让那一轮直接 return，避免又派发出去。
        // 详情页单独放行的作品也一并收回：离开 Wi-Fi 后同样退回等待，要走流量需用户再点一次。
        isRunning = false;
        releasedIllusts.clear();
        releasedPages.clear();
        for (DownloadTask d : new ArrayList<>(handles.values())) {
            try { d.cancel(); } catch (Exception ignored) {}
        }
        for (DownloadItem item : contentSnapshot()) {
            int s = item.getState();
            if (s == DownloadItem.DownloadState.DOWNLOADING || s == DownloadItem.DownloadState.INIT) {
                item.setState(DownloadItem.DownloadState.INIT);
                persistState(item);
                queueWrites.execute(() -> pageQueue.park(item));
            }
        }
        Common.showLog("[DL-NET] parkForNetwork: 下载退回等待态，等网络恢复后自动接续");
        ManagerReactive.invalidate();
    }

    public synchronized void stopOne(String uuid){
        stopOne(uuid, true);
    }

    private void stopOne(String uuid, boolean recordState) {
        releasedPages.remove(uuid);
        for (DownloadItem item : contentSnapshot()) {
            if(item.getUuid().equals(uuid)){
                item.setPaused(true);
                if (recordState) persistState(item);
                Common.showLog("已暂停 " + uuid);
                break;
            }
        }
        DownloadTask d = handles.get(uuid);
        if (d != null) {
            try { d.cancel(); } catch (Exception ignored) {}
        }
        ManagerReactive.invalidate();
    }

    public synchronized void clearAll() {
        stopAll(false);
        restoreGeneration++;
        restoreLiveKeys = null;
        content.clear();
        pendingPersistence.clear();
        persistenceFailures.clear();
        queueWrites.execute(() -> {
            try { pageQueue.clear(AppDatabase.getAppDatabase(mContext), Shaft.sGson); }
            catch (Throwable t) { reportPersistenceError("clear", t); }
        });
        ManagerReactive.invalidate();
    }

    public synchronized void clearOne(String uuid) {
        stopOne(uuid, false);
        DownloadItem downloadItem;
        synchronized (this) {
            DownloadItem found = null;
            for (DownloadItem it : content) {
                if (it != null && uuid.equals(it.getUuid())) {
                    found = it;
                    break;
                }
            }
            if (found == null) return;
            content.remove(found);
            pendingPersistence.remove(uuid);
            persistenceFailures.remove(uuid);
            downloadItem = found;
        }
        deletePersisted(downloadItem);
        ManagerReactive.invalidate();
    }

    /**
     * 每次有任务变化（startAll / 一条传输完成 / 用户改并发数）都调一次：把 INIT 队首
     * 拉出来，状态置 DOWNLOADING 后异步派发，直到正在传输的数量达到用户设置的
     * 并发数上限或没有 INIT 可用为止。
     *
     * synchronized 关键：state 检查 + 状态置 DOWNLOADING + 登记 [dispatching] 必须原子，
     * 否则两个 onFinally 同时回调可能挑到同一条 INIT 派发两次。
     *
     * public：用户改并发设置时希望"扩大槽位继续跑"但不希望像 startAll 那样
     * 把手动暂停的 item 也强制恢复 —— FragmentSettings 调这个。
     */
    public synchronized void pumpAvailableSlots() {
        if (!isRunning && releasedIllusts.isEmpty() && releasedPages.isEmpty()) return;
        int max = Shaft.sSettings.getMaxConcurrentDownloads();
        if (max < 1) max = 1;
        if (max > 5) max = 5;

        int activeBefore = activeCount();
        int active = activeBefore;
        int dispatched = 0;
        while (active < max) {
            DownloadItem next = getFirstReady();
            if (next == null) break;
            dispatchOne(next);
            active++;
            dispatched++;
        }
        // 让用户能在 logcat 里直接核实"并发数设置真的生效"：
        //   [DL-PARALLEL] pump max=5 activeBefore=0 dispatched=5 activeAfter=5
        // → 一次 pump 起了 5 个并行；max 跟 Settings 里设的数字一致就是真在用。
        Common.showLog("[DL-PARALLEL] pump max=" + max
                + " activeBefore=" + activeBefore
                + " dispatched=" + dispatched
                + " activeAfter=" + active);

        if (active == 0 && getFirstReady() == null && blockedClaims.isEmpty() && pendingPersistence.isEmpty()) {
            // 没活儿了
            isRunning = false;
            Common.showLog("Manager 已经全部下载完成");
        }
        // 单独放行的作品跑完（没有待派发 / 在传的页）就收回放行，下次再入列仍按泵的口径等待。
        if (!releasedIllusts.isEmpty()) {
            releasedIllusts.removeIf(id -> !hasPendingPage(id));
        }
        // dispatched > 0 说明刚刚把若干条 INIT 翻成 DOWNLOADING；invalidate 让
        // UI 立刻看到状态翻转（badge / 进度条）。哪怕 dispatched==0 也无所谓，
        // tryEmit 是 cheap idempotent 操作。
        ManagerReactive.invalidate();
    }

    /**
     * 派发一条 INIT 项：抢占式置 DOWNLOADING + 登记「正在派发」+ 异步起传输。
     *
     * <p><b>调用方必须持 {@code synchronized (this)}</b> —— 「state 检查 + 置 DOWNLOADING +
     * 登记 {@link #dispatching}」必须原子，否则两个 onFinally 同时回调可能挑到同一条 INIT
     * 派发两次（见 {@link #pumpAvailableSlots} 的注释）。
     *
     * <p>{@link #pumpAvailableSlots} 的补槽与 {@link #startOne} 的单条派发共用这一段，
     * 避免两套派发逻辑各自漂移（曾经 pump 里那 4 行就是单卡路径顺带放行整队的那把钥匙）。
     */
    private void dispatchOne(DownloadItem next) {
        // 抢占式置 DOWNLOADING：阻止下一次 pump 再挑到这条；progress callback
        // 进来再细化为带 nonius 的 DOWNLOADING（语义不变，只是同名状态）。
        next.setState(DownloadItem.DownloadState.DOWNLOADING);
        // 同一临界区内标记"正在派发"：句柄登记要绕 IO→主线程一跳才完成，这中间
        // resurrectIfStranded 不能把这条当成 stranded（详见 dispatching 字段注释）。
        dispatching.add(next.getUuid());
        // 兼容字段：第一个进入的当前下载 = 老 API 返回的 currentIllustID/uuid
        currentIllustID = next.getIllust().getId();
        uuid = next.getUuid();
        queueWrites.execute(() -> {
            try {
                synchronized (this) {
                    if (!canContinueAttempt(next)) {
                        dispatching.remove(next.getUuid());
                        next.setState(DownloadItem.DownloadState.INIT);
                        postMain(this::pumpAvailableSlots);
                        return;
                    }
                }
                int capacity = Math.max(1, Math.min(5, Shaft.sSettings.getMaxConcurrentDownloads()));
                ceui.pixiv.download.queue.ContentQueueStore.Task lease = pageQueue.claim(next, capacity);
                if (lease == null) {
                    PageQueueDiagnostics.event("waiting-for-permit", next);
                    synchronized (this) {
                        dispatching.remove(next.getUuid());
                        next.setState(DownloadItem.DownloadState.INIT);
                        blockedClaims.add(next.getUuid());
                    }
                    postMain(this::pumpAvailableSlots);
                    return;
                }
                leases.put(next.getUuid(), lease);
                PageQueueDiagnostics.event("claimed", next);
                postMain(() -> {
                    synchronized (Manager.this) {
                        if (!canContinueAttempt(next)) { workerTerminated(next); return; }
                    }
                    try { workerStarter.accept(next); }
                    catch (Throwable t) { reportPersistenceError("worker start", t); complete(next, false); }
                });
            } catch (Throwable t) {
                synchronized (this) {
                    dispatching.remove(next.getUuid());
                    next.setState(DownloadItem.DownloadState.FAILED);
                    persistenceFailures.add(next.getUuid());
                }
                reportPersistenceError("claim", t);
                postMain(this::pumpAvailableSlots);
            }
        });
    }

    /** 兼容老调用点：等价于 pumpAvailableSlots()。 */
    private void loop() { pumpAvailableSlots(); }

    /** Live workers, including cancellation and output finalization, still occupy capacity. */
    private int activeCount() {
        Set<String> active = new HashSet<>(handles.keySet());
        active.addAll(dispatching);
        for (DownloadItem it : content) {
            if (!it.isPaused() && it.getState() == DownloadItem.DownloadState.DOWNLOADING) active.add(it.getUuid());
        }
        return active.size();
    }

    /** 找出可以 dispatch 的下一条：state=INIT、未暂停，且在派发范围内（见 [isDispatchable]）。 */
    private DownloadItem getFirstReady() {
        for (DownloadItem it : content) {
            if (pendingPersistence.containsKey(it.getUuid()) || persistenceFailures.contains(it.getUuid())
                    || blockedClaims.contains(it.getUuid())) continue;
            if (handles.containsKey(it.getUuid()) || dispatching.contains(it.getUuid())) continue;
            if (isDispatchable(it, isRunning, releasedIllusts)
                    || (releasedPages.contains(it.getUuid()) && !it.isPaused()
                        && it.getState() == DownloadItem.DownloadState.INIT)) return it;
        }
        return null;
    }

    /**
     * 派发范围：泵开着时所有未暂停的 INIT 项；泵关着时只有 [releasedIllusts] 里作品的页。
     * 纯函数，便于单测钉住「详情页继续一个作品不带走整队」的口径。
     */
    static boolean isDispatchable(DownloadItem item, boolean pumpRunning, java.util.Set<Long> released) {
        if (item.isPaused() || item.getState() != DownloadItem.DownloadState.INIT) return false;
        return pumpRunning || (item.getIllust() != null && released.contains(item.getIllust().getId()));
    }

    /** 该作品是否还有待派发或在传的页（暂停的不算）。 */
    private boolean hasPendingPage(long illustId) {
        for (DownloadItem it : content) {
            if (it.isPaused() || it.getIllust() == null || it.getIllust().getId() != illustId) continue;
            int s = it.getState();
            if (s == DownloadItem.DownloadState.INIT || s == DownloadItem.DownloadState.DOWNLOADING) return true;
        }
        return false;
    }

    private void downloadOne(Context context, DownloadItem downloadItem) {
        Common.showLog("[DL-CACHE] downloadOne enter uuid=" + downloadItem.getUuid()
                + " name=" + downloadItem.getName() + " url=" + downloadItem.getUrl());
        // 这里刻意**不做**网络闸门：口径是「闸门只守自动启动」。能走到 downloadOne，
        // 说明上游已经放行（自动路径过了 startTaskWhenCreate，或用户在下载管理里手动继续）。
        // 旧实现在这里 canDownloadNow()==false 就 stopAll() 返回，把用户触发的下载也一并拦掉了 ——
        // 表现为切到蜂窝后「继续全部」也下不动。

        // aria2 远程下载模式（#692）：任务转发给远端 aria2（NAS），不在本地建文件。
        // 必须在 factory 创建之前拦截，否则会平白多出一条本地 MediaStore/SAF 行。
        // 动图（ugoira）不转发 —— zip 是本地播放/合成 GIF 的内部缓存下载，
        // 详见 Aria2Dispatcher.shouldDispatch。
        if (Aria2Dispatcher.shouldDispatch(downloadItem)) {
            dispatchToAria2(downloadItem);
            return;
        }

        // 空间不够就整体暂停，不往快满的盘上开新文件（pixez#1361）；aria2 落在远端，不受影响，
        // 所以放在它之后。这条保持 DOWNLOADING 且没有传输句柄，「全部继续」时由
        // resurrectIfStranded 复位重派。
        if (!StorageSpaceGuard.hasRoomForDownload(context)) {
            downloadItem.setState(DownloadItem.DownloadState.INIT);
            StorageSpaceGuard.pauseDownloadsForLowStorage(context);
            workerTerminated(downloadItem);
            return;
        }

        Common.showLog("Manager 下载单个 当前进度" + downloadItem.getNonius());

        // SAF factory 创建、文件查询、insert 全部在 IO 线程执行，
        // 避免 172P 连续下载时 SAF 操作阻塞主线程。
        IO.execute(() -> {
            final DownloadFileFactory[] preparing = new DownloadFileFactory[1];
            try {
            DownloadFileFactory factory;
            try {
                // SAF 与否只认 V3 下载配置，不读遗留 downloadWay（#984）；
                // 两个 factory 反正都走下载 facade，这里只决定 gif 之外的壳。
                if (!DownloadsRegistry.isSaf() || downloadItem.getIllust().isGif()) {
                    factory = new Android10DownloadFactory22(context, downloadItem);
                } else {
                    factory = new SAFactory(context, downloadItem);
                }
            } catch (Exception e) {
                PageQueueDiagnostics.failure("factory-init", downloadItem, e);
                Common.showLog("[DL] factory init failed: " + e);
                e.printStackTrace();
                postMain(() -> {
                    DownloadToasts.show(DownloadToastKind.STORAGE_UNAVAILABLE,
                            mContext.getString(R.string.string_365));
                    analytics().illustDownloadStorageFailed();
                    complete(downloadItem, false);
                    // 单条失败不再 stopAll —— 并发模式下其它正在传的 page 不应受牵连。
                    pumpAvailableSlots();
                });
                return;
            }

            preparing[0] = factory;
            boolean shouldSkip =
                    (factory instanceof Android10DownloadFactory22 && ((Android10DownloadFactory22) factory).isSkip())
                 || (factory instanceof SAFactory && ((SAFactory) factory).isSkip());

            // facade 的 skip 只按**当前模板**渲染出的路径查文件系统。用户升级前用的是
            // 别的命名规则、或记录是 DownloadImporter 从旧版命名的文件扫进来的
            // (issue #953)，那条路径上什么都没有 —— 明明盘上躺着同一张图还是会重下一份，
            // MediaStore 再给它加个 " (1)"。所以这里再按 (illustId, page) 问一次下载记录。
            // 只在策略确实是「已存在则跳过」时生效；Rename 是用户明确要新文件，
            // Replace 本来就要覆写，都不能被记录短路。gif 走的是 app cache 里的中间 zip，
            // 不参与。整段已经在 IO 线程池上，DB + fd 探测都安全。
            if (!shouldSkip && !downloadItem.getIllust().isGif()) {
                boolean skipPolicy =
                        (factory instanceof Android10DownloadFactory22 && ((Android10DownloadFactory22) factory).isSkipPolicy())
                     || (factory instanceof SAFactory && ((SAFactory) factory).isSkipPolicy());
                if (skipPolicy) {
                    shouldSkip = RecordedPageProbe.hasUsableRecord(
                            context,
                            downloadItem.getIllust().getId(),
                            downloadItem.getIndex());
                    if (shouldSkip) {
                        Common.showLog("[DL] skip by download record, illust="
                                + downloadItem.getIllust().getId() + " page=" + downloadItem.getIndex());
                    }
                }
            }

            if (shouldSkip) {
                Common.showLog("[DL] skip download (already exists), illust=" + downloadItem.getIllust().getId());
                complete(downloadItem, true);
                postMain(() -> {
                    synchronized (Manager.this) {
                        content.remove(downloadItem);
                    }
                    ManagerReactive.invalidate();
                    pumpAvailableSlots();
                });
                return;
            }

            final String dlUrl = downloadItem.getUrl();
            // content:// 目标（MediaStore / SAF）走 staging + 断点续传：目标行延迟到 commit
            // 才建（见 startDownloadChain / runStagedTransfer）。file:// / gif 走直写。
            final boolean staged = factory.targetIsContent();

            final long passSize;
            final Uri targetUri;
            if (staged) {
                // staged：续传 offset 来自本地 stage（sha256(url) 命名，跨会话稳定），
                // 不看 MediaStore 现有长度；也不现在 insert（避免 0 字节 .pending 泄漏）。
                passSize = 0;
                targetUri = null;
            } else {
                // 直写路径（file:// / gif zip）：保持原逻辑 —— query 现有长度做 append 续传，
                // 现在就 insert 打开目标。
                long fileSize = MediaStoreUtil.length(factory.query(), context);
                passSize = (!downloadItem.shouldStartNewDownload() && fileSize >= 0) ? fileSize : 0;
                Uri opened;
                try {
                    opened = factory.insert();
                } catch (Exception e) {
                    PageQueueDiagnostics.failure("factory-insert", downloadItem, e);
                    Common.showLog("[DL] factory.insert() failed: " + e);
                    e.printStackTrace();
                    // insert() 内部已自带 cleanup（MediaStoreBackend 在 openOutputStream
                    // 失败时会先 delete row 再抛），但 factory 还可能维护额外状态 ——
                    // 调一次 abandonWrite 兜底，幂等 + 内部判空。
                    try { factory.abandonWrite(); } catch (Exception ignored) {}
                    postMain(() -> {
                        DownloadToasts.show(DownloadToastKind.STORAGE_UNAVAILABLE,
                                mContext.getString(R.string.string_365));
                        analytics().illustDownloadStorageFailed();
                        complete(downloadItem, false);
                        pumpAvailableSlots();
                    });
                    return;
                }
                if (opened == null) {
                    Common.showLog("[DL] factory.insert() returned null targetUri");
                    try { factory.abandonWrite(); } catch (Exception ignored) {}
                    postMain(() -> {
                        DownloadToasts.show(DownloadToastKind.STORAGE_UNAVAILABLE,
                                mContext.getString(R.string.string_365));
                        analytics().illustDownloadStorageFailed();
                        complete(downloadItem, false);
                        pumpAvailableSlots();
                    });
                    return;
                }
                targetUri = opened;
            }

            // 回主线程启动下载链（handle 赋值需要在一致的线程）
            postMain(() -> {
                try { startDownloadChain(context, downloadItem, factory, targetUri, dlUrl, passSize, staged); }
                catch (Throwable t) {
                    reportPersistenceError("transfer preparation", t);
                    IO.execute(() -> {
                        DownloadTask handle = handles.get(downloadItem.getUuid());
                        if (handle != null) handle.cancel();
                        else { try { factory.abandonWrite(); } catch (Exception ignored) {} }
                        complete(downloadItem, false);
                    });
                }
            });
            } catch (Throwable t) {
                if (preparing[0] != null) {
                    try { preparing[0].abandonWrite(); } catch (Exception ignored) {}
                }
                reportPersistenceError("storage preparation", t);
                complete(downloadItem, false);
            }
        });
    }

    /**
     * aria2 远程下载（#692）：把 downloadItem 的 URL 通过 JSON-RPC 发给远端 aria2。
     * RPC 成功 = 这条任务完成（实际下载由远端 aria2 执行）；失败按普通下载失败
     * 处理（FAILED 留在列表里，用户可重试，重试会再次走到这里）。
     *
     * 结构刻意对齐 startDownloadChain：
     *   - disposable 进 handles —— stopAll/stopOne 能取消还没发出去的 RPC，
     *     resurrectIfStranded 也不会把 in-flight 的 RPC 误判成 stranded
     *   - 成功后 content.remove 排主线程 —— QueueDownloadManager.awaitIllustSettled
     *     靠"item 从 content 消失"判断 illust 完成，批量下载因此天然兼容
     *   - 不插 DownloadEntity（已完成 tab）—— 文件不在本地，点开会找不到；
     *     远端下载状态去 aria2 自己的前端（AriaNg 等）看
     */
    private void dispatchToAria2(DownloadItem downloadItem) {
        final String itemUuid = downloadItem.getUuid();
        DownloadTask d = DownloadTask.create(emitter -> {
            try {
                String gid = Aria2Dispatcher.dispatch(downloadItem);
                emitter.onNext(gid);
                emitter.onComplete();
            } catch (Exception e) {
                // 取消（disposed）后这个 error 静默丢弃，不会往外抛。
                emitter.tryOnError(e);
            }
        }, gid -> {
            Common.showLog("[ARIA2] dispatched uuid=" + itemUuid + " gid=" + gid
                    + " name=" + downloadItem.getName());
            // 与本地下载成功路径一致：content.remove 第一时间排上主线程
            postMain(() -> {
                synchronized (Manager.this) {
                    content.remove(downloadItem);
                }
                ManagerReactive.invalidate();
            });
            try { complete(downloadItem, true); } catch (Throwable t) { Common.showLog("[ARIA2] complete(success) failed: " + t); }
            if (!downloadItem.isSilent()) {
                postMain(() ->
                        DownloadToasts.show(DownloadToastKind.ARIA2,
                                mContext.getString(R.string.aria2_task_sent, downloadItem.getName())));
            }
        }, throwable -> {
            PageQueueDiagnostics.failure("aria2", downloadItem, throwable);
            Common.showLog("[ARIA2] dispatch failed: " + throwable);
            if (!downloadItem.isSilent()) {
                DownloadToasts.show(DownloadToastKind.ARIA2, mContext.getString(
                        R.string.aria2_send_failed, String.valueOf(throwable.getMessage())));
            }
            complete(downloadItem, false);
        }, () -> {
            Common.showLog("[ARIA2] onFinally uuid=" + itemUuid);
            workerTerminated(downloadItem);
        });
        registerHandle(itemUuid, d);
        d.start(IO);
    }

    /** Called before publishing/removing a completed page. Failed visibility commit retains staging. */
    static void finishPageOutput(DownloadFileFactory factory, File stageDir, String stageKey) {
        factory.finishWrite();
        if (stageDir != null) StageStore.clear(stageDir, stageKey);
    }

    private void startDownloadChain(Context context, DownloadItem downloadItem,
            DownloadFileFactory factory, Uri targetUri, String dlUrl,
            long passSize, boolean staged) {
        synchronized (this) {
            if (!canContinueAttempt(downloadItem)) {
                // Preparation can outlive a pause/clear. Clean up off main before releasing its slot.
                IO.execute(() -> {
                    try { factory.abandonWrite(); } catch (Exception ignored) {}
                    finally {
                        workerTerminated(downloadItem);
                    }
                });
                return;
            }
        }
        // 下载专用 H1.1 client，规避 H2 stream priority 串行化（详见 getDownloadOkHttpClient）。
        OkHttpClient client = getDownloadOkHttpClient();

        // 新一次传输尝试：把「最近读到字节」的时间戳清零。否则上一轮失败留下的旧戳，会让 UI 在
        // 新连接刚建好、还没收到首字节（甚至还在等共享文件 / 响应头）时就报出「已断流 N 秒」，
        // N 是两轮之间的间隔 —— 那不是断流，是换了条连接重来。
        downloadItem.setLastByteAtMs(0L);

        // ─── 断点续传 / staging 落点 ───
        // content:// 目标（MediaStore / SAF）一律走 staging（staged=true）：网络字节先写
        // cacheDir/staging_dl/{key}.part，直到 commit 才 factory.insert() 建目标行。好处：
        //   1) 暂停 / 失败 / 进程被杀都不会留下 0 字节 .pending 行 —— 目标行根本还没建；
        //   2) N 流并发写各自的 .part，不被 MediaProvider Binder pipe 串行化；
        //   3) .part 以 sha256(url) 命名（StageStore），跨「失败重试 / 冷启动」稳定可寻回，
        //      配 manifest 里的 validator + Content-Range 校验安全续传（见 runStagedTransfer）。
        // file:// 目标（gif zip / 用户纯路径）走直写路径（staged=false，行为不变）。
        final String itemUuid = downloadItem.getUuid();
        final java.io.File stageDir = staged
                ? new java.io.File(context.getCacheDir(), StageStore.STAGE_DIR_NAME) : null;
        final String stageKey = staged ? StageStore.keyForUrl(dlUrl) : null;
        final java.io.File stageFile = staged ? StageStore.partFile(stageDir, stageKey) : null;
        final java.io.File metaFile = staged ? StageStore.metaFile(stageDir, stageKey) : null;

        // 单主锁：同一个 url 不能有两个传输同时写同一个 .part（url-key 不像 uuid 那样
        // 天然唯一，必须挡住并发）。content 层已按 url 去重，这里是二道保险。抢不到就
        // 让本条失败，交给 pump / 队列重试 —— 那时另一条多半已 commit，record-probe 会 skip。
        final boolean acquiredStage;
        if (staged) {
            acquiredStage = activeStageKeys.add(stageKey);
            if (!acquiredStage) {
                Common.showLog("[STAGED-DL] stage key busy, defer uuid=" + itemUuid + " url=" + dlUrl);
                postMain(() -> {
                    complete(downloadItem, false);
                    pumpAvailableSlots();
                });
                return;
            }
        } else {
            acquiredStage = false;
        }

        DownloadTask d = DownloadTask.create(emitter -> {
            try {
                // 必须在可取消的 DownloadTask Body 内等待，不能占住主线程，也不能在
                // handles 注册前等待。staged 的本地复制会从 0 覆写，旧 partial 不会混入。
                File cachedFile = null;
                Timber.tag("SharedImageDownload").d("LOOKUP illustId=%d page=%d image=%s",
                        downloadItem.getIllust().getId(), downloadItem.getIndex(),
                        dlUrl.substring(dlUrl.lastIndexOf('/') + 1));
                if (!downloadItem.getIllust().isGif() && (staged || passSize == 0)) {
                    final int[] lastLoggedPercent = {-1};
                    cachedFile = ImageLoaderV3.awaitExistingFile(dlUrl, percent -> {
                        reportProgress(downloadItem, percent, 0L, 0L, false, emitter);
                        int bucket = percent / 10;
                        if (bucket != lastLoggedPercent[0]) {
                            lastLoggedPercent[0] = bucket;
                            Timber.tag("SharedImageDownload").d(
                                    "PROGRESS illustId=%d page=%d percent=%d",
                                    downloadItem.getIllust().getId(), downloadItem.getIndex(), percent);
                        }
                    });
                }
                if (emitter.isDisposed()) return;
                Timber.tag("SharedImageDownload").d(
                        "%s illustId=%d page=%d image=%s",
                        cachedFile != null ? "COPY_SHARED_FILE" : "USE_DOWNLOAD_TRANSFER",
                        downloadItem.getIllust().getId(), downloadItem.getIndex(),
                        dlUrl.substring(dlUrl.lastIndexOf('/') + 1));
                try {
                    if (staged) {
                        runStagedTransfer(context, downloadItem, factory, cachedFile, dlUrl,
                                stageDir, stageKey, stageFile, metaFile, client, emitter);
                    } else {
                        runDirectTransfer(context, downloadItem, cachedFile, targetUri, dlUrl,
                                passSize, client, emitter);
                    }
                } catch (Exception e) {
                    // 「断流立即重连」：读超时（对端沉默）在「用户主动调小过阈值」的前提下算预期内事件，
                    // 静默重连一次而不是直接判失败 —— 否则口径里的"更快重连"实际表现为"更快失败"。
                    // 判定与额度见 shouldSilentlyRetryAfterReadTimeout（有副作用：会记下额度已用，放最后判）。
                    //
                    // 就在这个传输任务里接着续传，不把状态翻回 INIT 再排队：泵关着时（行上单独
                    // 「重试」/「开始」、仅 Wi-Fi 下载离开 Wi-Fi）pump 不会再派发它，会卡在「等待中」。
                    // 留在同一个任务里，句柄、stage 单主锁、单独放行都还在；暂停 / 挂起取消了任务的话
                    // emitter 已 dispose，不会重连。不弹 toast、不发 DOWNLOAD_FAILED，对用户是"没发生过"。
                    boolean resumable = staged || "file".equals(targetUri.getScheme());
                    if (emitter.isDisposed() || !resumable
                            || !shouldSilentlyRetryAfterReadTimeout(downloadItem, e)) {
                        throw e;
                    }
                    Common.showLog("[DL-RETRY] 读超时静默重连 uuid=" + downloadItem.getUuid()
                            + " name=" + downloadItem.getName());
                    // 换了条连接：旧戳会让 UI 在等新响应头时接着读秒「已断流」。
                    downloadItem.setLastByteAtMs(0L);
                    if (staged) {
                        // stage 的 .part / manifest 原样保留，runStagedTransfer 按其长度带 Range 续传。
                        runStagedTransfer(context, downloadItem, factory, null, dlUrl,
                                stageDir, stageKey, stageFile, metaFile, client, emitter);
                    } else {
                        // 直写路径从目标文件已写长度续传（append）。
                        runDirectTransfer(context, downloadItem, null, targetUri, dlUrl,
                                new File(targetUri.getPath()).length(), client, emitter);
                    }
                }
            } catch (Exception e) {
                if (!emitter.isDisposed()) {
                    emitter.onError(e);
                }
            }
        },
        // 完成回调保持在 IO 线程（DownloadTask 在 Body 跑完后同线程交付），Gson 序列化 +
        // DB 操作 + finishWrite 不阻塞主线程。只有 UI 通知（广播、Toast）和 pump 回主线程。
        s -> {
            Common.showLog("downloadOne " + s);

            // Commit storage visibility before publishing success or removing the active row.
            // Throwing here reaches DownloadTask.onError and keeps this page visible as FAILED.
            // A failed commit retains its stage for retry and must never produce a core success tombstone.
            finishPageOutput(factory, stageDir, stageKey);

            postMain(() -> {
                int sizeBefore;
                boolean removed;
                int sizeAfter;
                synchronized (Manager.this) {
                    sizeBefore = content.size();
                    removed = content.remove(downloadItem);
                    sizeAfter = content.size();
                }
                if (removed) ManagerReactive.invalidate();
                Common.showLog("[DL-REMOVE] remove=" + removed + " sizeBefore=" + sizeBefore
                        + " sizeAfter=" + sizeAfter + " name=" + downloadItem.getName());
            });

            if(downloadItem.getIllust().isGif()){
                Shaft.getMMKV().encode(Params.ILLUST_ID + "_" + downloadItem.getIllust().getId(), true);
                postMain(() ->
                    PixivOperate.unzipAndPlay(context, downloadItem.getIllust(), downloadItem.isAutoSave()));
            }

            // Storage commit succeeded. Record enrichment failures do not invalidate the file;
            // each is isolated so the queue can acknowledge successful output.
            DownloadEntity downloadEntity = null;
            // 动图这条 DownloadItem 下的是 app cache 里的中间 zip
            // （Android10DownloadFactory22 → LegacyFile.gifZipFile），不是用户拿到的成品。
            // 把它记进「已完成」，卡片的 filePath 就指着一个压缩包，点开等于拿 zip 当图片
            // 解码 —— 一片黑（issue #920）。成品 GIF 由 UgoiraDownloadRecord 在真正编完
            // 落盘后自己记一行；zip 这里不写库、也不发完成广播。
            if (downloadItem.getIllust().isGif()) {
                Common.showLog("[DL] ugoira zip is an intermediate artefact, no 已完成 record");
            } else {
                // 重复下检测：写「已完成」之前先看这一页是不是早就有一条**文件还在**的记录。
                // 命中就说明同一张图被下了不止一次（重试路径重派、冷启动残留重派、或用户
                // 手动再下一次）。用 findUsableUri 而不是 hasUsableRecord，是为了把旧文件
                // 路径一起打出来，好判断到底是"重下覆盖"还是"多出一份 (1)"。
                // 探测是阻塞 DB + fd 校验，跑在 IO worker 上（本回调本来就在 IO 线程）。
                try {
                    Uri preexisting = RecordedPageProbe.findUsableUri(
                            Shaft.getContext(),
                            downloadItem.getIllust().getId(),
                            downloadItem.getIndex());
                    if (preexisting != null) {
                        Common.showLog("[DL-DUP] 落盘前已存在同 (illust,page) 的可用记录，疑似重复下载"
                                + " illust=" + downloadItem.getIllust().getId()
                                + " page=" + downloadItem.getIndex()
                                + " 旧=" + preexisting
                                + " 新=" + factory.getFileUri()
                                + " name=" + downloadItem.getName());
                    }
                } catch (Throwable t) {
                    Common.showLog("[DL-DUP] probe failed: " + t);
                }
                try {
                    downloadEntity = new DownloadEntity();
                    downloadEntity.setIllustGson(ceui.pixiv.download.model.DownloadRecordSlim.createIllustJson(downloadItem.getIllust()));
                    downloadEntity.setFileName(downloadItem.getName());
                    downloadEntity.setDownloadTime(System.currentTimeMillis());
                    downloadEntity.setFilePath(factory.getFileUri().toString());
                    // 已知 illust id，直接 set，insertDownload 就不必再解析 illustGson。
                    downloadEntity.setIllustId(downloadItem.getIllust().getId());
                    // v41 的 page 列：DownloadItem.index 就是 0 基页码。让「已存在则跳过」和
                    // 详情页复用本地文件能按 (illustId, page) 查，不依赖文件叫什么名字。
                    downloadEntity.setPage(downloadItem.getIndex());
                    AppDatabase.getAppDatabase(Shaft.getContext()).downloadDao().insertDownload(downloadEntity);
                    Common.showLog("[DL-CACHE] db inserted DownloadEntity fileName=" + downloadEntity.getFileName()
                            + " filePath=" + downloadEntity.getFilePath());
                    ManagerReactive.pokeDoneTable();
                } catch (Throwable t) {
                    Common.showLog("[DL] DownloadEntity insert failed (file already on disk, skipping 已完成 tab record): " + t);
                    downloadEntity = null;
                }
            }

            // 可选:把作品标签写进 JPEG 的 XMP 关键词(issue #938)。开关默认关,只处理 JPEG,
            // 内部包 try/catch —— 写元数据失败绝不影响这条已成功的下载。gif 会被扩展名过滤掉。
            // 开关前置判断:默认关时这条下载热路径零额外开销(不建 tag 列表、不读文件)。
            try {
                if (Shaft.sSettings.isWriteTagsToImageExif()) {
                    ceui.pixiv.api.model.Illust exifIllust = downloadItem.getIllust();
                    java.util.List<String> exifTags = (exifIllust != null && exifIllust.getTags() != null)
                            ? exifIllust.getTagNames()
                            : java.util.Collections.<String>emptyList();
                    ceui.pixiv.download.ExifKeywordWriter.writeIfEnabled(
                            Shaft.getContext(), factory.getFileUri(), downloadItem.getName(), exifTags);
                }
            } catch (Throwable t) { Common.showLog("[DL] exif keyword write failed: " + t); }
            try { complete(downloadItem, true); } catch (Throwable t) { Common.showLog("[DL] complete(success) failed: " + t); }

            // 广播放第二个 Runnable，跟 content.remove 顺序保留（main thread FIFO）。
            final DownloadEntity finalEntity = downloadEntity;
            postMain(() -> {
                if (!downloadItem.isSilent()) {
                    DownloadToasts.show(DownloadToastKind.DOWNLOAD_DONE,
                            downloadItem.getName() + mContext.getString(R.string.has_been_downloaded));
                }
                {
                    Intent intent = new Intent(Params.DOWNLOAD_ING);
                    Holder holder = new Holder();
                    holder.setCode(Params.DOWNLOAD_SUCCESS);
                    holder.setDownloadItem(downloadItem);
                    intent.putExtra(Params.CONTENT, holder);
                    LocalBroadcastManager.getInstance(Shaft.getContext()).sendBroadcast(intent);
                    Common.showLog("[DL-REMOVE] DOWNLOAD_ING broadcast sent");
                }
                if (finalEntity != null) {
                    Intent intent = new Intent(Params.DOWNLOAD_FINISH);
                    intent.putExtra(Params.CONTENT, finalEntity);
                    LocalBroadcastManager.getInstance(Shaft.getContext()).sendBroadcast(intent);
                }
            });
        }, throwable -> {
            //下载失败，处理相关逻辑
            PageQueueDiagnostics.failure("transfer-or-commit", downloadItem, throwable);
            Common.showLog("Manager download error: " + throwable.getMessage());

            if (!downloadItem.isSilent()) {
                DownloadToasts.show(DownloadToastKind.DOWNLOAD_FAILED,
                        "下载失败，原因：" + throwable.toString());
            }
            Common.showLog("下载失败，原因：" + throwable.toString());
            // issue #857：网络抖动 / 断链 → 进这里。之前只 complete + 广播，从不
            // 清理 factory.insert() 阶段创建的 MediaStore 行，导致用户的相册根
            // 目录里堆出大量 0 字节 `.pending-NNNN` 文件。abandonWrite 内部对
            // MediaStore 行做 delete、对 SAF 文件做 delete、对 legacy 文件做 delete
            // （只删自己刚创建的那条；pre-existing 文件不动）。
            try { factory.abandonWrite(); } catch (Exception ignored) {}
            analytics().illustDownloadFailed(throwable);
            complete(downloadItem, false);
            // 写到一半盘满了：剩下的任务照样会一条条失败，不如整体停下等用户清理（pixez#1361）。
            // 不只认 ENOSPC 原文——有的 provider 把它包成别的 IOException，所以再按余量复查一次。
            if (StorageSpaceGuard.isOutOfSpace(throwable)
                    || !StorageSpaceGuard.hasRoomForDownload(context)) {
                StorageSpaceGuard.pauseDownloadsForLowStorage(context);
            }
            {
                //通知 DOWNLOAD_ING 有一项下载失败
                Intent intent = new Intent(Params.DOWNLOAD_ING);
                Holder holder = new Holder();
                holder.setCode(Params.DOWNLOAD_FAILED);
                int idx;
                synchronized (Manager.this) {
                    idx = content.indexOf(downloadItem);
                }
                holder.setIndex(idx);
                holder.setDownloadItem(downloadItem);
                intent.putExtra(Params.CONTENT, holder);
                LocalBroadcastManager.getInstance(Shaft.getContext()).sendBroadcast(intent);
            }
        }, () -> {
            // 这条传完了：释放 stage 单主锁 + 把句柄从表里移除，主线程上 pump 下一个
            // 空闲槽位。onFinally 对 onNext / onError / cancel(暂停) 都会跑，在这里释放锁最稳。
            IO.execute(() -> {
                try {
                    DownloadTask ended = handles.get(itemUuid);
                    if (ended != null && ended.isCancelled()
                            && !downloadItem.isSuccess()) {
                        factory.abandonWrite();
                    }
                } catch (Exception e) {
                    Timber.tag("ContentQueue").w(e, "Cancelled output cleanup failed");
                } finally {
                    if (acquiredStage) activeStageKeys.remove(stageKey);
                    workerTerminated(downloadItem);
                }
            });
        });
        // 先登记句柄、再启动：极快路径（stage 已完整，commit 不走网络）会在主线程执行到
        // 这一行之前就跑完 onFinally → handles.remove。先注册保证 remove 永远晚于 put，
        // 不会在表里留下一条已完成的死句柄（那会让 resurrectIfStranded 误以为"还在跑"，
        // 冷启动残留的 stranded 页永远不复位）。
        registerHandle(itemUuid, d);
        d.start(IO);
    }

    /**
     * staged（content://）传输：网络字节 → 本地 {@code .part} → commit 到目标行。
     *
     * 断点续传的全部安全性都在这里：
     *   1. {@code .part} 已有字节数 = 续传 offset。带 {@code Range: bytes=N-}；有
     *      validator（ETag/Last-Modified）再带 {@code If-Range}，让服务器一个来回内
     *      原子决定「资源没变→206 续传 / 变了→200 全量」。
     *   2. 拿到响应后先经 {@link StageStore#decideWrite} 判定，**绝不盲目 append**：
     *      - 200：服务器无视 Range（或首次），从 offset 0 覆写 {@code .part}；
     *      - 206 且 Content-Range 起点 == 已有字节：追加；
     *      - 206 起点对不上 / 416 字节矛盾：ABORT，弃 {@code .part} 整段重下；
     *      - 416 且已持有全部字节：直接提交现有 {@code .part}。
     *   3. commit 时才 {@link DownloadFileFactory#insert()} 建目标行 —— 中途暂停 / 失败
     *      不会留下 0 字节 {@code .pending}。{@code .part} + manifest 在订阅者侧
     *      {@code finishWrite()} 成功后才删（见 startDownloadChain 的 onNext 消费块）。
     *
     * 暂停 / 取消（{@code emitter.isDisposed()}）时**保留** {@code .part} + manifest，
     * 供下次续传；只有 finishWrite 成功或 ABORT 才清理。
     */
    private void runStagedTransfer(Context context, DownloadItem downloadItem,
            DownloadFileFactory factory, File cachedFile, String dlUrl,
            java.io.File stageDir, String stageKey, java.io.File stageFile, java.io.File metaFile,
            OkHttpClient client, DownloadEmitter emitter) throws Exception {
        Response response = null;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        // commit 前的完整性基线：本次落盘后 .part 理应有多少字节。-1 = 未知（chunked /
        // 无 Content-Length）时跳过校验。见下方 commit 前的 stageFile.length() 比对。
        long expectedTotal = -1L;
        try {
            if (cachedFile != null) {
                // 缓存命中：本地已有完整字节 → 从 0 覆写 stage（不续传），删旧 manifest。
                Common.showLog("[DL-CACHE] staged local copy, src=" + cachedFile.getAbsolutePath());
                runCatchingDelete(metaFile);
                inputStream = new java.io.FileInputStream(cachedFile);
                outputStream = openStageStream(stageFile, 0);
                expectedTotal = cachedFile.length();
                pumpBytes(inputStream, outputStream, downloadItem, 0L, expectedTotal, true, emitter);
                if (emitter.isDisposed()) return;
                closeQuietly(outputStream); outputStream = null;
                closeQuietly(inputStream); inputStream = null;
            } else {
                long existingLen = stageFile.length();
                StageStore.Manifest mf = existingLen > 0 ? StageStore.readManifest(metaFile) : null;

                // 预检：manifest 已知总长时，不必发网络请求就能判断 partial 状态，省一次
                // 往返、也不依赖服务器 416 一定带 Content-Range。
                boolean skipNetwork = false;
                if (mf != null && mf.total >= 0) {
                    if (existingLen == mf.total) {
                        // 字节已齐（上次 commit 前断了 / commit 自身失败）→ 直接提交。
                        skipNetwork = true;
                        expectedTotal = mf.total;
                        Common.showLog("[STAGED-DL] stage already complete (" + existingLen + "B), commit without fetch");
                    } else if (existingLen > mf.total) {
                        // partial 比总长还大 = 脏数据，整段重下。
                        Common.showLog("[STAGED-DL] stage overshoot len=" + existingLen
                                + " > total=" + mf.total + ", reset");
                        StageStore.clear(stageDir, stageKey);
                        existingLen = 0;
                        mf = null;
                    }
                }

                if (!skipNetwork) {
                    // issue #865: 只有真正发出去的网络请求换 host；dlUrl 在别处（dedup / peek /
                    // stage key）保持原值。默认 PIXIV 模式下 rewrite 是 no-op。
                    Request.Builder rb = new Request.Builder()
                            .url(ceui.lisa.http.ImageHostManager.INSTANCE.rewrite(dlUrl))
                            .addHeader(Params.MAP_KEY, Params.IMAGE_REFERER);
                    if (existingLen > 0) {
                        rb.addHeader("Range", "bytes=" + existingLen + "-");
                        if (mf != null && mf.validator != null
                                && (StageStore.VALIDATOR_ETAG.equals(mf.validatorType)
                                    || StageStore.VALIDATOR_LASTMOD.equals(mf.validatorType))) {
                            rb.addHeader("If-Range", mf.validator);
                        }
                    }
                    Common.showLog("[STAGED-DL] fetch url=" + dlUrl + " existing=" + existingLen);
                    Timber.tag("SharedImageDownload").d("NETWORK illustId=%d page=%d resumeBytes=%d",
                            downloadItem.getIllust().getId(), downloadItem.getIndex(), existingLen);
                    response = client.newCall(rb.build()).execute();
                    int code = response.code();
                    // 416 也要放行进 decideWrite（可能意味着"字节已齐"）。
                    if (!response.isSuccessful() && code != 416) {
                        emitter.onError(new IOException("HTTP " + code));
                        return;
                    }
                    ResponseBody body = response.body();
                    long contentLength = body != null ? body.contentLength() : -1L;
                    StageStore.WriteDecision dec = StageStore.decideWrite(
                            code, response.header("Content-Range"), contentLength, existingLen);
                    Common.showLog("[STAGED-DL] code=" + code + " mode=" + dec.mode
                            + " startOffset=" + dec.startOffset + " total=" + dec.total);
                    // 没有 Content-Length 时把响应头打出来：chunked 和"okhttp 透明解压 gzip"
                    // 都会让 contentLength() 返回 -1，但两者修法完全不同，得能分开。
                    if (contentLength < 0) {
                        Common.showLog("[DL-HDR] contentLength=-1 transferEncoding="
                                + response.header("Transfer-Encoding")
                                + " contentEncoding=" + response.header("Content-Encoding")
                                + " server=" + response.header("Server")
                                + " via=" + response.header("Via")
                                + " name=" + downloadItem.getName());
                    }

                    if (dec.mode == StageStore.WriteMode.ABORT) {
                        // 无法安全续传：弃掉 partial + manifest，抛错让队列 retry 走整段重下。
                        StageStore.clear(stageDir, stageKey);
                        emitter.onError(new IOException("resume aborted (code=" + code + "), restart fresh"));
                        return;
                    }
                    // FRESH / APPEND / ALREADY_COMPLETE 落盘后 .part 理应等于 dec.total。
                    expectedTotal = dec.total;
                    if (dec.mode == StageStore.WriteMode.FRESH || dec.mode == StageStore.WriteMode.APPEND) {
                        if (body == null) {
                            emitter.onError(new IOException("Empty response body"));
                            return;
                        }
                        // 先落 manifest（validator + total）：哪怕这次中途断了，下次也能凭
                        // If-Range 安全续传。
                        StageStore.writeManifest(metaFile, StageStore.buildManifest(
                                dlUrl, response.header("ETag"), response.header("Last-Modified"), dec.total));
                        boolean append = dec.mode == StageStore.WriteMode.APPEND;
                        inputStream = body.byteStream();
                        outputStream = openStageStream(stageFile, append ? dec.startOffset : 0);
                        pumpBytes(inputStream, outputStream, downloadItem, dec.startOffset, dec.total, false, emitter);
                        if (emitter.isDisposed()) return;  // 暂停 / 取消：保留 stage 续传
                        closeQuietly(outputStream); outputStream = null;
                        closeQuietly(inputStream); inputStream = null;
                    }
                    // ALREADY_COMPLETE：字节已在 stage 里，直接进 commit。
                    closeQuietly(response); response = null;
                }
            }

            // —— commit 前的完整性闸门 ——
            // .part 落盘长度必须等于已知总长，否则 commit 出去就是截断图。网络短读会被
            // OkHttp 的 premature-EOF 抛错拦在前面；这里兜住的是本地侧的隐性截断 ——
            // 典型：续传窗口内 cacheDir 被「清除缓存」/ 第三方清理软件抹掉 .part（目录还在），
            // openStageStream 的 append 模式会**静默重建一个空文件**只写 tail，字节数就少了
            // 前缀那一段。这类 .part 无法安全续传（不知道缺哪几段），直接清掉整段重下。
            // total 未知（chunked / 无 Content-Length，expectedTotal<0）时无从校验，放行。
            if (expectedTotal >= 0) {
                long stagedLen = stageFile.length();
                if (stagedLen != expectedTotal) {
                    Common.showLog("[STAGED-DL] length mismatch stagedLen=" + stagedLen
                            + " != expected=" + expectedTotal + ", reset + restart");
                    StageStore.clear(stageDir, stageKey);
                    emitter.onError(new IOException(
                            "stage length mismatch: " + stagedLen + " != " + expectedTotal));
                    return;
                }
            }

            // —— commit：延迟 insert 目标行，写 stage → 目标；成功后删 stage + manifest ——
            Uri targetUri = factory.insert();
            if (targetUri == null) {
                emitter.onError(new IOException("staging commit: factory.insert() returned null"));
                return;
            }
            commitStageToTarget(context, stageFile, targetUri, emitter);
            if (emitter.isDisposed()) {
                // 暂停 / 取消正好落在 commit 拷贝窗口内（insert() 已建目标行，但还没
                // finishWrite 清 IS_PENDING）。不收口的话这条 partial pending 行会一直
                // 悬挂到下次冷启动才被 cleanupPendingOrphans 扫掉 —— 现在就 abandonWrite
                // 删掉它（只删本次 insert 刚建的那条，pre-existing 不动）。stage .part
                // **不清**，留给续传：下次 stage 已齐 → skipNetwork → 重新 insert + commit。
                // 注：进程被杀（非 dispose）走不到这里，那种残留仍靠冷启动 cleaner 兜底。
                try { factory.abandonWrite(); } catch (Exception ignored) {}
                return;
            }
            // stage 的清理**不在这里做**：onNext 与订阅者消费块之间隔着一次 observeOn(io)
            // 调度跳变，dispose 落在这个窗口会把 onNext 静默丢掉 —— 若此时 stage 已清，
            // 这条 IS_PENDING=1 的目标行没人 finishWrite，下次冷启动被 cleanupPendingOrphans
            // 连文件一起删掉，一张已下完的图就白丢了。清理挪到订阅者侧 finishWrite 成功
            // 之后（见 startDownloadChain 的 onNext 消费块）：onNext 被丢弃时 stage 仍在，
            // 下次尝试走 skipNetwork → 重新 insert + commit，零字节浪费。
            emitter.onNext(targetUri.toString());
            emitter.onComplete();
        } finally {
            closeQuietly(outputStream);
            closeQuietly(inputStream);
            closeQuietly(response);
        }
    }

    /**
     * 直写（file:// / gif zip）传输：保持历史行为不变 —— {@code passSize>0} 时带 Range
     * 头 + append 模式续写目标文件，无 manifest / validator（file:// 没有 ContentProvider
     * 的半成品窗口，暂停即续；跨失败续传只有 staged content:// 路径提供）。
     */
    private void runDirectTransfer(Context context, DownloadItem downloadItem, File cachedFile,
            Uri targetUri, String dlUrl, long passSize, OkHttpClient client,
            DownloadEmitter emitter) throws Exception {
        Response response = null;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            long contentLength;
            if (cachedFile != null) {
                Common.showLog("[DL-CACHE] direct local copy, src=" + cachedFile.getAbsolutePath()
                        + " dst=" + targetUri);
                inputStream = new java.io.FileInputStream(cachedFile);
                contentLength = cachedFile.length();
            } else {
                Request.Builder rb = new Request.Builder()
                        .url(ceui.lisa.http.ImageHostManager.INSTANCE.rewrite(dlUrl))
                        .addHeader(Params.MAP_KEY, Params.IMAGE_REFERER);
                if (passSize > 0) {
                    rb.addHeader("Range", "bytes=" + passSize + "-");
                }
                Timber.tag("SharedImageDownload").d("NETWORK illustId=%d page=%d resumeBytes=%d",
                        downloadItem.getIllust().getId(), downloadItem.getIndex(), passSize);
                response = client.newCall(rb.build()).execute();
                if (!response.isSuccessful()) {
                    emitter.onError(new IOException("HTTP " + response.code()));
                    return;
                }
                ResponseBody body = response.body();
                if (body == null) {
                    emitter.onError(new IOException("Empty response body"));
                    return;
                }
                inputStream = body.byteStream();
                contentLength = body.contentLength();
            }

            long totalSize = contentLength > 0 ? contentLength + passSize : 0;
            if ("file".equals(targetUri.getScheme())) {
                outputStream = new java.io.FileOutputStream(targetUri.getPath(), passSize > 0);
            } else {
                outputStream = context.getContentResolver()
                        .openOutputStream(targetUri, passSize > 0 ? "wa" : "w");
            }
            if (outputStream == null) {
                emitter.onError(new IOException("Cannot open output stream for " + targetUri));
                return;
            }
            pumpBytes(inputStream, outputStream, downloadItem, passSize, totalSize, cachedFile != null, emitter);
            if (emitter.isDisposed()) return;
            // 成功路径上的 close 不能吞：盘快满时缓冲里最后一段可能到 close 才真正落盘失败，
            // 吞掉就会把截断的文件当成下载成功记进「已完成」（pixez#1361）。抛出去走 onError 清理。
            outputStream.close(); outputStream = null;
            emitter.onNext(targetUri.toString());
            emitter.onComplete();
        } finally {
            closeQuietly(outputStream);
            closeQuietly(inputStream);
            closeQuietly(response);
        }
    }

    /**
     * 8KB buffer 拷贝主循环 + 节流进度上报。两条传输路径共用。
     *
     * {@code startOffset} 是已下字节基线（续传时 > 0）；进度按 {@code totalSize} 算，
     * {@code totalSize<=0}（chunked / 无 Content-Length）时 progress 留 0 但字节数照涨。
     * {@code emitter.isDisposed()}（暂停 / 取消）时**立即 return，不 flush** —— 已写进
     * {@code .part} 的字节留在盘上正是续传要的；调用方据 {@code isDisposed()} 跳过 commit。
     *
     * @return 实际写到的总字节数（含 startOffset）。
     */
    /** 单次 read 超过这个时长就记一笔 —— 对端不发数据的指纹（连接卡住 / 对端不响应）。 */
    private static final long READ_STALL_LOG_MS = 2000L;
    /** 整段传输超过这个时长、且均速低于 [SLOW_TRANSFER_MIN_KBPS] 时记一笔。 */
    private static final long SLOW_TRANSFER_MIN_MS = 5000L;
    private static final long SLOW_TRANSFER_MIN_KBPS = 300L;

    private long pumpBytes(InputStream in, OutputStream out, DownloadItem item,
            long startOffset, long totalSize, boolean localCopy, DownloadEmitter emitter) throws IOException {
        byte[] buffer = new byte[8192];
        long downloaded = startOffset;
        int lastProgress = 0;
        long lastUpdateNs = 0L;
        final long startedNs = System.nanoTime();
        while (true) {
            // read 和 write 必须分开计时。合成一个数分不清是"链路不吐数据"还是"本地写卡住"
            // —— 写慢会顶满 socket 接收缓冲，反过来让下一次 read 也变慢，两者症状一样。
            long readStart = System.nanoTime();
            int n = in.read(buffer);
            long readMs = (System.nanoTime() - readStart) / 1_000_000L;
            if (n == -1) {
                if (readMs >= READ_STALL_LOG_MS) {
                    Common.showLog("[DL-STALL] 收尾 read 停顿 " + readMs + "ms 已下=" + downloaded
                            + "/" + totalSize + " name=" + item.getName());
                }
                break;
            }
            if (emitter.isDisposed()) {
                return downloaded;
            }
            long writeStart = System.nanoTime();
            out.write(buffer, 0, n);
            long writeMs = (System.nanoTime() - writeStart) / 1_000_000L;
            if (readMs >= READ_STALL_LOG_MS || writeMs >= READ_STALL_LOG_MS) {
                Common.showLog("[DL-STALL] read=" + readMs + "ms write=" + writeMs + "ms 已下="
                        + downloaded + "/" + totalSize + " name=" + item.getName());
            }
            downloaded += n;
            // 断流计时起点：IO 线程上"真正读到字节"的时刻。UI 端读它算已断流秒数 —— 起点钉在源头，
            // 不受"进度回调要 postMain + 500ms 节流"影响，主线程再忙也不会让断流读秒偏晚。
            item.setLastByteAtMs(android.os.SystemClock.elapsedRealtime());
            long nowNs = System.nanoTime();
            int progress = totalSize > 0 ? (int) (downloaded * 100 / totalSize) : 0;
            boolean pctChanged = totalSize > 0 && progress != lastProgress;
            boolean timeElapsed = (nowNs - lastUpdateNs) > 500_000_000L; // 500ms
            if (pctChanged || timeElapsed) {
                lastProgress = progress;
                lastUpdateNs = nowNs;
                reportProgress(item, progress, downloaded, totalSize, localCopy, emitter);
            }
        }
        // 字节已读完，不再等对端：清掉断流戳。之后还有 commit（insert 建目标行、整段拷
        // stage → 目标），这段本地耗时期间行仍是 DOWNLOADING，不清就会被读成「已断流N/10s」。
        item.setLastByteAtMs(0L);
        out.flush();
        logIfSlowTransfer(item, startOffset, downloaded, totalSize, localCopy, startedNs);
        return downloaded;
    }

    /**
     * 整段传输偏慢时记一笔。
     *
     * 单次 read 的停顿阈值抓不到"每次 read 都很短、但整体就是慢"的形态：实测过一次
     * 续传 1.8MB 花了 10.4 秒（178 KB/s，同一份日志里的全新下载是 2–4 MB/s），那种
     * 情况每次 read 只有几十毫秒，只有"总时长 + 均速"能把它刻画出来。{@code localCopy}
     * 是本地文件复制，慢的原因不在这条链路上，跳过。
     */
    /**
     * 这次传输算不算"整段偏慢"。
     *
     * 抽成 static 是为了让阈值能被单测钉住，而不是只能靠读代码：本地复制不算（慢的原因
     * 不在这条链路上）、时长不够不算、没有有效字节不算、均速达标不算。均速用
     * {@code bytes/ms}，数值上就是 KB/s。
     */
    static boolean isSlowTransfer(long tookMs, long transferredBytes, boolean localCopy) {
        if (localCopy) return false;
        if (tookMs < SLOW_TRANSFER_MIN_MS) return false;
        if (transferredBytes <= 0) return false;
        return (transferredBytes / tookMs) < SLOW_TRANSFER_MIN_KBPS;
    }

    private void logIfSlowTransfer(DownloadItem item, long startOffset, long downloaded,
            long totalSize, boolean localCopy, long startedNs) {
        long tookMs = (System.nanoTime() - startedNs) / 1_000_000L;
        long bytes = downloaded - startOffset;
        if (!isSlowTransfer(tookMs, bytes, localCopy)) return;
        Common.showLog("[DL-SLOW] 整段偏慢 用时=" + tookMs + "ms 本次下=" + bytes
                + "B 进度=" + downloaded + "/" + totalSize + " 均速=" + (bytes / tookMs)
                + "KB/s name=" + item.getName());
    }

    private void reportProgress(DownloadItem item, int percent, long downloaded, long total,
            boolean localCopy, DownloadEmitter emitter) {
        if (emitter.isDisposed()) return;
        postMain(() -> {
            // 暂停/重试以后迟到的共享进度不能把旧任务重新标成 DOWNLOADING。
            if (emitter.isDisposed()) return;
            // 显示任务可能已到 99%；随后本地复制不能把下载列表的进度拉回 0%。
            int progress = localCopy ? Math.max(item.getNonius(), percent) : percent;
            item.setNonius(progress);
            item.setCurrentSize(downloaded);
            item.setTotalSize(total);
            item.setState(DownloadItem.DownloadState.DOWNLOADING);
            ManagerReactive.invalidate();
            try {
                Callback<DownloadProgress> callback = getCallback(item.getUuid());
                if (callback != null) callback.doSomething(new DownloadProgress(progress, downloaded, total));
            } catch (Exception e) {
                Common.showLog("Manager progress callback error: " + e.getMessage());
            }
        });
    }

    /**
     * stage → 目标 Uri 一次性串行 copy。读循环已走完，不再有 TCP 反压；commit 串行
     * 只是几十 ms。{@code emitter.isDisposed()} 时中途 return（不删 stage，留给续传）。
     */
    private void commitStageToTarget(Context context, java.io.File stageFile, Uri targetUri,
            DownloadEmitter emitter) throws IOException {
        long t0 = System.nanoTime();
        java.io.FileInputStream stageIn = null;
        OutputStream mediaOut = null;
        try {
            stageIn = new java.io.FileInputStream(stageFile);
            mediaOut = context.getContentResolver().openOutputStream(targetUri, "w");
            if (mediaOut == null) {
                throw new IOException("staging commit: openOutputStream null for " + targetUri);
            }
            byte[] copyBuf = new byte[64 * 1024];
            int n;
            while ((n = stageIn.read(copyBuf)) != -1) {
                if (emitter.isDisposed()) return;
                mediaOut.write(copyBuf, 0, n);
            }
            mediaOut.flush();
            // 同 runDirectTransfer：close 失败 = 目标文件没写全，必须让 commit 失败而不是悄悄吞掉。
            mediaOut.close();
            mediaOut = null;
        } finally {
            closeQuietly(mediaOut);
            closeQuietly(stageIn);
        }
        Common.showLog("[STAGED-DL] commit done ms=" + (System.nanoTime() - t0) / 1_000_000L
                + " dst=" + targetUri);
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    private static void runCatchingDelete(java.io.File f) {
        if (f != null) {
            try { f.delete(); } catch (Exception ignored) {}
        }
    }

    /**
     * 打开 staging .part 文件做写入，兜底 cacheDir 被外部抹掉的情形。
     *
     * Android 在低存储时会主动清 app 的 cacheDir，用户也能从「设置→应用→
     * 清除缓存」一键清掉，第三方清理软件、StorageManager.allocateBytes 触发
     * 的回收同理。批量队列在 PENDING 排队若干分钟到几小时是常态（issue #885
     * 截图里 34 条 PENDING），等真正调度到时 staging_dl 子目录大概率已经被
     * 抹掉。FileOutputStream 不会递归建目录 → ENOENT 直接抛，整批 FAILED。
     *
     * 这里捕到 FNF 后看 effectivePassSize:
     *   - == 0：从头下，request 没带 Range 头。重建父目录后开新文件，零数据
     *     损失。即便 mkdirs 又失败（权限 / 磁盘满），原 FNF 照 throw，外层
     *     onError 走标 FAILED 流程，不会比修前更差。
     *   - >  0：续传，request 已经带 Range:bytes={N}- 头让服务器跳过前 N
     *     字节。父目录没了 = stage 前 N 字节也没了，重建空目录硬接着写会
     *     落出一张 (totalSize - N) 字节的损坏图、commit 进相册。不能救，
     *     直接 throw 让 retry path 再进来一次 —— 下次 stageFile.length()==0
     *     自然走整段重下。
     *
     * package-private 是给同包测试 (ManagerStagingConcurrencyTest) 直接调，
     * 不暴露给外部。
     */
    static java.io.FileOutputStream openStageStream(
            java.io.File stageFile, long effectivePassSize) throws IOException {
        final boolean append = effectivePassSize > 0;
        try {
            return new java.io.FileOutputStream(stageFile, append);
        } catch (java.io.FileNotFoundException fnf) {
            if (append) throw fnf;
            java.io.File parent = stageFile.getParentFile();
            if (parent != null && (parent.isDirectory() || parent.mkdirs())) {
                return new java.io.FileOutputStream(stageFile, false);
            }
            throw fnf;
        }
    }

    private String uuid;
    private long currentIllustID;

    public long getCurrentIllustID() {
        return currentIllustID;
    }

    public String getUuid() {
        return uuid;
    }

    private final Map<String, Callback<DownloadProgress>> mCallback = new HashMap<>();

    public Callback<DownloadProgress> getCallback(String uuid) {
        return mCallback.getOrDefault(uuid, null);
    }

    public void clearCallback() {
        mCallback.clear();
    }

    public void setCallback(Callback<DownloadProgress> callback) {
        mCallback.put("", callback);
    }

    public void setCallback(String uuid, Callback<DownloadProgress> callback) {
        mCallback.put(uuid, callback);
    }

    public List<DownloadItem> getContent() {
        return content;
    }

    /**
     * 是否有这个 uuid 的活动 disposable。QueueDownloadManager 的 retry path 用这个
     * 区分"冷启动 Manager.restore 带回的 stranded DOWNLOADING（无 handle，应翻 INIT
     * 重发）"vs"真在跑的 DOWNLOADING（有 handle，**绝不能**翻 INIT，否则 pump 会
     * 再 dispatch 一条传输任务，跟原 chain 抢同一个 stage 文件 / targetUri）"。
     *
     * 正在派发（[dispatching]：已置 DOWNLOADING、句柄还在 IO → 主线程路上）同样算在跑，
     * 跟 {@link #resurrectIfStranded} 的判据保持一致，否则 retry path 会在这段窗口里
     * 把它翻 INIT，复现同 uuid 两条传输。
     */
    public boolean isRunningHandle(String uuid) {
        return handles.containsKey(uuid) || dispatching.contains(uuid);
    }

    /**
     * 队列外写同一张图的入口（大图页「保存这一张」）借用 {@link #activeStageKeys} 的同 url
     * 单主锁，免得和这里的传输同时写同一个模板目标（#1312）。抢到返回 true，用完必须
     * {@link #releaseUrlWrite}。占着期间这条 url 开传会走「stage key busy」判失败：QueueDownloadManager
     * 派的页由队列自动重试，直接 addTask(s) 进来的（详情页「下载全部」）停在 FAILED 等用户重试。
     */
    public boolean tryAcquireUrlWrite(String url) {
        return activeStageKeys.add(StageStore.keyForUrl(url));
    }

    public void releaseUrlWrite(String url) {
        activeStageKeys.remove(StageStore.keyForUrl(url));
    }
}


