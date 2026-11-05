package ceui.lisa.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import ceui.lisa.utils.Common;

/**
 * {@link Manager} 单条传输的执行/取消句柄，替代原先的
 * {@code Observable.create(...).subscribeOn(io).observeOn(io).doFinally(...).subscribe(onNext, onError)}
 * + {@code Disposable}。事件交付沿用 RxJava 2 语义，资源释放等待实际退出：
 *
 * <ul>
 *   <li>{@link Body} 在 IO 线程跑，通过 {@link DownloadEmitter} 发 onNext / onComplete / onError；
 *       终态（complete / error）之后再发任何事件一律静默丢弃，{@link DownloadEmitter#isDisposed()}
 *       在终态或被取消后都返回 true（与 {@code ObservableCreate.CreateEmitter} 一致）。</li>
 *   <li>Body 跑完后，值/错误在**同一条 IO 线程**上顺序交给 {@code onNext} / {@code onError}
 *       消费者（对应 observeOn(io)：消费仍在 IO，不进主线程）。交付前再查一次取消标记——
 *       "producer 已 onNext、consumer 还没跑"这段窗口里被 cancel 的话该值被丢弃，
 *       和 observeOn 跳变窗口的行为相同。</li>
 *   <li>{@code onNext} 消费者抛异常 → 转交 {@code onError}；{@code onError} 抛异常 → 只打日志
 *       （对应 RxJavaPlugins 的全局兜底，不 crash）。</li>
 *   <li>{@code onFinally} 恰好跑一次，且只在 Body 和消费回调均已退出后运行。
 *       尚未执行的任务可立即取消；运行中的任务必须等待退出，调用者才能安全释放文件锁与名额。</li>
 *   <li>{@link #cancel()} 只在 Body 还在跑（producing 阶段）时中断工作线程（Rx 的
 *       subscribeOn worker dispose 也会 interrupt）；已进入消费阶段就不中断——Rx 里消费者
 *       在另一条 observeOn 线程上，dispose 本来就碰不到它，DB 写入 / finishWrite 不能被打断。</li>
 * </ul>
 */
final class DownloadTask {

    interface Body {
        void run(DownloadEmitter emitter) throws Exception;
    }

    interface Consumer<T> {
        void accept(T value) throws Exception;
    }

    private static final int QUEUED = 0;
    private static final int PRODUCING = 1;
    private static final int CONSUMING = 2;
    private static final int DONE = 3;

    private final Body body;
    private final Consumer<String> onNext;
    private final Consumer<Throwable> onError;
    private final Runnable onFinally;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicBoolean finallyRan = new AtomicBoolean(false);
    private final AtomicBoolean submitted = new AtomicBoolean(false);
    /**
     * 守住「读 phase/worker → interrupt」与「phase 切换 → 清中断标记」两段的原子性：
     * 否则 cancel() 读到 PRODUCING 后被抢占，工作线程跑完交付、归还线程池、领走别条下载，
     * 主线程恢复时 interrupt 会打到那条无辜的下载（Rx 的 FutureTask.cancel(true) 靠 runner CAS
     * 防的就是这个）。
     */
    private final Object interruptLock = new Object();
    private Thread worker;
    private int phase = QUEUED;

    private DownloadTask(Body body, Consumer<String> onNext, Consumer<Throwable> onError, Runnable onFinally) {
        this.body = body;
        this.onNext = onNext;
        this.onError = onError;
        this.onFinally = onFinally;
    }

    /**
     * 只构造、不启动。调用方可以先把句柄登记进 {@code Manager.handles} 再 {@link #start}，
     * 避免"任务已经跑完并把自己从表里移除，登记才发生"——那会在表里留下一条已完成的
     * 死句柄，让"还在跑吗"的判断出错。
     */
    static DownloadTask create(Body body, Consumer<String> onNext,
                               Consumer<Throwable> onError, Runnable onFinally) {
        return new DownloadTask(body, onNext, onError, onFinally);
    }

    /** 提交到 [io] 执行。可在 [create] 之后、句柄登记完成之后再调。 */
    void start(ExecutorService io) {
        if (!submitted.compareAndSet(false, true)) throw new IllegalStateException("Task already submitted");
        try {
            io.execute(this::run);
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            synchronized (interruptLock) { phase = DONE; }
            try {
                if (!cancelled.get()) onError.accept(rejected);
            } catch (Throwable t) {
                Common.showLog("DownloadTask rejection handler threw: " + t);
            } finally { runFinally(); }
        }
    }

    static DownloadTask launch(ExecutorService io, Body body, Consumer<String> onNext,
                               Consumer<Throwable> onError, Runnable onFinally) {
        DownloadTask task = create(body, onNext, onError, onFinally);
        task.start(io);
        return task;
    }

    /** 对应 {@code Disposable.dispose()}：幂等。 */
    void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        boolean neverStarted = false;
        synchronized (interruptLock) {
            if (phase == QUEUED) {
                phase = DONE;
                neverStarted = true;
            }
            Thread t = worker;
            if (phase == PRODUCING && t != null && t != Thread.currentThread()) {
                t.interrupt();
            }
        }
        if (neverStarted) runFinally();
    }

    boolean isCancelled() {
        return cancelled.get();
    }

    private void runFinally() {
        if (!finallyRan.compareAndSet(false, true)) return;
        try {
            onFinally.run();
        } catch (Throwable t) {
            Common.showLog("DownloadTask onFinally threw: " + t);
        }
    }

    private void run() {
        synchronized (interruptLock) {
            // Claim execution atomically with cancellation, before exposing a live worker.
            if (phase != QUEUED) return;
            phase = PRODUCING;
            worker = Thread.currentThread();
        }
        Emitter emitter = new Emitter();
        try {
            try {
                body.run(emitter);
            } catch (Throwable t) {
                // Body 自己没兜住的异常（Rx 里 create 的 lambda 抛出会走 tryOnError）。
                emitter.tryOnError(t);
            }
            // producing 结束：清掉可能残留的中断标记（线程要还给池子）。与 cancel() 同锁，
            // 保证不会有 interrupt 在这之后才落下来。
            synchronized (interruptLock) {
                phase = CONSUMING;
                Thread.interrupted();
            }
            if (cancelled.get()) {
                return;
            }
            deliver(emitter);
        } finally {
            synchronized (interruptLock) {
                phase = DONE;
                worker = null;
                Thread.interrupted();
            }
            runFinally();
        }
    }

    private void deliver(Emitter emitter) {
        List<String> values;
        Throwable error;
        synchronized (emitter) {
            values = new ArrayList<>(emitter.values);
            error = emitter.error;
        }
        for (String v : values) {
            if (cancelled.get()) return;
            try {
                onNext.accept(v);
            } catch (Throwable t) {
                // LambdaObserver：onNext 抛错 → 取消上游并转 onError。
                error = t;
                break;
            }
        }
        if (error != null && !cancelled.get()) {
            try {
                onError.accept(error);
            } catch (Throwable t) {
                Common.showLog("DownloadTask onError handler threw: " + t);
            }
        }
    }

    private final class Emitter implements DownloadEmitter {
        private final List<String> values = new ArrayList<>();
        private Throwable error;
        private boolean terminated;

        @Override
        public synchronized void onNext(String value) {
            if (isDisposed()) return;
            values.add(value);
        }

        @Override
        public synchronized void onComplete() {
            if (isDisposed()) return;
            terminated = true;
        }

        @Override
        public synchronized void onError(Throwable e) {
            if (isDisposed()) return;
            error = e;
            terminated = true;
        }

        @Override
        public void tryOnError(Throwable e) {
            onError(e);
        }

        @Override
        public boolean isDisposed() {
            return cancelled.get() || terminated;
        }
    }
}

