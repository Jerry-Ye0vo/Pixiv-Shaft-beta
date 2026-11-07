package ceui.lisa.core

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * [DownloadTask] 的取消状态机（替代 Rx `Observable.create + Disposable` 后的自有实现）。
 * 单线程 executor 让「下一条任务复用同一条线程」可观测——残留的 interrupt 标记会在这里现形。
 */
class DownloadTaskTest {

    private val io = Executors.newSingleThreadExecutor { r -> Thread(r, "dl-test").apply { isDaemon = true } }

    @After
    fun tearDown() {
        io.shutdownNow()
    }

    /** 在同一条池线程上跑一个探针，返回它看到的 interrupt 标记（并顺手清掉）。 */
    private fun probeInterruptFlag(): Boolean {
        val seen = AtomicReference<Boolean>()
        io.submit { seen.set(Thread.interrupted()) }.get(2, TimeUnit.SECONDS)
        return seen.get()
    }

    @Test
    fun `cancel while queued skips body and runs onFinally exactly once`() {
        val gate = CountDownLatch(1)
        io.execute { gate.await() } // 占住唯一线程，让任务排队
        val bodyRan = AtomicInteger()
        val finallyRan = AtomicInteger()
        val task = DownloadTask.launch(io, { bodyRan.incrementAndGet() }, {}, {}, { finallyRan.incrementAndGet() })

        task.cancel()
        assertEquals(1, finallyRan.get())
        gate.countDown()
        io.submit {}.get(2, TimeUnit.SECONDS) // 等队列排空

        assertEquals(0, bodyRan.get())
        assertEquals(1, finallyRan.get())
        assertTrue(task.isCancelled)
    }

    @Test
    fun `cancel while producing interrupts body, delivers nothing, leaves no interrupt flag`() {
        val started = CountDownLatch(1)
        val interrupted = AtomicReference<Boolean>(false)
        val next = AtomicReference<String>()
        val error = AtomicReference<Throwable>()
        val finallyRan = AtomicInteger()
        val task = DownloadTask.launch(io, { emitter ->
            started.countDown()
            try {
                Thread.sleep(5_000)
                emitter.onNext("should-not-arrive")
            } catch (e: InterruptedException) {
                interrupted.set(true)
                emitter.tryOnError(e)
            }
        }, { next.set(it) }, { error.set(it) }, { finallyRan.incrementAndGet() })

        assertTrue(started.await(2, TimeUnit.SECONDS))
        task.cancel()
        io.submit {}.get(2, TimeUnit.SECONDS)

        assertTrue("producing 阶段的 cancel 必须打断 Body", interrupted.get())
        assertNull(next.get())
        assertNull(error.get())
        assertEquals(1, finallyRan.get())
        assertFalse("池线程归还后不能残留 interrupt 标记", probeInterruptFlag())
    }

    @Test
    fun `cancel while consuming does not interrupt the delivering thread`() {
        val inConsumer = CountDownLatch(1)
        val release = CountDownLatch(1)
        val interruptSeenInConsumer = AtomicReference<Boolean>()
        val error = AtomicReference<Throwable>()
        val task = DownloadTask.launch(io, { emitter ->
            emitter.onNext("uri")
            emitter.onComplete()
        }, {
            inConsumer.countDown()
            release.await(2, TimeUnit.SECONDS)
            interruptSeenInConsumer.set(Thread.currentThread().isInterrupted)
        }, { error.set(it) }, {})

        assertTrue(inConsumer.await(2, TimeUnit.SECONDS))
        task.cancel() // 已进入消费阶段：对应 Rx 里 dispose 碰不到 observeOn 线程
        release.countDown()
        io.submit {}.get(2, TimeUnit.SECONDS)

        assertEquals(false, interruptSeenInConsumer.get())
        assertNull(error.get())
        assertFalse(probeInterruptFlag())
    }

    @Test
    fun `cancel keeps resources until an uncooperative body actually exits`() {
        val started = CountDownLatch(1)
        val interrupted = CountDownLatch(1)
        val release = CountDownLatch(1)
        val finished = CountDownLatch(1)
        val task = DownloadTask.launch(io, {
            started.countDown()
            while (release.count > 0) {
                try { release.await() }
                catch (_: InterruptedException) { interrupted.countDown() }
            }
        }, {}, {}, { finished.countDown() })
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS))
            task.cancel()
            assertTrue(interrupted.await(2, TimeUnit.SECONDS))
            assertEquals("Cancellation is not worker termination", 1L, finished.count)
        } finally { release.countDown() }
        assertTrue(finished.await(2, TimeUnit.SECONDS))
        assertFalse(probeInterruptFlag())
    }

    @Test
    fun `cancel during output commit defers finalization until commit returns`() {
        val committing = CountDownLatch(1)
        val release = CountDownLatch(1)
        val finished = CountDownLatch(1)
        val task = DownloadTask.launch(io, { it.onNext("output") }, {
            committing.countDown()
            release.await(2, TimeUnit.SECONDS)
        }, {}, { finished.countDown() })
        try {
            assertTrue(committing.await(2, TimeUnit.SECONDS))
            task.cancel()
            assertEquals(1L, finished.count)
        } finally { release.countDown() }
        assertTrue(finished.await(2, TimeUnit.SECONDS))
    }

    @Test
    fun `executor rejection reports failure and releases reservation`() {
        io.shutdown()
        val error = AtomicReference<Throwable>()
        val finished = AtomicInteger()
        DownloadTask.launch(io, { throw AssertionError("must not run") }, {},
            { error.set(it) }, { finished.incrementAndGet() })
        assertTrue(error.get() is java.util.concurrent.RejectedExecutionException)
        assertEquals(1, finished.get())
    }

    @Test
    fun `racing cancel against phase transition never leaks an interrupt onto the pool thread`() {
        repeat(500) { i ->
            val done = CountDownLatch(1)
            val task = DownloadTask.launch(io, { emitter ->
                emitter.onNext("v$i")
                emitter.onComplete()
            }, {}, {}, { done.countDown() })
            // 与工作线程的 PRODUCING→CONSUMING→DONE 切换赛跑
            task.cancel()
            assertTrue(done.await(2, TimeUnit.SECONDS))
            assertFalse("第 $i 轮：cancel 与 phase 切换赛跑后池线程带着 interrupt 标记", probeInterruptFlag())
        }
    }
}

