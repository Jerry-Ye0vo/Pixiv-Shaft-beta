package ceui.pixiv.imageloader

import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentPriority
import ceui.pixiv.download.queue.ContentQueueStore
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okio.ByteString.Companion.encodeUtf8
import java.io.Closeable
import java.io.File
import java.io.IOException

/** A consumer owns a cache reference until it finishes reading, not merely until fetch returns. */
class OriginalFileLease internal constructor(val file: File, private val pin: Closeable) : Closeable {
    override fun close() = pin.close()
}

/** Shared source transfer; page COPY jobs never hold a NETWORK permit while awaiting this job.
 * No fetch starts before its persisted NETWORK claim commits. Exact URLs include query identity.
 * Cancelling one consumer only withdraws its interest; the last consumer stops the worker.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal class OriginalTransferQueue(
    private val runtime: ContentQueueRuntime,
    private val scope: CoroutineScope,
    private val fetcher: ImageFetcher,
    private val cached: (String) -> File?,
    private val pin: (String) -> Closeable,
    private val capacity: () -> Int,
    private val shouldRetryStall: (Throwable, Boolean) -> Boolean = { _, _ -> false },
) {
    private class Flight {
        val result = CompletableDeferred<File>()
        val progress = MutableStateFlow(0)
        lateinit var job: Job
        var consumers = 0
        var stopping = false
        @Volatile var saveRequested = false
        @Volatile var priority = ContentQueueRuntime.BACKGROUND
        @Volatile var taskId: String? = null
    }
    private val mutex = Mutex()
    private val flights = mutableMapOf<String, Flight>()

    suspend fun acquire(url: String, onProgress: (Int) -> Unit, saving: Boolean = false): OriginalFileLease {
        var loan: OriginalFileLease? = null
        try {
            return withContext(Dispatchers.IO) { acquireOnIo(url, onProgress, saving).also { loan = it } }
        } catch (error: Throwable) {
            // withContext can reject delivery on a cancelled caller after the IO result was created.
            loan?.close()
            throw error
        }
    }

    private suspend fun acquireOnIo(url: String, onProgress: (Int) -> Unit, saving: Boolean): OriginalFileLease {
        val priority = if (!saving) ContentQueueRuntime.VISIBLE else
            currentCoroutineContext()[ContentPriority]?.value ?: ContentQueueRuntime.SAVE
        val reference = pin(url) // reserve before probe and commit-triggered LRU trim
        var handedOff = false
        try {
            cached(url)?.takeIf { it.isFile && it.length() > 0 }?.let {
                handedOff = true
                return OriginalFileLease(it, reference)
            }
            while (true) {
                currentCoroutineContext().ensureActive()
                val (flight, retiring) = mutex.withLock {
                    val existing = flights[url]
                    if (existing != null) {
                        val retiring = existing.stopping
                        if (!retiring) {
                            existing.consumers++
                            if (saving) existing.saveRequested = true
                            if (priority > existing.priority) {
                                existing.priority = priority
                                existing.taskId?.let { runtime.prioritize(it, priority) }
                            }
                        }
                        existing to retiring
                    } else Flight().also { created ->
                        created.consumers = 1
                        created.saveRequested = saving
                        created.priority = priority
                        flights[url] = created
                        created.job = scope.launch(start = CoroutineStart.ATOMIC) { transfer(url, created) }
                    } to false
                }
                if (retiring) { flight.job.join(); continue }
                try {
                    val file = coroutineScope {
                        val progress = launch { flight.progress.collect { onProgress(it) } }
                        try { flight.result.await() } finally { progress.cancel() }
                    }
                    if (!file.isFile || file.length() <= 0) throw IOException("Shared image cache unavailable")
                    handedOff = true
                    return OriginalFileLease(file, reference)
                } finally {
                    withContext(NonCancellable) {
                        mutex.withLock {
                            flight.consumers--
                            if (flight.consumers == 0 && !flight.job.isCompleted) {
                                flight.stopping = true
                                flight.job.cancel()
                            }
                        }
                    }
                }
            }
            @Suppress("UNREACHABLE_CODE") error("unreachable")
        } finally { if (!handedOff) reference.close() }
    }

    private suspend fun transfer(url: String, flight: Flight) {
        var lease: ContentQueueStore.Task? = null
        var taskId: String? = null
        var committed = false
        var failure: Throwable? = null
        var workerPin: Closeable? = null
        try {
            workerPin = pin(url) // last waiter may cancel before the socket/body actually exits
            runtime.initialize()
            // A preceding transfer or another consumer may have populated the cache while queuing.
            val already = cached(url)?.takeIf { it.isFile && it.length() > 0 }
            if (already != null) { flight.result.complete(already); return }
            val digest = url.encodeUtf8().sha256().hex()
            taskId = "original:$digest"
            flight.taskId = taskId
            runtime.store.admit(taskId, "original-cache:$digest", "pixiv-bytes:$digest",
                Pool.NETWORK, url, State.QUEUED, false)
            while (lease == null) {
                currentCoroutineContext().ensureActive()
                val signal = runtime.changes.value
                lease = runtime.claim(taskId, Pool.NETWORK, capacity().coerceIn(1, 5),
                    flight.priority)
                if (lease == null) {
                    runtime.changes.first { it != signal }
                }
            }
            // Cache-only probe after the claim prevents a stale miss from forcing another request.
            var file = cached(url)?.takeIf { it.isFile && it.length() > 0 }
            var retried = false
            while (file == null) {
                try { file = fetcher.fetch(url) { flight.progress.value = it } }
                catch (error: Exception) {
                    if (error is CancellationException || retried || !shouldRetryStall(error, flight.saveRequested)) throw error
                    retried = true
                    flight.progress.value = 0
                }
            }
            currentCoroutineContext().ensureActive()
            if (!file.isFile || file.length() <= 0) throw IOException("Shared image transfer produced no bytes")
            val claimed = requireNotNull(lease)
            if (!runtime.store.markCommitted(claimed.id, claimed.attempt)) throw IOException("Stale image transfer")
            committed = true
            flight.result.complete(file)
        } catch (error: Throwable) {
            failure = error
            withContext(NonCancellable) { mutex.withLock { flight.stopping = true } }
            flight.result.completeExceptionally(error)
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                try {
                    val ended = lease
                    val admitted = taskId
                    if (ended != null) {
                        if (failure is CancellationException && !committed) runtime.store.cancel(ended.id)
                        if (!runtime.store.finish(ended.id, ended.attempt, committed, failure?.javaClass?.simpleName)) {
                            throw IOException("Stale image transfer acknowledgement")
                        }
                        runtime.store.compactTerminal(ended.id)
                    } else if (admitted != null) {
                        runtime.store.cancel(admitted)
                        runtime.store.compactTerminal(admitted)
                    }
                } catch (ack: Throwable) {
                    // Fail closed: the persisted live permit remains unavailable until process recovery.
                    timber.log.Timber.tag("ContentQueue").e(ack, "Image transfer acknowledgement failed")
                } finally {
                    try { workerPin?.close() }
                    catch (error: Exception) { timber.log.Timber.tag("ContentQueue").w(error, "Cache pin release failed") }
                    taskId?.let(runtime::withdraw) ?: runtime.changed()
                    mutex.withLock { if (flights[url] === flight) flights.remove(url) }
                }
            }
        }
    }
}
