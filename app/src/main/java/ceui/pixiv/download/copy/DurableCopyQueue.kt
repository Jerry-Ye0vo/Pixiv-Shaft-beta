package ceui.pixiv.download.copy

import android.content.Context
import ceui.pixiv.download.queue.ContentQueueRuntime
import ceui.pixiv.download.queue.ContentQueueStore
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import timber.log.Timber

/** Durable, process-owned local outputs. UI jobs never own the writer or its storage permission.
 * One actor serializes restore/admission/commands; physical workers still require shared COPY claims.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class DurableCopyQueue(
    context: Context,
    private val runtime: ContentQueueRuntime,
    private val output: CopyOutput,
    private val capacity: () -> Int,
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
    private val gson: Gson = Gson(),
    private val succeeded: suspend (CopyRequest) -> Unit = {},
) {
    private val app = context.applicationContext
    private val sources by lazy { File(app.filesDir, "content_queue_sources") }
    private val lock = Any()
    private val families = linkedMapOf<String, CopyTicket>()
    private val tickets = java.util.concurrent.ConcurrentHashMap<String, CopyTicket>()
    private val workers = mutableMapOf<String, Job>()
    private val inputReferences = java.util.concurrent.ConcurrentHashMap<String, java.io.Closeable>()
    private val commands = Channel<suspend () -> Unit>(Channel.UNLIMITED)
    private val pumpQueued = AtomicBoolean()
    private val mutable = MutableStateFlow<Map<String, CopyStatus>>(emptyMap())
    val states: StateFlow<Map<String, CopyStatus>> = mutable
    private val saved = MutableStateFlow<Set<String>>(emptySet())
    val savedSources: StateFlow<Set<String>> = saved
    val ready = CompletableDeferred<Unit>()
    @Volatile private var initializationFailure: Throwable? = null

    init {
        runtime.addListener(Runnable { requestPump() })
        scope.launch(start = CoroutineStart.ATOMIC) {
            try {
                runtime.initialize()
                restore()
                ready.complete(Unit)
                requestPump() // admissions/cancellations already queued during restore run before this pump
                for (command in commands) {
                    try { command() }
                    catch (error: CancellationException) { throw error }
                    catch (error: Exception) { Timber.tag("ContentQueue").e(error, "Copy queue command failed") }
                }
            } catch (error: Throwable) {
                ready.completeExceptionally(error)
                if (error is CancellationException) throw error
                initializationFailure = error
                synchronized(lock) { families.values.toList() }.forEach {
                    it.publish(State.FAILED, error.message); publish(it)
                }
                inputReferences.values.forEach { it.close() }; inputReferences.clear()
                Timber.tag("ContentQueue").e(error, "Copy queue initialization failed")
            }
        }
    }

    /** Main-safe reservation. The request factory, persistence and all file work run on IO. */
    fun submit(family: String, source: File, request: suspend () -> CopyRequest): CopyTicket {
        initializationFailure?.let { error ->
            return CopyTicket(PREFIX + UUID.randomUUID(), family).also { it.publish(State.FAILED, error.message) }
        }
        val ticket = synchronized(lock) {
            val old = families[family]
            if (old != null && old.status.value.state !in TERMINAL) return old
            CopyTicket(PREFIX + UUID.randomUUID(), family).also {
                inputReferences[it.id] = CopyInputReferences.reserve(source)
                families[family] = it
            }
        }
        ticket.signal = { requestPump() }
        commands.trySend { admit(ticket, request) }
        return ticket
    }

    /** Process shutdown/test teardown; physical worker finalizers still acknowledge before store close. */
    suspend fun shutdown() {
        scope.coroutineContext[Job]?.cancelAndJoin()
        inputReferences.values.forEach { it.close() }; inputReferences.clear()
        commands.close()
    }

    fun ticket(id: String): CopyTicket? = tickets[id]
    fun resume(id: String) { commands.trySend {
        val ticket = tickets[id] ?: return@trySend
        if (workers.containsKey(id)) return@trySend
        if (runtime.store.get(id)?.state !in setOf(State.PAUSED, State.FAILED)) return@trySend
        ticket.retry()
        if (runtime.store.resume(id)) { ticket.publish(State.QUEUED); publish(ticket); requestPump(); runtime.changed() }
    } }

    fun cancel(family: String): Boolean = synchronized(lock) { families[family] }?.cancel() ?: false

    private fun requestPump() {
        if (pumpQueued.compareAndSet(false, true)) {
            if (commands.trySend { pumpQueued.set(false); pump() }.isFailure) pumpQueued.set(false)
        }
    }

    private suspend fun admit(reservation: CopyTicket, factory: suspend () -> CopyRequest) {
        val canonical = synchronized(lock) { families[reservation.family] }
        if (canonical != null && canonical !== reservation && canonical.status.value.state !in TERMINAL) {
            reservation.bind(canonical, scope)
            inputReferences.remove(reservation.id)?.close()
            return
        }
        if (reservation.isCancelled()) {
            inputReferences.remove(reservation.id)?.close()
            reservation.publish(State.CANCELLED); publish(reservation); return
        }
        try {
            val supplied = factory()
            supplied.validate()
            val latest = tickets.values.maxOfOrNull { it.intent?.request?.submittedAt ?: 0L } ?: 0L
            val request = supplied.copy(submittedAt = maxOf(supplied.submittedAt, latest + 1))
            check(request.family == reservation.family) { "Copy identity changed during admission" }
            val previous = tickets.values.filter { it.family == request.family }.maxByOrNull { it.sequence }
                ?.takeIf { it.status.value.state == State.FAILED }
            val old = previous?.intent
            val source = File(request.sourcePath)
            val sameInput = old != null && (old.request.sourceSize == request.sourceSize &&
                old.request.sourceModified == request.sourceModified || !source.exists())
            if (previous != null && old != null && sameInput && old.request.destination == request.destination) {
                previous.retry()
                previous.intent = old.copy(request = request.copy(sourceSize = old.request.sourceSize,
                    sourceModified = old.request.sourceModified))
                val row = runtime.store.admit(previous.id, request.family, resource(request.destination), Pool.COPY,
                    gson.toJson(previous.intent), State.QUEUED, false)
                previous.sequence = row.sequence
                synchronized(lock) { families[request.family] = previous }
                val reference = inputReferences.remove(reservation.id)
                if (reference != null) {
                    if (old.snapshotName?.let(::snapshot)?.isFile == true || inputReferences.putIfAbsent(previous.id, reference) != null) reference.close()
                }
                previous.publish(State.QUEUED); publish(previous)
                reservation.bind(previous, scope)
            } else {
                val fallback = if (old != null && sameInput) old.snapshotName else null
                reservation.intent = CopyIntent(request = request, fallbackSnapshot = fallback)
                val row = runtime.store.admit(reservation.id, request.family, resource(request.destination), Pool.COPY,
                    gson.toJson(reservation.intent), State.QUEUED, false)
                reservation.sequence = row.sequence
                tickets[reservation.id] = reservation
                reservation.publish(State.QUEUED); publish(reservation)
            }
            requestPump()
        } catch (error: Exception) {
            inputReferences.remove(reservation.id)?.close()
            synchronized(lock) {
                if (families[reservation.family] === reservation) families.remove(reservation.family)
            }
            reservation.publish(State.FAILED, error.message); publish(reservation)
            Timber.tag("ContentQueue").e(error, "Copy admission failed")
        }
    }

    private fun restore() {
        var after = 0L
        val through = runtime.store.highWaterMark()
        while (true) {
            val rows = runtime.store.activePage(PREFIX, after, through, 32)
            if (rows.isEmpty()) break
            for (row in rows) { after = row.sequence; restoreRow(row) }
        }
        // Never inflate all completed history or probe every gallery URI on cold startup.
        runtime.store.recentTasks(PREFIX, Pool.COPY, State.SUCCEEDED, WARM_RECEIPTS).asReversed().forEach(::restoreRow)
    }

    private fun restoreRow(row: ContentQueueStore.Task) {
        if (row.state == State.CANCELLED || row.payload.isEmpty()) return
        try {
            val intent = gson.fromJson(row.payload, CopyIntent::class.java)
            require(intent.version == 1) { "Unsupported copy payload" }
            intent.request.validate()
            intent.snapshotName?.let(::snapshot)
            intent.fallbackSnapshot?.let(::snapshot)
            val ticket = CopyTicket(row.id, intent.request.family)
            ticket.intent = intent; ticket.sequence = row.sequence; ticket.signal = { requestPump() }
            if (row.state == State.PAUSED) ticket.pause()
            tickets[row.id] = ticket
            ticket.publish(row.state, row.error)
            if (row.state == State.SUCCEEDED) {
                if (output.available(intent)) saved.value += savedKey(intent.request)
                cleanup(intent, row.id)
            } else {
                if (intent.snapshotName?.let(::snapshot)?.isFile != true) {
                    inputReferences[row.id] = CopyInputReferences.reserve(File(intent.request.sourcePath))
                }
                synchronized(lock) {
                    val pending = families[ticket.family]
                    // An explicit new retry must not first observe the restored FAILED terminal state.
                    if (!(row.state == State.FAILED && pending != null && pending.intent == null)) {
                        families[ticket.family] = ticket
                        if (pending != null && pending.intent == null) {
                            pending.bind(ticket, scope)
                            inputReferences.remove(pending.id)?.close()
                        }
                    }
                }
            }
            publish(ticket)
        } catch (error: Exception) {
            Timber.tag("ContentQueue").e(error, "Unreadable copy intent %s retained", row.id)
        }
    }

    private fun pump() {
        for (ticket in tickets.values.toList()) {
            if (workers.containsKey(ticket.id)) continue
            val row = runtime.store.get(ticket.id) ?: continue
            if (ticket.isCancelled() && row.state !in setOf(State.SUCCEEDED, State.CANCELLED)) {
                runtime.store.cancel(row.id)
                ticket.publish(State.CANCELLED); publish(ticket)
                ticket.intent?.let { cleanup(it, ticket.id) }
                runtime.changed()
                continue
            }
            if (row.state != State.QUEUED) continue
            if (ticket.isPaused()) {
                runtime.store.pause(row.id); runtime.withdraw(row.id)
                ticket.publish(State.PAUSED); publish(ticket); continue
            }
            val lease = runtime.claim(row.id, Pool.COPY, capacity().coerceIn(1, 5)) ?: continue
            ticket.publish(State.RUNNING); publish(ticket)
            workers[ticket.id] = scope.launch(start = CoroutineStart.LAZY) { run(ticket, lease) }.also { it.start() }
        }
    }

    private suspend fun run(ticket: CopyTicket, lease: ContentQueueStore.Task) {
        var intent = requireNotNull(ticket.intent)
        var committed = false
        var failure: Throwable? = null
        try {
            ticket.checkCancellation()
            intent = prepare(intent, ticket)
            checkpoint(lease, intent)
            inputReferences.remove(ticket.id)?.close()
            intent.fallbackSnapshot?.takeIf { it != intent.snapshotName }?.let { snapshot(it).delete() }
            ticket.intent = intent
            ticket.checkCancellation()
            ticket.beginWrite() // accepted user cancellation is impossible after this boundary
            withContext(NonCancellable + Dispatchers.IO) {
                val receipt = intent.receiptUri
                if (receipt != null && output.matches(receipt, requireNotNull(intent.sourceHash), intent.sourceBytes)) {
                    output.finishRecovered(receipt, intent.request.destination)
                } else {
                    val handle = receipt?.let { output.resume(it, intent.request.destination) }
                        ?: output.open(intent.request.destination)
                    if (handle == null) {
                        intent = intent.copy(skipped = true)
                    } else {
                        try {
                            intent = intent.copy(receiptUri = handle.uri.toString())
                            checkpoint(lease, intent) // recovery knows the exact output before bytes or finishWrite
                            handle.stream.use { stream -> snapshot(intent.snapshotName!!).inputStream().use { it.copyTo(stream) } }
                            handle.onFinish()
                        } catch (error: Throwable) {
                            try { handle.onAbort() } catch (abort: Throwable) { if (abort !== error) error.addSuppressed(abort) }
                            throw error
                        }
                    }
                }
                committed = true
                intent = intent.copy(outputCommitted = true)
                checkpoint(lease, intent)
                check(runtime.store.markCommitted(lease.id, lease.attempt)) { "Stale copy completion" }
            }
        } catch (error: Throwable) { failure = error }
        finally {
            withContext(NonCancellable + Dispatchers.IO) {
                try {
                    if (failure is CancellationException && !committed) {
                        if (ticket.isPaused()) runtime.store.pause(lease.id) else runtime.store.cancel(lease.id)
                    }
                    check(runtime.store.finish(lease.id, lease.attempt, committed, failure?.message)) { "Stale copy acknowledgement" }
                    val state = runtime.store.get(lease.id).state
                    ticket.intent = intent
                    if (intent.snapshotName?.let(::snapshot)?.isFile == true) inputReferences.remove(ticket.id)?.close()
                    if (state == State.SUCCEEDED || state == State.CANCELLED) cleanup(intent, ticket.id)
                    commands.send {
                        workers.remove(ticket.id)
                        if (state == State.FAILED) ticket.retry() // no live writer remains; failed jobs may be cancelled
                        if (state == State.SUCCEEDED) saved.value += savedKey(intent.request)
                        ticket.publish(state, failure?.message, if (state == State.SUCCEEDED) 100 else 0)
                        publish(ticket)
                        trimWarmProjection()
                        if (state == State.SUCCEEDED) {
                            try { succeeded(intent.request) }
                            catch (error: Exception) { Timber.tag("ContentQueue").w(error, "Copy completion notification failed") }
                        }
                        requestPump()
                    }
                    runtime.changed()
                } catch (error: Throwable) {
                    // Physical work is over, but retain the reservation until recovery if acknowledgement fails.
                    Timber.tag("ContentQueue").e(error, "Copy acknowledgement failed %s", ticket.id)
                }
            }
        }
    }

    private suspend fun prepare(intent: CopyIntent, ticket: CopyTicket): CopyIntent {
        sources.mkdirs()
        val name = intent.snapshotName ?: ticket.id.substringAfterLast(':') + ".source"
        val target = snapshot(name)
        if (target.isFile) {
            if (target.length() <= 0) throw IOException("Stored copy input is empty")
            val hash = hash(target)
            if (intent.sourceHash != null && intent.sourceHash != hash) throw IOException("Stored copy input changed")
            return intent.copy(snapshotName = name, sourceHash = hash, sourceBytes = target.length())
        }
        val original = File(intent.request.sourcePath)
        val input = if (original.isFile) original else intent.fallbackSnapshot?.let(::snapshot)
            ?: throw IOException("Copy input no longer exists; regenerate the image and retry")
        if (!input.isFile || input.length() <= 0) throw IOException("Copy input is missing or empty")
        val expectedSize = input.length()
        val expectedModified = input.lastModified()
        val temporary = File(sources, "$name.copying")
        try {
            val context = currentCoroutineContext()
            runInterruptible {
                FileInputStream(input).use { from -> FileOutputStream(temporary).use { to ->
                    val buffer = ByteArray(32 * 1024)
                    while (true) {
                        ticket.checkCancellation(); context.ensureActive()
                        val count = from.read(buffer)
                        if (count < 0) break
                        to.write(buffer, 0, count)
                    }
                    to.fd.sync()
                } }
            }
            ticket.checkCancellation()
            if (temporary.length() != expectedSize || input.length() != expectedSize || input.lastModified() != expectedModified) {
                throw IOException("Copy input changed while being preserved; retry the current image")
            }
            if (!temporary.renameTo(target)) throw IOException("Cannot preserve copy input")
            return intent.copy(snapshotName = name, sourceHash = hash(target), sourceBytes = target.length())
        } finally { temporary.delete() }
    }

    private fun checkpoint(lease: ContentQueueStore.Task, intent: CopyIntent) {
        check(runtime.store.checkpoint(lease.id, lease.attempt, gson.toJson(intent))) { "Stale copy checkpoint" }
    }
    private fun snapshot(name: String): File {
        require(name == File(name).name && !name.contains('\\') && name !in setOf(".", ".."))
        return File(sources, name)
    }
    private fun cleanup(intent: CopyIntent, id: String) {
        inputReferences.remove(id)?.close()
        snapshot(intent.snapshotName ?: id.substringAfterLast(':') + ".source").delete()
        File(sources, id.substringAfterLast(':') + ".source.copying").delete()
        intent.fallbackSnapshot?.takeIf { it != intent.snapshotName }?.let { snapshot(it).delete() }
    }
    private fun trimWarmProjection() {
        val old = tickets.values.filter { it.status.value.state in setOf(State.SUCCEEDED, State.CANCELLED) }
            .sortedByDescending { it.intent?.request?.submittedAt ?: 0 }.drop(WARM_RECEIPTS)
        if (old.isEmpty()) return
        for (ticket in old) {
            tickets.remove(ticket.id, ticket)
            synchronized(lock) { if (families[ticket.family] === ticket) families.remove(ticket.family) }
        }
        mutable.value = mutable.value - old.map { it.id }.toSet()
    }
    private fun publish(ticket: CopyTicket) { mutable.value += ticket.id to ticket.status.value }
    private fun resource(destination: CopyDestination): String {
        // Two source versions may resolve to the same output: serialize before Rename/Replace probing.
        val identity = gson.toJson(destination.copy(policy = "", mime = ""))
        return "copy-output:" + MessageDigest.getInstance("SHA-256").digest(identity.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it.toInt() and 255) }
    }
    companion object {
        const val PREFIX = "copy:"
        private const val WARM_RECEIPTS = 128
        private val TERMINAL = setOf(State.SUCCEEDED, State.CANCELLED, State.FAILED)
        fun savedKey(request: CopyRequest) = request.kind + ":" + request.sourcePath
        internal fun hash(file: File): String {
            val digest = MessageDigest.getInstance("SHA-256")
            file.inputStream().use { input ->
                val buffer = ByteArray(32 * 1024)
                while (true) { val count = input.read(buffer); if (count < 0) break; digest.update(buffer, 0, count) }
            }
            return digest.digest().joinToString("") { "%02x".format(it.toInt() and 255) }
        }
    }
}
