package ceui.pixiv.download.queue

import android.content.Context
import ceui.pixiv.download.queue.ContentQueueStore.State
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import timber.log.Timber

/** Process-owned coordinator. Only child stages claim physical pools; UI lifetimes only observe. */
@OptIn(ExperimentalCoroutinesApi::class)
class ContentPlanQueue(
    private val runtime: ContentQueueRuntime,
    private val width: Int = 2,
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) {
    val store = ContentPlanStore(runtime.store)
    private val lock = Any()
    private val families = mutableMapOf<String, ContentPlanTicket>()
    private val tickets = ConcurrentHashMap<String, ContentPlanTicket>()
    private val handlers = ConcurrentHashMap<String, ContentPlanHandler>()
    private val workers = mutableMapOf<String, Job>()
    private val cleaned = ConcurrentHashMap.newKeySet<String>()
    private val commands = Channel<suspend () -> Unit>(Channel.UNLIMITED)
    private val pumping = AtomicBoolean()
    private val wake = Runnable { requestPump() }
    private val mutable = MutableStateFlow<Map<String, ContentPlanStatus>>(emptyMap())
    val states: StateFlow<Map<String, ContentPlanStatus>> = mutable
    val ready = CompletableDeferred<Unit>()
    @Volatile private var initializationFailure: Throwable? = null

    init {
        runtime.addListener(wake)
        scope.launch(start = CoroutineStart.ATOMIC) {
            try {
                runtime.initialize()
                var after = 0L
                val through = store.highWaterMark()
                while (true) {
                    currentCoroutineContext().ensureActive()
                    val rows = store.activePage(after, through, 32)
                    if (rows.isEmpty()) break
                    for (plan in rows) { after = plan.sequence; restore(plan, false) }
                }
                mutable.value = tickets.values.associate { it.id to it.status.value }
                ready.complete(Unit); requestPump()
                for (command in commands) {
                    try { command() }
                    catch (error: Exception) {
                        if (error is CancellationException && !currentCoroutineContext().isActive) throw error
                        Timber.tag("ContentQueue").e(error, "Parent queue command failed")
                    }
                }
            } catch (error: Throwable) {
                ready.completeExceptionally(error)
                if (error is CancellationException) throw error
                initializationFailure = error
                synchronized(lock) { families.values.toList() }.forEach { it.publish(State.FAILED, error.message); publish(it) }
                Timber.tag("ContentQueue").e(error, "Parent queue initialization failed")
            }
        }
    }

    fun register(handler: ContentPlanHandler) {
        require(handler.kind.isNotBlank())
        require(handler.kind.endsWith("-group") == handler.isGroup) { "Group kinds must declare their separate planning lane" }
        val old = handlers.putIfAbsent(handler.kind, handler)
        check(old == null || old === handler) { "Parent kind already has an owner: ${handler.kind}" }
        if (old == null) commands.trySend {
            for (state in listOf(State.SUCCEEDED, State.CANCELLED)) {
                for (plan in store.recent(handler.kind, state, 128).asReversed()) {
                    val settled = settleCancelled(handler, plan)
                    if (settled.state in setOf(State.SUCCEEDED, State.CANCELLED)) cleanup(handler, settled, settled.state)
                    if (!tickets.containsKey(plan.id)) restore(settled)
                    else if (settled.state != plan.state) tickets[plan.id]?.let {
                        it.reset(settled.state); it.publish(settled.state, settled.error); publish(it)
                    }
                }
            }
            trim()
        }
        requestPump()
    }

    /** The immutable request factory executes on IO. Identical stopped intentions can be resumed. */
    fun submit(family: String, kind: String, resumeStopped: Boolean = true, replaceActive: Boolean = false,
               reuseSucceeded: Boolean = false,
               initialState: State = State.QUEUED,
               reusePersisted: Boolean = false,
               request: suspend () -> String): ContentPlanTicket {
        require(family.isNotBlank() && kind.isNotBlank())
        require(initialState in setOf(State.QUEUED, State.PAUSED, State.FAILED))
        val owner = executing.get()
        check(owner == null || (owner.startsWith("group:") && handlers[kind]?.isGroup == false)) {
            "Only a group coordinator may submit non-group children"
        }
        val ticket = synchronized(lock) {
            val existing = families[family]
            if (existing != null && existing.status.value.state !in ContentPlanTicket.OBSERVABLE_END && existing.stop() != ContentPlanTicket.CANCEL) {
                if (!replaceActive) return existing
                existing.cancel() // a commit may already own the gate; the durable successor still waits
            }
            ContentPlanTicket("plan:" + UUID.randomUUID(), family, kind).also { it.reset(initialState); families[family] = it }
        }
        initializationFailure?.let { ticket.publish(State.FAILED, it.message); publish(ticket); return ticket }
        ticket.signal = { requestPump() }
        commands.trySend { admit(ticket, resumeStopped, replaceActive, reuseSucceeded, reusePersisted, initialState, request) }
        return ticket
    }
    fun ticket(id: String): ContentPlanTicket? = tickets[id]
    fun latest(family: String): ContentPlanTicket? = synchronized(lock) { families[family] }
    fun refreshEligibility() { requestPump() }
    fun resume(id: String) { commands.trySend {
        val ticket = tickets[id] ?: return@trySend
        if (workers.containsKey(id)) return@trySend
        val plan = store.get(id) ?: return@trySend
        if (plan.state == State.QUEUED) { handlers[plan.kind]?.onUserResume(plan); requestPump(); return@trySend }
        if (ticket.status.value.state !in setOf(State.PAUSED, State.FAILED) || !ticket.reopen()) return@trySend
        handlers[plan.kind]?.onUserResume(plan)
        if (store.resume(id)) { ticket.publish(State.QUEUED); publish(ticket); requestPump() }
    } }
    fun finishByUser(ticket: ContentPlanTicket) { commands.trySend {
        val plan = store.get(ticket.id) ?: return@trySend
        check(handlers[plan.kind]?.isGroup == true) { "Only groups can finish with already fetched items" }
        store.requestFinish(plan.id)
        requestPump()
    } }
    suspend fun shutdown() {
        runtime.removeListener(wake)
        scope.coroutineContext[Job]?.cancelAndJoin(); commands.close()
        synchronized(Companion) { if (singleton?.second === this) singleton = null }
    }

    private suspend fun admit(reservation: ContentPlanTicket, resumeStopped: Boolean, successor: Boolean,
                              reuseSucceeded: Boolean, reusePersisted: Boolean, initialState: State, factory: suspend () -> String) {
        try {
            if (reservation.stop() == ContentPlanTicket.CANCEL) throw CancellationException("Parent withdrawn before admission")
            check(handlers.containsKey(reservation.kind)) { "No worker registered for ${reservation.kind}" }
            val fulfilled = when {
                reusePersisted -> store.latest(reservation.family, reservation.kind)
                reuseSucceeded -> store.succeeded(reservation.family, reservation.kind)
                else -> null
            }
            if (fulfilled != null) {
                if (!tickets.containsKey(fulfilled.id)) restore(fulfilled)
                val canonical = requireNotNull(tickets[fulfilled.id])
                synchronized(lock) { if (families[reservation.family] === reservation) families[reservation.family] = canonical }
                reservation.bind(canonical, scope)
                return
            }
            val request = withContext(executing.asContextElement("admission:" + reservation.id)) { factory() }
            // A new user intention can queue while the cancelled predecessor still closes its body.
            tickets.values.filter { it.family == reservation.family && it.stop() == ContentPlanTicket.CANCEL }
                .forEach { store.cancel(it.id) }
            var plan = store.admit(reservation.id, reservation.family, reservation.kind, request, successor, initialState)
            if (plan.kind != reservation.kind || plan.request != request) {
                if (plan.state !in setOf(State.PAUSED, State.FAILED)) error("A different parent intention is already active")
                store.cancel(plan.id)
                handlers[plan.kind]?.cleanup(plan, State.CANCELLED)
                tickets[plan.id]?.also { it.reset(State.CANCELLED); it.publish(State.CANCELLED); publish(it) }
                plan = store.admit(reservation.id, reservation.family, reservation.kind, request, false, initialState)
            }
            val canonical = tickets[plan.id] ?: (if (plan.id == reservation.id) reservation
                else ContentPlanTicket(plan.id, plan.family, plan.kind).also { it.reset(plan.state) })
                .also { tickets[plan.id] = it }
            if (reservation.stop() == ContentPlanTicket.PAUSE) {
                if (canonical !== reservation) canonical.pause()
                store.pause(plan.id); plan = requireNotNull(store.get(plan.id))
            }
            if (resumeStopped && reservation.stop() != ContentPlanTicket.PAUSE &&
                !(plan.id == reservation.id && initialState != State.QUEUED) && plan.state in setOf(State.PAUSED, State.FAILED)) {
                if (!canonical.reopen()) { store.cancel(plan.id); throw CancellationException("Parent withdrawn before retry") }
                check(store.resume(plan.id)); plan = requireNotNull(store.get(plan.id))
            }
            canonical.admitted = true; canonical.sequence = plan.sequence
            canonical.signal = { requestPump() }
            canonical.publish(plan.state, plan.error); publish(canonical)
            synchronized(lock) { families[plan.family] = canonical }
            if (canonical !== reservation) reservation.bind(canonical, scope)
            requestPump()
        } catch (error: Exception) {
            reservation.publish(if (error is CancellationException) State.CANCELLED else State.FAILED, error.message)
            publish(reservation)
            Timber.tag("ContentQueue").w(error, "Parent admission failed")
        }
    }
    private fun restore(plan: ContentPlanStore.Plan, notify: Boolean = true) {
        val ticket = ContentPlanTicket(plan.id, plan.family, plan.kind)
        ticket.admitted = true; ticket.sequence = plan.sequence; ticket.reset(plan.state)
        ticket.publish(plan.state, plan.error); ticket.signal = { requestPump() }
        tickets[plan.id] = ticket
        synchronized(lock) {
            val pending = families[plan.family]
            // Do not bind a new explicit retry to an already-terminal FAILED observation.
            if (pending == null || (pending.admitted && pending.sequence < plan.sequence)) families[plan.family] = ticket
        }
        if (notify) publish(ticket)
    }
    private fun requestPump() {
        if (pumping.compareAndSet(false, true)) {
            if (commands.trySend { pumping.set(false); pump() }.isFailure) pumping.set(false)
        }
    }
    private suspend fun pump() {
        for (ticket in tickets.values.sortedBy { it.sequence }) {
            currentCoroutineContext().ensureActive()
            var plan = store.get(ticket.id) ?: continue
            when (store.parentState(plan.family)) {
                State.CANCEL_REQUESTED, State.CANCELLED -> ticket.cancel()
                State.PAUSED, State.PAUSE_REQUESTED, State.FAILED -> ticket.pause()
                else -> Unit
            }
            when (ticket.stop()) {
                ContentPlanTicket.CANCEL -> store.cancel(ticket.id)
                ContentPlanTicket.PAUSE -> store.pause(ticket.id)
                ContentPlanTicket.YIELD -> store.yieldForPolicy(ticket.id)
            }
            plan = store.get(ticket.id) ?: continue
            val worker = workers[ticket.id]
            if (worker != null) {
                if (plan.state == State.RUNNING && handlers[plan.kind]?.let { !eligible(it, plan) } == true && ticket.yieldForPolicy()) {
                    store.yieldForPolicy(ticket.id)
                    plan = requireNotNull(store.get(ticket.id))
                }
                if (plan.state in setOf(State.CANCEL_REQUESTED, State.PAUSE_REQUESTED, State.YIELD_REQUESTED)) worker.cancel()
                ticket.publish(plan.state, plan.error); publish(ticket)
                continue
            }
            if (plan.state in setOf(State.CANCELLED, State.PAUSED, State.FAILED)) {
                if (plan.state == State.CANCELLED) handlers[plan.kind]?.let {
                    val settled = settleCancelled(it, plan)
                    if (settled.state != plan.state) ticket.reset(settled.state)
                    plan = settled
                    if (plan.state in setOf(State.CANCELLED, State.SUCCEEDED)) cleanup(it, plan, plan.state)
                }
                ticket.finished(plan.state); ticket.publish(plan.state, plan.error); publish(ticket)
                continue
            }
            val handler = handlers[plan.kind] ?: continue
            if (plan.state != State.QUEUED) continue
            if (!eligible(handler, plan)) continue
            if (workers.keys.any { tickets[it]?.family == plan.family }) continue
            val lease = store.claim(plan.id, width.coerceIn(1, 8)) ?: continue
            ticket.publish(State.RUNNING); publish(ticket)
            workers[ticket.id] = scope.launch(start = CoroutineStart.LAZY) { run(ticket, lease, handler) }.also { it.start() }
        }
    }
    private suspend fun run(ticket: ContentPlanTicket, lease: ContentPlanStore.Plan, handler: ContentPlanHandler) {
        val context = ContentPlanContext(lease, ticket, store) { publish(ticket) }
        var failure: Throwable? = null
        try {
            context.checkStop()
            withContext(executing.asContextElement(if (handler.isGroup) "group:" + lease.id else lease.id) +
                ContentPriority(handler.priority(lease))) { handler.execute(context) }
            check(context.outputCommitted) { "Parent worker returned without committing an output receipt" }
        } catch (error: Throwable) { failure = error }
        finally {
            withContext(NonCancellable + Dispatchers.IO) {
                try {
                    if (!context.outputCommitted) {
                        when (ticket.stop()) {
                            ContentPlanTicket.PAUSE -> store.pause(lease.id)
                            ContentPlanTicket.CANCEL -> store.cancel(lease.id)
                            ContentPlanTicket.YIELD -> store.yieldForPolicy(lease.id)
                            else -> if (failure is CancellationException) store.pause(lease.id)
                        }
                    }
                    var before = requireNotNull(store.get(lease.id))
                    if (context.outputCommitted && before.state != State.COMMITTED) {
                        check(store.markCommitted(lease.id, lease.attempt)) { "Parent output receipt could not commit" }
                        before = requireNotNull(store.get(lease.id))
                    }
                    val terminal = if (context.outputCommitted) State.SUCCEEDED
                        else if (before.state == State.CANCEL_REQUESTED) State.CANCELLED else null
                    if (terminal != null) cleanup(handler, before, terminal)
                    check(store.finish(lease.id, lease.attempt, context.outputCommitted, failure?.message)) { "Stale parent acknowledgement" }
                    val finished = requireNotNull(store.get(lease.id))
                    commands.send {
                        workers.remove(lease.id)
                        ticket.finished(finished.state)
                        when (ticket.stop()) {
                            ContentPlanTicket.CANCEL -> store.cancel(lease.id)
                            ContentPlanTicket.PAUSE -> store.pause(lease.id)
                        }
                        val latest = requireNotNull(store.get(lease.id))
                        if (latest.state == State.CANCELLED) cleanup(handler, latest, latest.state)
                        ticket.finished(latest.state); ticket.publish(latest.state, latest.error); publish(ticket)
                        trim()
                        requestPump()
                    }
                    runtime.changed()
                } catch (error: Throwable) {
                    Timber.tag("ContentQueue").e(error, "Parent acknowledgement failed; reservation retained")
                }
            }
        }
    }
    private fun eligible(handler: ContentPlanHandler, plan: ContentPlanStore.Plan): Boolean = try {
        handler.canRun(plan)
    } catch (error: Exception) {
        tickets[plan.id]?.let { it.publish(plan.state, error.message); publish(it) }
        Timber.tag("ContentQueue").w(error, "Parent eligibility unavailable %s", plan.id)
        false
    }
    private suspend fun settleCancelled(handler: ContentPlanHandler, plan: ContentPlanStore.Plan): ContentPlanStore.Plan {
        if (plan.state != State.CANCELLED || plan.checkpoint == "{}") return plan
        try {
            if (withContext(executing.asContextElement("reconcile:" + plan.id)) { handler.reconcile(plan) })
                check(store.reconcileCancelled(plan.id, plan.checkpoint, true, null))
        } catch (error: Exception) {
            if (error is CancellationException) throw error
            store.reconcileCancelled(plan.id, plan.checkpoint, false, "已写入产物校验失败，请恢复权限后重试：${error.message}")
            Timber.tag("ContentQueue").w(error, "Cancelled output reconciliation retained its inputs")
        }
        return requireNotNull(store.get(plan.id))
    }
    private fun publish(ticket: ContentPlanTicket) { mutable.update { it + (ticket.id to ticket.status.value) } }
    private suspend fun cleanup(handler: ContentPlanHandler, plan: ContentPlanStore.Plan, state: State) {
        if (!cleaned.add(plan.id)) return
        try { withContext(executing.asContextElement("cleanup:" + plan.id)) { handler.cleanup(plan, state) } }
        catch (error: Exception) { cleaned.remove(plan.id); Timber.tag("ContentQueue").w(error, "Parent input cleanup failed") }
    }
    private fun trim() {
        val old = tickets.values.filter { it.status.value.state in setOf(State.SUCCEEDED, State.CANCELLED) }
            .sortedByDescending { it.sequence }.drop(128)
        for (ticket in old) {
            tickets.remove(ticket.id, ticket); cleaned.remove(ticket.id)
            synchronized(lock) { if (families[ticket.family] === ticket) families.remove(ticket.family) }
        }
        if (old.isNotEmpty()) mutable.update { it - old.map { ticket -> ticket.id }.toSet() }
    }

    companion object {
        private val executing = ThreadLocal<String?>()
        @Volatile private var singleton: Pair<Context, ContentPlanQueue>? = null
        @JvmStatic fun get(context: Context): ContentPlanQueue {
            val app = context.applicationContext
            return singleton?.takeIf { it.first === app }?.second ?: synchronized(this) {
                singleton?.takeIf { it.first === app }?.second ?: ContentPlanQueue(ContentQueueRuntime.get(app)).also { singleton = app to it }
            }
        }
    }
}
