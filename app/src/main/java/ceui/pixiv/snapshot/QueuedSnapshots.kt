package ceui.pixiv.snapshot

import android.content.Context
import ceui.lisa.activities.Shaft
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.IOException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

/** Startup recovery and foreground/offline generation share one typed parent owner. */
class QueuedSnapshots internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    private val transfers: SnapshotAssetTransfers = SnapshotGenerator.transfers,
    private val allowed: () -> Boolean = { ceui.lisa.utils.DownloadLimitTypeUtil.autoStartAllowed() },
    private val generate: suspend (SnapshotRequest, Illust, suspend (String) -> Unit) -> Unit = { request, illust, progress ->
        if (request.automatic) SnapshotGenerator.generateAutoRaw(app, illust, request.original, progress)
        else SnapshotGenerator.generateRaw(app, illust, request.comments, request.original, request.fresh, progress)
    },
) : ContentPlanHandler {
    override val kind = "snapshot"
    private val gson = Gson()
    private val manual = ConcurrentHashMap.newKeySet<String>()
    init { queue.register(this) }
    fun enqueue(illust: Illust, comments: Boolean, original: Boolean, fresh: Boolean, automatic: Boolean): ContentPlanTicket {
        val family = "snapshot:${if (automatic) "auto" else "manual"}:${illust.id}:$original:${comments && !automatic}"
        if (!automatic) manual.add(family)
        return queue.submit(family, kind) {
            gson.toJson(SnapshotRequest(snapshotId = UUID.randomUUID().toString(), createdAt = System.currentTimeMillis(),
                illustJson = gson.toJson(illust), comments = comments && !automatic, original = original, fresh = fresh, automatic = automatic))
        }
    }
    suspend fun create(illust: Illust, comments: Boolean, original: Boolean, fresh: Boolean,
        automatic: Boolean, progress: suspend (String) -> Unit): String {
        val ticket = enqueue(illust, comments, original, fresh, automatic)
        val end = ticket.status.first { progress(it.phase); it.state in ContentPlanTicket.OBSERVABLE_END }
        when (end.state) {
            State.CANCELLED, State.PAUSED -> throw CancellationException("Snapshot generation stopped")
            State.SUCCEEDED -> Unit
            else -> throw IOException(end.error ?: "Snapshot generation failed")
        }
        return requireNotNull(gson.fromJson(queue.store.get(ticket.id).checkpoint, SnapshotCheckpoint::class.java).manifestJson)
    }
    override fun canRun(plan: ContentPlanStore.Plan): Boolean = manual.contains(plan.family) || allowed() || runCatching {
        gson.fromJson(plan.checkpoint, SnapshotCheckpoint::class.java)?.manifestJson != null
    }.getOrDefault(false)
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, SnapshotRequest::class.java).also {
        require(it.version == 1 && UUID.fromString(it.snapshotId).toString() == it.snapshotId && it.createdAt > 0)
        require(!it.automatic || !it.comments)
    }
    override suspend fun execute(context: ContentPlanContext) {
        val request = request(context.plan)
        val illust = gson.fromJson(request.illustJson, Illust::class.java)
        require(illust.id > 0)
        ContentResourceLeases.write("snapshot:${request.snapshotId}") {
            val session = SnapshotPlanSession(context, request, app, phases, transfers)
            session.sweepInterruptedWrites()
            if (!session.recoverManifest()) {
                if (!manual.contains(context.plan.family) && !allowed()) { session.forgetManifest(); context.yieldForPolicy() }
                withContext(SnapshotPlanSession.current.asContextElement(session)) {
                    generate(request, illust) { context.progress(it) }
                }
            }
        }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state != State.CANCELLED) return
        val request = request(plan)
        ContentResourceLeases.write("snapshot:${request.snapshotId}") {
        phases.run(plan.id, "snapshot-cleanup", "snapshot-manifest:${request.snapshotId}", Pool.COPY) {
            val directory = SnapshotRepository.dir(app, request.snapshotId)
            // A complete manifest is already an externally visible snapshot, never a retry input.
            val manifest = File(directory, if (request.automatic) AUTO_SNAPSHOT_MANIFEST else SNAPSHOT_MANIFEST)
            if (!manifest.isFile && directory.exists() && !directory.deleteRecursively()) throw IOException("Cannot remove cancelled snapshot input")
        }
        }
    }
    companion object {
        @Volatile private var instance: Pair<Context, QueuedSnapshots>? = null
        @JvmStatic fun get(context: Context): QueuedSnapshots {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedSnapshots(app).also { instance = app to it }
            }
        }
        @JvmStatic fun refreshEligibility() { instance?.second?.queue?.refreshEligibility() }
        @JvmStatic fun onNetworkGateClosed() { instance?.second?.let { it.manual.clear(); it.queue.refreshEligibility() } }
    }
}
