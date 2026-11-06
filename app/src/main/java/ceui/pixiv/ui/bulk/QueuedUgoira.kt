package ceui.pixiv.ui.bulk

import android.content.Context
import androidx.annotation.Keep
import ceui.lisa.activities.Shaft
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.copy.AndroidCopyOutput
import ceui.pixiv.download.copy.CopyDestination
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.io.FilterOutputStream
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

@Keep
data class UgoiraSaveRequest(val version: Int = 1, val illustJson: String, val preferVideo: Boolean,
    val gif: CopyDestination, val video: CopyDestination)
@Keep
data class UgoiraSaveDomain(val version: Int = 1, val video: Boolean, val metadataJson: String? = null)

internal class UgoiraOutputSession(val context: ContentPlanContext, val directory: File,
    val request: UgoiraSaveRequest, private val phases: ContentPhaseRunner) {
    private var savedMetadata: String? = runCatching {
        val checkpoint = Gson().fromJson(context.checkpoint, ArtifactCheckpoint::class.java)
        Gson().fromJson(checkpoint.domain, UgoiraSaveDomain::class.java)?.metadataJson
    }.getOrNull()
    fun metadata(): ceui.lisa.models.GifResponse? = savedMetadata?.let { Gson().fromJson(it, ceui.lisa.models.GifResponse::class.java) }
    suspend fun preserveMetadata(metadata: ceui.lisa.models.GifResponse) {
        if (savedMetadata != null) return
        savedMetadata = Gson().toJson(metadata)
        context.checkpoint(Gson().toJson(ArtifactCheckpoint(domain = Gson().toJson(UgoiraSaveDomain(video = false, metadataJson = savedMetadata)))))
    }
    suspend fun capture(video: Boolean, pool: Pool, writer: suspend (OutputStream) -> Unit) =
        phases.run(context.plan.id, "ugoira-render", "ugoira-input:${context.plan.id}", pool) {
            directory.mkdirs()
            val source = File(directory, "artifact"); val temporary = File(directory, "artifact.building")
            try {
                FileOutputStream(temporary).use { output ->
                    writer(object : FilterOutputStream(output) { override fun close() { flush() } }); output.fd.sync()
                }
                currentCoroutineContext().ensureActive(); context.checkStop()
                if (!temporary.renameTo(source)) throw IOException("Cannot preserve generated ugoira")
            } finally { temporary.delete() }
            val gson = Gson()
            context.checkpoint(gson.toJson(ArtifactCheckpoint(bytes = source.length(), sha256 = ArtifactPublisher.digest(source),
                domain = gson.toJson(UgoiraSaveDomain(video = video, metadataJson = savedMetadata)))))
        }
    companion object { val current = ThreadLocal<UgoiraOutputSession?>() }
}

/** All new save entries use the same process-owned, replayable parent and final URI receipt. */
class QueuedUgoira internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    output: ceui.pixiv.download.copy.CopyOutput = AndroidCopyOutput(app) { DownloadsRegistry.downloads },
    private val allowed: () -> Boolean = { ceui.lisa.utils.DownloadLimitTypeUtil.autoStartAllowed() },
    private val prepare: suspend (Illust, Boolean) -> Unit = { illust, video ->
        if (video) try { UgoiraEngine.loadPlayableFrames(illust) }
        catch (cancelled: CancellationException) { throw cancelled }
        catch (failed: Exception) { timber.log.Timber.w(failed, "Playback preparation failed; retrying ugoira source") }
    },
    private val generate: suspend (Illust, (UgoiraPhase) -> Unit) -> Unit = { illust, progress -> downloadUgoiraRaw(illust, onPhase = progress) },
    private val record: (Illust, android.net.Uri, Boolean, String) -> Unit = ceui.pixiv.download.UgoiraDownloadRecord::recordStrict,
) : ContentPlanHandler {
    override val kind = "ugoira-save"
    private val gson = Gson()
    private val publisher = ArtifactPublisher(phases, output)
    private val manual = ConcurrentHashMap.newKeySet<String>()
    init { queue.register(this) }
    @JvmOverloads fun enqueue(illust: Illust, family: String = "ugoira-save:${illust.id}", once: Boolean = false): ContentPlanTicket {
        manual.add(family)
        return queue.submit(family, kind, reuseSucceeded = once) {
        fun destination(video: Boolean): CopyDestination {
            val item = DownloadItems.ugoira(illust, video)
            val bucket = DownloadsRegistry.store.loadOrFallback().resolve(item.bucket)
            val path = DownloadsRegistry.downloads.resolvePath(item)
            return CopyDestination.capture(bucket.storage, path, item.mime, bucket.overwrite)
        }
        gson.toJson(UgoiraSaveRequest(illustJson = gson.toJson(illust), preferVideo = Shaft.sSettings.isUgoiraSaveAsMp4(),
            gif = destination(false), video = destination(true)))
        }
    }
    suspend fun save(illust: Illust, phase: (UgoiraPhase) -> Unit = {}) {
        await(enqueue(illust), phase)
    }
    suspend fun saveBatch(rowId: Long, illust: Illust, phase: (UgoiraPhase) -> Unit) {
        await(enqueue(illust, batchFamily(rowId), true), phase)
    }
    private suspend fun await(ticket: ContentPlanTicket, phase: (UgoiraPhase) -> Unit) {
        val end = ticket.status.first {
            runCatching { phase(UgoiraPhase.valueOf(it.phase)) }
            it.state in ContentPlanTicket.OBSERVABLE_END
        }
        when (end.state) {
            State.SUCCEEDED -> Unit
            State.CANCELLED, State.PAUSED -> throw CancellationException("Ugoira save stopped")
            else -> throw IOException(end.error ?: "Ugoira save failed")
        }
    }
    override fun canRun(plan: ContentPlanStore.Plan): Boolean {
        if (manual.contains(plan.family) || allowed()) return true
        return runCatching {
            val checkpoint = gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java)
            checkpoint.version == 1 && checkpoint.bytes > 0 && File(directory(plan.id), "artifact").length() == checkpoint.bytes
        }.getOrDefault(false)
    }
    private fun directory(id: String): File {
        val hash = MessageDigest.getInstance("SHA-256").digest(id.toByteArray()).joinToString("") { "%02x".format(it) }
        return File(app.filesDir, "content_ugoira_inputs/$hash")
    }
    override suspend fun execute(context: ContentPlanContext) {
        val request = gson.fromJson(context.plan.request, UgoiraSaveRequest::class.java)
        require(request.version == 1)
        val illust = gson.fromJson(request.illustJson, Illust::class.java)
        require(illust.id > 0 && illust.isGif())
        val directory = directory(context.plan.id); val source = File(directory, "artifact")
        var saved = publisher.checkpoint(context)
        val ready = phases.run(context.plan.id, "ugoira-input-check", "ugoira-input:${context.plan.id}", Pool.COPY) {
            saved.bytes > 0 && source.isFile && source.length() == saved.bytes && ArtifactPublisher.digest(source) == saved.sha256
        }
        if (!ready) {
            if (saved.receipt != null) throw IOException("Ugoira input changed after output opening")
            if (source.exists()) phases.run(context.plan.id, "ugoira-invalid-input", "ugoira-input:${context.plan.id}", Pool.COPY) {
                if (!source.delete()) throw IOException("Cannot replace invalid ugoira input")
            }
            if (!manual.contains(context.plan.family) && !allowed()) context.yieldForPolicy()
            prepare(illust, request.preferVideo)
            UgoiraCacheAccess.read {
                val session = UgoiraOutputSession(context, directory, request, phases)
                withContext(UgoiraOutputSession.current.asContextElement(session)) {
                    generate(illust) { context.progress(it.name) }
                }
            }
            saved = publisher.checkpoint(context)
        }
        val domain = gson.fromJson(saved.domain, UgoiraSaveDomain::class.java)
        check(domain.version == 1)
        publisher.publish(context, source, if (domain.video) request.video else request.gif) { uri ->
            record(illust, uri, domain.video,
                (if (domain.video) request.video else request.gif).relativePath().filename)
        }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        phases.run(plan.id, "ugoira-cleanup", "ugoira-input:${plan.id}", Pool.COPY) {
            val dir = directory(plan.id)
            if (dir.exists() && !dir.deleteRecursively()) throw IOException("Cannot remove owned ugoira output input")
        }
    }
    companion object {
        fun batchFamily(rowId: Long) = "ugoira-batch:$rowId"
        @JvmStatic fun onNetworkGateClosed() { instance?.second?.let { it.manual.clear(); it.queue.refreshEligibility() } }
        @JvmStatic fun refreshEligibility() { instance?.second?.queue?.refreshEligibility() }
        @Volatile private var instance: Pair<Context, QueuedUgoira>? = null
        @JvmStatic fun get(context: Context): QueuedUgoira {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedUgoira(app).also { instance = app to it }
            }
        }
    }
}
