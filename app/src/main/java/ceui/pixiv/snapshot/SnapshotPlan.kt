package ceui.pixiv.snapshot

import android.content.Context
import androidx.annotation.Keep
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import com.google.gson.Gson
import java.io.File
import java.io.IOException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

@Keep
data class SnapshotRequest(val version: Int = 1, val snapshotId: String, val createdAt: Long,
    val illustJson: String, val comments: Boolean, val original: Boolean, val fresh: Boolean, val automatic: Boolean)
@Keep
data class SnapshotResource(val identity: String, val bytes: Long, val sha256: String)
@Keep
data class SnapshotCheckpoint(val version: Int = 1, val beanJson: String? = null, val commentsJson: String? = null,
    val resources: Map<String, SnapshotResource> = emptyMap(), val directories: Set<String> = emptySet(), val manifestJson: String? = null)

/** Serial checkpoints reconcile parallel assets without holding COPY while waiting for NETWORK. */
internal class SnapshotPlanSession(val context: ContentPlanContext, val request: SnapshotRequest,
    app: Context, private val phases: ContentPhaseRunner, private val transfers: SnapshotAssetTransfers,
    private val recovered: (Illust, SnapshotRequest) -> Unit = { bean, spec ->
        SnapshotIndex.onSnapshotWritten(bean.id, spec.snapshotId, spec.automatic, spec.createdAt)
    }) {
    val directory = SnapshotRepository.dir(app, request.snapshotId)
    private val gson = Gson()
    private val checkpointLock = Mutex()
    private var saved = runCatching { gson.fromJson(context.checkpoint, SnapshotCheckpoint::class.java) }
        .getOrNull()?.takeIf { it.version == 1 } ?: SnapshotCheckpoint()
    private suspend fun save(value: SnapshotCheckpoint) { context.checkpoint(gson.toJson(value)); saved = value }
    private fun relative(target: File): String {
        val base = directory.canonicalPath + File.separator
        require(target.canonicalPath.startsWith(base)) { "Snapshot resource escapes its owned directory" }
        return target.canonicalPath.removePrefix(base).replace(File.separatorChar, '/')
    }
    suspend fun sweepInterruptedWrites() = phases.run(context.plan.id, "snapshot-interrupted-inputs",
        "snapshot-manifest:${request.snapshotId}", Pool.COPY) {
        if (directory.isDirectory) directory.walkTopDown().filter {
            it.isFile && it.name.matches(Regex(".*\\.queue-[0-9a-f-]{36}"))
        }.forEach { if (!it.delete()) throw IOException("Cannot remove interrupted snapshot staging file") }
    }
    suspend fun bean(fetch: suspend () -> Illust): Illust {
        saved.beanJson?.let { return gson.fromJson(it, Illust::class.java) }
        val bean = fetch()
        checkpointLock.withLock { save(saved.copy(beanJson = gson.toJson(bean))) }
        return bean
    }
    suspend fun comments(fetch: suspend () -> SnapshotComments): SnapshotComments {
        saved.commentsJson?.let { return gson.fromJson(it, SnapshotComments::class.java) }
        val comments = fetch()
        checkpointLock.withLock { save(saved.copy(commentsJson = gson.toJson(comments))) }
        return comments
    }
    suspend fun resource(identity: String, target: File, write: suspend () -> Unit) {
        val path = relative(target)
        val known = checkpointLock.withLock { saved.resources[path] }
        val reusable = known != null && known.identity == identity && phases.run(context.plan.id,
            "snapshot-resource-check", "snapshot-file:" + target.absolutePath, Pool.COPY) { matches(target, known) }
        if (reusable) return
        write()
        val receipt = phases.run(context.plan.id, "snapshot-resource-receipt", "snapshot-file:" + target.absolutePath, Pool.COPY) {
            if (!target.isFile || target.length() <= 0) throw IOException("Empty snapshot resource: $path")
            SnapshotResource(identity, target.length(), ArtifactPublisher.digest(target))
        }
        checkpointLock.withLock { save(saved.copy(resources = saved.resources + (path to receipt))) }
    }
    suspend fun directoryReady(target: File): Boolean {
        val prefix = relative(target).trimEnd('/') + "/"
        val snapshot = checkpointLock.withLock { saved }
        if (prefix !in snapshot.directories) return false
        val expected = snapshot.resources.filterKeys { it.startsWith(prefix) }
        return expected.isNotEmpty() && phases.run(context.plan.id, "snapshot-directory-check", "snapshot-directory:${target.absolutePath}", Pool.COPY) {
            val actual = target.walkTopDown().filter { it.isFile }.map(::relative).toSet()
            actual == expected.keys && expected.all { (path, receipt) -> matches(File(directory, path), receipt) }
        }
    }
    suspend fun preserveDirectory(target: File) {
        val prefix = relative(target).trimEnd('/') + "/"
        val receipts = phases.run(context.plan.id, "snapshot-directory-receipt", "snapshot-directory:${target.absolutePath}", Pool.COPY) {
            target.walkTopDown().filter { it.isFile }.associate { file ->
                relative(file) to SnapshotResource("directory:$prefix", file.length(), ArtifactPublisher.digest(file))
            }.also { require(it.isNotEmpty()) }
        }
        checkpointLock.withLock { save(saved.copy(resources = saved.resources.filterKeys { !it.startsWith(prefix) } + receipts,
            directories = saved.directories + prefix)) }
    }
    suspend fun publish(manifest: Any, automatic: Boolean, index: () -> Unit) {
        require(automatic == request.automatic)
        val text = gson.toJson(manifest)
        checkpointLock.withLock { save(saved.copy(manifestJson = text)) }
        phases.run(context.plan.id, "snapshot-manifest", "snapshot-manifest:${request.snapshotId}", Pool.COPY) {
            context.commit {
                transfers.writeTextClaimed(File(directory, if (automatic) AUTO_SNAPSHOT_MANIFEST else SNAPSHOT_MANIFEST), text)
                index()
            }
        }
    }
    suspend fun forgetManifest() = checkpointLock.withLock { save(saved.copy(manifestJson = null)) }
    suspend fun recoverManifest(): Boolean {
        val text = saved.manifestJson ?: return false
        return phases.run(context.plan.id, "snapshot-recover", "snapshot-manifest:${request.snapshotId}", Pool.COPY) {
            val file = File(directory, if (request.automatic) AUTO_SNAPSHOT_MANIFEST else SNAPSHOT_MANIFEST)
            if (!file.isFile || file.readText() != text || saved.resources.isEmpty() ||
                !saved.resources.all { (path, receipt) -> matches(File(directory, path), receipt) }) return@run false
            context.commit {
                val bean = gson.fromJson(request.illustJson, Illust::class.java)
                recovered(bean, request)
            }
            true
        }
    }
    private suspend fun matches(file: File, receipt: SnapshotResource): Boolean = file.isFile && file.length() == receipt.bytes &&
        receipt.bytes > 0 && ArtifactPublisher.digest(file) == receipt.sha256
    companion object { val current = ThreadLocal<SnapshotPlanSession?>() }
}
