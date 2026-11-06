package ceui.pixiv.snapshot

import android.content.Context
import android.content.Intent
import android.net.Uri
import ceui.lisa.activities.Shaft
import ceui.pixiv.download.copy.CopyDestination
import ceui.pixiv.download.copy.DocumentCopyOutput
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.FilterOutputStream
import java.io.IOException
import java.security.MessageDigest
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

/** Replayable local snapshot operations; directory leases are acquired before physical permits. */
class QueuedSnapshotOperations internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    recoverLegacy: Boolean = true,
) : ContentPlanHandler {
    override val kind = "snapshot-operation"
    private val gson = Gson()
    init {
        queue.register(this)
        if (recoverLegacy) CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, error ->
            timber.log.Timber.w(error, "Legacy snapshot backup discovery failed")
        }).launch {
            queue.ready.await()
            SnapshotRepository.root(app).listFiles()?.filter { it.isDirectory && it.name.startsWith(".backup_") }
                ?.sortedByDescending { it.lastModified() }?.forEach { backup ->
                    val manifest = SnapshotValidator.readJson<SnapshotManifest>(File(backup, SNAPSHOT_MANIFEST))
                    if (manifest != null) runCatching { enqueue(SnapshotOperationRequest(operation = RECOVER,
                        snapshotId = manifest.snapshotId, fileName = backup.name)) }
                        .onFailure { timber.log.Timber.w(it, "Unrecognized old snapshot backup retained") }
                }
        }
    }
    fun enqueue(request: SnapshotOperationRequest): ContentPlanTicket {
        require(request.version == 1 && request.operation in OPERATIONS)
        request.snapshotId?.let { SnapshotRepository.dir(app, it) }
        for (uri in listOfNotNull(request.uri, request.tree)) runCatching {
            val flags = if (request.operation == IMPORT) Intent.FLAG_GRANT_READ_URI_PERMISSION
                else Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION
            app.contentResolver.takePersistableUriPermission(Uri.parse(uri), flags)
        }
        val family = "snapshot-operation:${request.operation}:${request.snapshotId}:${hash(request.uri.orEmpty() + request.tree.orEmpty() + request.fileName.orEmpty())}"
        return queue.submit(family, kind, reuseSucceeded = request.operation == RECOVER) { gson.toJson(request) }
    }
    suspend fun await(ticket: ContentPlanTicket): SnapshotOperationState {
        val end = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
        when (end.state) {
            State.CANCELLED, State.PAUSED -> throw CancellationException("Snapshot operation stopped")
            State.SUCCEEDED -> Unit
            else -> throw SnapshotException(end.error ?: "快照操作失败")
        }
        val saved = gson.fromJson(requireNotNull(queue.store.get(ticket.id)).checkpoint, ArtifactCheckpoint::class.java)
        return gson.fromJson(saved.domain, SnapshotOperationState::class.java)
    }
    private fun root(id: String) = File(app.filesDir, "content_snapshot_operations/${hash(id)}")
    private fun state(context: ContentPlanContext): SnapshotOperationState = runCatching {
        val outer = gson.fromJson(context.checkpoint, ArtifactCheckpoint::class.java)
        gson.fromJson(outer.domain, SnapshotOperationState::class.java)
    }.getOrNull()?.takeIf { it.version == 1 } ?: SnapshotOperationState()
    private suspend fun save(context: ContentPlanContext, state: SnapshotOperationState) {
        val outer = runCatching { gson.fromJson(context.checkpoint, ArtifactCheckpoint::class.java) }
            .getOrNull()?.takeIf { it.version == 1 } ?: ArtifactCheckpoint()
        context.checkpoint(gson.toJson(outer.copy(domain = gson.toJson(state))))
    }
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, SnapshotOperationRequest::class.java).also {
        require(it.version == 1 && it.operation in OPERATIONS)
        it.snapshotId?.let { id -> SnapshotRepository.dir(app, id) }
    }
    override suspend fun execute(context: ContentPlanContext) {
        val request = request(context.plan)
        when (request.operation) {
            EXPORT, EXPORT_TREE -> export(context, request)
            IMPORT -> import(context, request)
            PROMOTE -> promote(context, request)
            DELETE -> delete(context, request)
            RECOVER -> recoverLegacy(context, request)
        }
    }
    private suspend fun export(context: ContentPlanContext, request: SnapshotOperationRequest) {
        val id = requireNotNull(request.snapshotId)
        val source = File(root(context.plan.id), "archive.shaftsnap")
        val saved = runCatching { gson.fromJson(context.checkpoint, ArtifactCheckpoint::class.java) }
            .getOrNull()?.takeIf { it.version == 1 }
        val ready = phases.run(context.plan.id, "snapshot-archive-check", "snapshot-operation:${context.plan.id}", Pool.COPY) {
            source.isFile && source.length() > 0 && (saved == null || saved.bytes == 0L ||
                (source.length() == saved.bytes && ArtifactPublisher.digest(source) == saved.sha256))
        }
        if (!ready) {
            if (saved?.receipt != null) throw IOException("Export input changed after output opening")
            ContentResourceLeases.read("snapshot:$id") {
                phases.run(context.plan.id, "snapshot-archive", "snapshot-operation:${context.plan.id}", Pool.ENCODE) {
                    val directory = SnapshotRepository.dir(app, id)
                    val manifest = SnapshotRepository.readManifest(app, id) ?: throw SnapshotException("快照不存在: $id")
                    request.manifestHash?.let { expected ->
                        if (ArtifactPublisher.digest(File(directory, SNAPSHOT_MANIFEST)) != expected) throw SnapshotException("快照在导出前已改变，请重新选择")
                    }
                    SnapshotValidator.validateContents(directory, manifest)
                    source.parentFile!!.mkdirs()
                    val temporary = File(source.parentFile, "archive.building")
                    val active = currentCoroutineContext()
                    try {
                        FileOutputStream(temporary).use { output ->
                            SnapshotArchive.zipDirectory(directory, object : FilterOutputStream(output) { override fun close() { flush() } }) {
                                active.ensureActive(); context.checkStop()
                            }
                            output.fd.sync()
                        }
                        active.ensureActive(); context.checkStop()
                        if (!temporary.renameTo(source)) throw IOException("Cannot commit snapshot archive input")
                    } finally { temporary.delete() }
                }
            }
        }
        save(context, state(context).copy(prepared = true))
        val name = requireNotNull(request.fileName)
        val destination = CopyDestination("cache", segments = listOf("explicit-snapshot", hash(request.uri.orEmpty() + request.tree.orEmpty()), name),
            mime = "application/zip", policy = "Replace")
        ArtifactPublisher(phases, DocumentCopyOutput(app, request.uri, request.tree, name, "application/zip"))
            .publish(context, source, destination)
    }
    private suspend fun files(directory: File): Map<String, SnapshotResource> = directory.walkTopDown().filter { it.isFile }.associate { file ->
        val path = file.relativeTo(directory).invariantSeparatorsPath
        require(safeResolve(directory, path).canonicalPath == file.canonicalPath)
        path to SnapshotResource("import", file.length(), ArtifactPublisher.digest(file))
    }
    private suspend fun matches(directory: File, expected: Map<String, SnapshotResource>): Boolean = directory.isDirectory &&
        expected.isNotEmpty() && expected.all { (path, receipt) ->
            val file = safeResolve(directory, path)
            file.isFile && file.length() == receipt.bytes && ArtifactPublisher.digest(file) == receipt.sha256
        } && directory.walkTopDown().filter { it.isFile }.map { it.relativeTo(directory).invariantSeparatorsPath }.toSet() == expected.keys
    private fun backup(context: ContentPlanContext) = File(SnapshotRepository.root(app), ".queue-import-${hash(context.plan.id)}.old")
    private suspend fun import(context: ContentPlanContext, request: SnapshotOperationRequest) {
        val staging = File(root(context.plan.id), "unpacked")
        var saved = state(context)
        var manifest = saved.manifestJson?.let { gson.fromJson(it, SnapshotManifest::class.java) }
        if (saved.prepared && manifest != null) {
            val known = requireNotNull(manifest)
            val completed = ContentResourceLeases.write("snapshot:${known.snapshotId}") {
                phases.run(context.plan.id, "snapshot-import-recover", "snapshot-manifest:${known.snapshotId}", Pool.COPY) {
                    val target = SnapshotRepository.dir(app, known.snapshotId)
                    val old = backup(context)
                    if (!target.exists() && old.exists() && !old.renameTo(target)) throw IOException("Cannot recover previous snapshot")
                    if (!matches(target, saved.files)) false
                    else { context.commit { register(known) }; true }
                }
            }
            if (completed) return
        }
        val prepared = saved.prepared && phases.run(context.plan.id, "snapshot-import-input-check",
            "snapshot-operation:${context.plan.id}", Pool.COPY) { matches(staging, saved.files) }
        if (!prepared) {
            phases.run(context.plan.id, "snapshot-import-unpack", "snapshot-operation:${context.plan.id}", Pool.COPY) {
                remove(staging); staging.mkdirs()
                val active = currentCoroutineContext()
                app.contentResolver.openInputStream(Uri.parse(requireNotNull(request.uri)))?.use { input ->
                    SnapshotArchive.unzip(input, staging) { active.ensureActive(); context.checkStop() }
                } ?: throw SnapshotException("无法读取所选文件，请重新选择")
                manifest = SnapshotValidator.readJson<SnapshotManifest>(File(staging, SNAPSHOT_MANIFEST))
                    ?: throw SnapshotException("不是有效的 .shaftsnap 快照")
                require(manifest!!.schemaVersion == SNAPSHOT_SCHEMA_VERSION)
                requireSnapshotId(manifest!!.snapshotId); SnapshotValidator.validate(staging, manifest!!)
                saved = SnapshotOperationState(manifestJson = gson.toJson(manifest), files = files(staging), prepared = true)
                save(context, saved)
            }
        }
        val ready = requireNotNull(manifest)
        ContentResourceLeases.write("snapshot:${ready.snapshotId}") {
            phases.run(context.plan.id, "snapshot-import-install", "snapshot-manifest:${ready.snapshotId}", Pool.COPY) {
                val target = SnapshotRepository.dir(app, ready.snapshotId)
                val old = backup(context); target.parentFile!!.mkdirs()
                if (old.exists()) {
                    if (!target.exists()) { if (!old.renameTo(target)) throw IOException("Cannot recover previous snapshot") }
                    else throw IOException("Previous snapshot backup requires recovery")
                }
                context.commit {
                    val existed = target.exists()
                    if (existed && !target.renameTo(old)) throw IOException("Cannot preserve previous snapshot")
                    if (!staging.renameTo(target)) {
                        if (existed && !old.renameTo(target)) throw IOException("Snapshot rollback failed; backup retained")
                        throw IOException("Cannot install validated snapshot")
                    }
                    register(ready)
                }
            }
        }
    }
    private fun register(manifest: SnapshotManifest) {
        SnapshotRuntimeCache.remove(manifest.snapshotId)
        SnapshotIndex.onSnapshotWritten(manifest.illustId, manifest.snapshotId, false, manifest.createdAt)
    }
    private suspend fun promote(context: ContentPlanContext, request: SnapshotOperationRequest) {
        val id = requireNotNull(request.snapshotId)
        ContentResourceLeases.write("snapshot:$id") {
            phases.run(context.plan.id, "snapshot-promote", "snapshot-manifest:$id", Pool.COPY) {
                val directory = SnapshotRepository.dir(app, id)
                var saved = state(context)
                val formal = saved.manifestJson?.let { gson.fromJson(it, SnapshotManifest::class.java) }
                    ?: AutoSnapshotRepository.readAutoManifest(app, id)?.toSnapshotManifest()
                    ?: throw SnapshotException("自动快照不存在: $id")
                require(formal.snapshotId == id)
                SnapshotValidator.validateContents(directory, formal)
                saved = saved.copy(manifestJson = gson.toJson(formal), prepared = true); save(context, saved)
                context.commit {
                    atomicText(File(directory, SNAPSHOT_MANIFEST), requireNotNull(saved.manifestJson))
                    val auto = File(directory, AUTO_SNAPSHOT_MANIFEST)
                    if (auto.exists() && !auto.delete()) throw IOException("Cannot retire automatic snapshot manifest")
                    register(formal)
                }
            }
        }
    }
    private suspend fun delete(context: ContentPlanContext, request: SnapshotOperationRequest) {
        val id = requireNotNull(request.snapshotId)
        ContentResourceLeases.write("snapshot:$id") {
            phases.run(context.plan.id, "snapshot-delete", "snapshot-manifest:$id", Pool.COPY) {
                val target = SnapshotRepository.dir(app, id)
                val saved = state(context).let { if (it.prepared) it else it.copy(prepared = true, existed = target.isDirectory) }
                save(context, saved)
                context.commit {
                    remove(target); SnapshotRuntimeCache.remove(id); SnapshotIndex.onSnapshotDeleted(id)
                }
            }
        }
    }
    private suspend fun recoverLegacy(context: ContentPlanContext, request: SnapshotOperationRequest) {
        val id = requireNotNull(request.snapshotId)
        val name = requireNotNull(request.fileName)
        require(name.startsWith(".backup_") && !name.contains('/') && !name.contains('\\'))
        ContentResourceLeases.write("snapshot:$id") {
            phases.run(context.plan.id, "snapshot-legacy-recovery", "snapshot-manifest:$id", Pool.COPY) {
                val target = SnapshotRepository.dir(app, id)
                val old = File(SnapshotRepository.root(app), name)
                if (target.exists()) {
                    val manifest = SnapshotRepository.readManifest(app, id) ?: throw SnapshotException("已有快照需要人工检查，保留旧备份: $id")
                    require(manifest.schemaVersion == SNAPSHOT_SCHEMA_VERSION)
                    SnapshotValidator.validateContents(target, manifest)
                    save(context, SnapshotOperationState(manifestJson = gson.toJson(manifest), prepared = true))
                    context.commit { register(manifest) } // keep the older backup; never guess that it is disposable
                } else {
                    val manifest = SnapshotValidator.readJson<SnapshotManifest>(File(old, SNAPSHOT_MANIFEST))
                        ?: throw SnapshotException("旧快照备份无效，已保留: $name")
                    require(manifest.snapshotId == id && manifest.schemaVersion == SNAPSHOT_SCHEMA_VERSION)
                    SnapshotValidator.validateContents(old, manifest)
                    save(context, SnapshotOperationState(manifestJson = gson.toJson(manifest), prepared = true))
                    context.commit {
                        if (!old.renameTo(target)) throw IOException("Cannot recover old snapshot backup")
                        register(manifest)
                    }
                }
            }
        }
    }
    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val request = request(plan)
        val outer = gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java)
        if (request.operation in setOf(EXPORT, EXPORT_TREE)) {
            val name = requireNotNull(request.fileName)
            val destination = CopyDestination("cache", segments = listOf("explicit-snapshot",
                hash(request.uri.orEmpty() + request.tree.orEmpty()), name), mime = "application/zip", policy = "Replace")
            return ArtifactPublisher(phases, DocumentCopyOutput(app, request.uri, request.tree, name, "application/zip"))
                .reconcile(plan, destination)
        }
        val saved = runCatching { gson.fromJson(outer.domain, SnapshotOperationState::class.java) }.getOrNull() ?: return false
        if (!saved.prepared) return false
        val manifest = saved.manifestJson?.let { gson.fromJson(it, SnapshotManifest::class.java) }
        val id = request.snapshotId ?: manifest?.snapshotId ?: return false
        return ContentResourceLeases.write("snapshot:$id") {
            phases.run(plan.id, "snapshot-operation-reconcile", "snapshot-manifest:$id", Pool.COPY) {
                val target = SnapshotRepository.dir(app, id)
                when (request.operation) {
                    DELETE -> if (target.exists()) false else { SnapshotRuntimeCache.remove(id); SnapshotIndex.onSnapshotDeleted(id); true }
                    IMPORT -> if (manifest != null && matches(target, saved.files)) { register(manifest); true } else false
                    PROMOTE, RECOVER -> {
                        val file = File(target, SNAPSHOT_MANIFEST)
                        if (manifest == null || !file.isFile || file.readText() != saved.manifestJson) false
                        else {
                            SnapshotValidator.validateContents(target, manifest)
                            if (request.operation == PROMOTE) {
                                val auto = File(target, AUTO_SNAPSHOT_MANIFEST)
                                if (auto.exists() && !auto.delete()) throw IOException("Cannot finish snapshot promotion")
                            }
                            register(manifest); true
                        }
                    }
                    else -> false
                }
            }
        }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        val old = File(SnapshotRepository.root(app), ".queue-import-${hash(plan.id)}.old")
        if (state == State.CANCELLED && old.exists()) {
            val snapshot = runCatching {
                val outer = gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java)
                val saved = gson.fromJson(outer.domain, SnapshotOperationState::class.java)
                gson.fromJson(saved.manifestJson, SnapshotManifest::class.java)
            }.getOrNull()
            if (snapshot != null) ContentResourceLeases.write("snapshot:${snapshot.snapshotId}") {
                phases.run(plan.id, "snapshot-import-rollback", "snapshot-manifest:${snapshot.snapshotId}", Pool.COPY) {
                    val target = SnapshotRepository.dir(app, snapshot.snapshotId)
                    if (!target.exists() && !old.renameTo(target)) throw IOException("Cannot restore cancelled snapshot import backup")
                }
            }
        }
        phases.run(plan.id, "snapshot-operation-cleanup", "snapshot-operation:${plan.id}", Pool.COPY) {
            if (state == State.SUCCEEDED) remove(old)
            // Cancelled operations never delete a backup that may be the only previous output.
            remove(root(plan.id))
        }
    }
    companion object {
        const val EXPORT = "export"; const val EXPORT_TREE = "export-tree"; const val IMPORT = "import"
        const val PROMOTE = "promote"; const val DELETE = "delete"
        const val RECOVER = "recover-legacy-import"
        private val OPERATIONS = setOf(EXPORT, EXPORT_TREE, IMPORT, PROMOTE, DELETE, RECOVER)
        private fun hash(value: String) = MessageDigest.getInstance("SHA-256").digest(value.toByteArray()).joinToString("") { "%02x".format(it) }
        private fun remove(file: File) { if (file.exists() && !file.deleteRecursively()) throw IOException("Cannot remove owned snapshot files") }
        private fun atomicText(target: File, text: String) {
            val temporary = File(target.parentFile, target.name + ".operation")
            try {
                FileOutputStream(temporary).use { it.write(text.toByteArray(Charsets.UTF_8)); it.fd.sync() }
                if (!temporary.renameTo(target)) throw IOException("Cannot commit snapshot metadata")
            } finally { temporary.delete() }
        }
        @Volatile private var instance: Pair<Context, QueuedSnapshotOperations>? = null
        @JvmStatic fun get(context: Context): QueuedSnapshotOperations {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedSnapshotOperations(app).also { instance = app to it }
            }
        }
    }
}
