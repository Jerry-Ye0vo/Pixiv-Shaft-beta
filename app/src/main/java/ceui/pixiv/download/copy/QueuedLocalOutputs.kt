package ceui.pixiv.download.copy

import android.content.Context
import androidx.annotation.Keep
import ceui.lisa.activities.Shaft
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.model.Bucket
import ceui.pixiv.download.model.RelativePath
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

@Keep
data class LocalOutputRequest(val version: Int = 1, val type: String, val subjectId: Long,
    val destination: CopyDestination, val contextJson: String = "", val content: String? = null,
    val sourcePath: String? = null, val sourceBytes: Long = 0, val sourceModified: Long = 0,
    val producer: String? = null, val asVideo: Boolean = false)

data class LocalOutputSubmission(val ticket: ContentPlanTicket, val shareFile: File)

/** Text and immutable local outputs use the same durable publisher. A legacy producer only owns
 * its preparation callback until capture; a cold process must never pretend it can replay a closure.
 */
class QueuedLocalOutputs internal constructor(private val app: Context,
    val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    output: CopyOutput = AndroidCopyOutput(app) { DownloadsRegistry.downloads },
    private val recorded: (LocalOutputRequest, android.net.Uri) -> Unit = { request, uri ->
        if (request.type == UGOIRA) {
            val illust = Gson().fromJson(request.contextJson, ceui.pixiv.api.model.Illust::class.java)
            ceui.pixiv.download.UgoiraDownloadRecord.recordStrict(illust, uri, request.asVideo, request.destination.relativePath().filename)
        }
    },
) : ContentPlanHandler {
    override val kind = "local-output"
    private val gson = Gson()
    private val publisher = ArtifactPublisher(phases, output)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val producers = ConcurrentHashMap<String, Consumer<File>>()
    private class Pin(val family: String, val reference: Closeable, val cache: Boolean) {
        @Volatile var ticket: ContentPlanTicket? = null
    }
    private val pins = ConcurrentHashMap<String, Pin>()
    init { queue.register(this) }
    private fun directory(id: String) = File(app.filesDir, "content_local_outputs/${hash(id)}")
    fun enqueue(family: String, source: File? = null, producer: Pair<String, Consumer<File>>? = null,
        cacheReference: Closeable? = null,
        reuseSucceeded: Boolean = false,
        request: suspend () -> LocalOutputRequest): ContentPlanTicket {
        val sourceReference = try { source?.let(CopyInputReferences::reserve) }
            catch (failed: Throwable) { cacheReference?.close(); throw failed }
        val reference = if (sourceReference != null || cacheReference != null) Closeable {
            try { sourceReference?.close() } finally { cacheReference?.close() }
        } else null
        val token = UUID.randomUUID().toString()
        val pin = reference?.let { Pin(family, it, cacheReference != null).also { entry -> pins[token] = entry } }
        if (producer != null) producers[producer.first] = producer.second
        val ticket = try { queue.submit(family, kind, reuseSucceeded = reuseSucceeded) { gson.toJson(request()) } }
        catch (failure: Throwable) { pins.remove(token)?.reference?.close(); producer?.let { producers.remove(it.first) }; throw failure }
        pin?.ticket = ticket
        scope.launch {
            val stopped = ticket.status.first { it.phase == CAPTURED || it.state in ContentPlanTicket.OBSERVABLE_END }
            pins.remove(token)?.reference?.close()
            if (stopped.state == State.PAUSED) ticket.status.first {
                it.phase == CAPTURED || it.state in setOf(State.SUCCEEDED, State.CANCELLED, State.FAILED)
            }
            producer?.let { producers.remove(it.first) }
        }
        return ticket
    }
    internal suspend fun shutdown() {
        scope.coroutineContext[Job]?.cancelAndJoin()
        pins.values.forEach { it.reference.close() }; pins.clear(); producers.clear()
    }
    private fun releasePins(id: String) {
        pins.entries.filter { it.value.ticket?.id == id }.forEach { (token, pin) ->
            if (pins.remove(token, pin)) pin.reference.close()
        }
    }
    suspend fun await(ticket: ContentPlanTicket): Boolean {
        val end = ticket.status.first { it.state in ContentPlanTicket.OBSERVABLE_END }
        if (end.state == State.CANCELLED || end.state == State.PAUSED) throw CancellationException("Local output stopped")
        return end.state == State.SUCCEEDED
    }
    suspend fun receipt(ticket: ContentPlanTicket): ArtifactCheckpoint = withContext(Dispatchers.IO) {
        gson.fromJson(requireNotNull(queue.store.get(ticket.id)).checkpoint, ArtifactCheckpoint::class.java)
    }
    fun freeze(bucket: Bucket, path: RelativePath, mime: String): CopyDestination {
        val resolved = DownloadsRegistry.store.loadOrFallback().resolve(bucket)
        return CopyDestination.capture(resolved.storage, path, mime, resolved.overwrite)
    }
    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val request = gson.fromJson(plan.request, LocalOutputRequest::class.java)
        return publisher.reconcile(plan, request.destination) { recorded(request, it) }
    }
    override suspend fun execute(context: ContentPlanContext) {
        val request = gson.fromJson(context.plan.request, LocalOutputRequest::class.java)
        require(request.version == 1 && request.type in TYPES)
        request.destination.storageChoice(); request.destination.relativePath(); request.destination.overwrite()
        val source = File(directory(context.plan.id), "artifact")
        val saved = publisher.checkpoint(context)
        val reusable = phases.run(context.plan.id, "local-input-check", "local-input:${context.plan.id}", Pool.COPY) {
            source.isFile && source.length() > 0 && (saved.bytes == 0L ||
                (saved.bytes == source.length() && saved.sha256 == ArtifactPublisher.digest(source)))
        }
        if (!reusable) {
            if (saved.receipt != null) throw IOException("Local input changed after output opening")
            suspend fun capture() = phases.run(context.plan.id, "local-input-capture",
                request.sourcePath?.let { "local-source:$it" } ?: "local-input:${context.plan.id}", Pool.COPY) {
                source.parentFile!!.mkdirs()
                val temporary = File(source.parentFile, "artifact.building")
                try {
                    if (request.producer != null) {
                        val producer = producers.remove(request.producer)
                            ?: throw IOException("上次生成被中断，请重新导出备份")
                        val generated = File(requireNotNull(request.sourcePath)); generated.parentFile?.mkdirs()
                        producer.accept(generated)
                    }
                    FileOutputStream(temporary).use { output ->
                        if (request.content != null) output.write(request.content.toByteArray(Charsets.UTF_8))
                        else {
                            val input = File(requireNotNull(request.sourcePath))
                            if (request.producer == null && (input.length() != request.sourceBytes || input.lastModified() != request.sourceModified))
                                throw IOException("Local source changed before capture")
                            input.inputStream().use { stream ->
                                val expected = stream.channel.size(); val buffer = ByteArray(32 * 1024); var bytes = 0L
                                while (true) {
                                    currentCoroutineContext().ensureActive(); context.checkStop()
                                    val n = stream.read(buffer); if (n < 0) break
                                    output.write(buffer, 0, n); bytes += n
                                }
                                if (bytes == 0L || bytes != expected || stream.channel.size() != expected)
                                    throw IOException("Local source changed during capture")
                            }
                        }
                        output.fd.sync()
                    }
                    if (temporary.length() <= 0) throw IOException("Empty local output")
                    currentCoroutineContext().ensureActive(); context.checkStop()
                    if (!temporary.renameTo(source)) throw IOException("Cannot preserve local output input")
                } finally { temporary.delete() }
                context.checkpoint(gson.toJson(saved.copy(bytes = source.length(), sha256 = ArtifactPublisher.digest(source))))
            }
            // Legacy share inputs live in GIF cache; take the directory lease before COPY admission.
            val borrowed = pins.values.any { it.cache && it.family == context.plan.family && (it.ticket == null || it.ticket?.id == context.plan.id) }
            if (request.sourcePath != null && !borrowed) ceui.pixiv.ui.bulk.UgoiraCacheAccess.read { capture() }
            else capture()
        }
        releasePins(context.plan.id); context.progress(CAPTURED)
        publisher.publish(context, source, request.destination) { recorded(request, it) }
    }
    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        releasePins(plan.id)
        if (state !in setOf(State.SUCCEEDED, State.CANCELLED)) return
        if (!directory(plan.id).exists()) return
        phases.run(plan.id, "local-output-cleanup", "local-input:${plan.id}", Pool.COPY) {
            val directory = directory(plan.id)
            if (directory.exists() && !directory.deleteRecursively()) throw IOException("Cannot remove owned local output input")
        }
    }
    companion object {
        const val CAPTION = "caption"; const val BACKUP = "backup"; const val UGOIRA = "ugoira-existing"; const val FILE = "file"; const val TEXT = "text"
        private const val CAPTURED = "local-input-captured"
        private val TYPES = setOf(CAPTION, BACKUP, UGOIRA, FILE, TEXT)
        private fun hash(value: String) = MessageDigest.getInstance("SHA-256").digest(value.toByteArray()).joinToString("") { "%02x".format(it) }
        @Volatile private var instance: Pair<Context, QueuedLocalOutputs>? = null
        @JvmStatic fun get(context: Context): QueuedLocalOutputs {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedLocalOutputs(app).also { instance = app to it }
            }
        }
        @JvmStatic fun text(context: Context, bucket: Bucket, path: RelativePath, mime: String, text: String): ContentPlanTicket {
            val outputs = get(context)
            return outputs.enqueue("local-text:${UUID.randomUUID()}") {
                LocalOutputRequest(type = TEXT, subjectId = 0, content = text, destination = outputs.freeze(bucket, path, mime))
            }
        }
        @JvmStatic fun file(context: Context, bucket: Bucket, path: RelativePath, mime: String, file: File): ContentPlanTicket {
            val outputs = get(context)
            return outputs.enqueue("local-file:${UUID.randomUUID()}", source = file) {
                LocalOutputRequest(type = FILE, subjectId = 0, sourcePath = file.absolutePath, sourceBytes = file.length(),
                    sourceModified = file.lastModified(), destination = outputs.freeze(bucket, path, mime))
            }
        }
        @JvmStatic fun backup(context: Context, name: String, producer: Consumer<File>): LocalOutputSubmission {
            val outputs = get(context); val token = UUID.randomUUID().toString()
            val share = File(ceui.lisa.file.LegacyFile.gifCacheFolder(context), ".queued-backups/$token/" + ceui.lisa.file.LegacyFile.safeFileName(name))
            val ticket = outputs.enqueue("backup:$token", producer = token to producer) {
                LocalOutputRequest(type = BACKUP, subjectId = 0, producer = token, sourcePath = share.absolutePath,
                    destination = outputs.freeze(Bucket.Backup, DownloadItems.backupDestination(name), "application/json"))
            }
            return LocalOutputSubmission(ticket, share)
        }
        @JvmStatic fun awaitBlocking(context: Context, ticket: ContentPlanTicket): Boolean = runBlocking(Dispatchers.IO) { get(context).await(ticket) }
    }
}
