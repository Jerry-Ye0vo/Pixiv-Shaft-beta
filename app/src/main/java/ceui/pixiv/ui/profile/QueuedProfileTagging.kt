package ceui.pixiv.ui.profile

import android.content.Context
import android.net.Uri
import android.os.SystemClock
import android.system.Os
import android.system.OsConstants
import androidx.annotation.Keep
import ceui.lisa.database.AppDatabase
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import com.google.gson.Gson
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.io.IOException
import java.security.MessageDigest

@Keep internal data class ProfileTagRequest(val version: Int = 1, val force: Boolean = false)
@Keep internal data class ProfileTagProgress(
    val version: Int = 1, val after: Long = 0, val ceiling: Long = 0,
    val scanned: Int = 0, val inferred: Int = 0, val reused: Int = 0,
    val skipped: Int = 0, val failed: Int = 0, val elapsedMs: Long = 0,
    val initialized: Boolean = false,
) {
    val perMinute: Double get() = if (elapsedMs > 0) inferred * 60_000.0 / elapsedMs else 0.0
}

/** Durable analysis parent. Downloads and image inference share the existing queue owners. */
internal class QueuedProfileTagging internal constructor(
    private val app: Context,
    private val queue: ContentPlanQueue = ContentPlanQueue.get(app),
    private val phases: ContentPhaseRunner = ContentPhases.runner(app),
    private val database: () -> AppDatabase = { AppDatabase.getAppDatabase(app) },
    private val modelReady: () -> Boolean = { LocalTaggerModel.ready(app) },
    private val createTagger: () -> ProfileTagger = { OfflineImageTagger.open(app) },
) : ContentPlanHandler {
    override val kind = KIND
    private val gson = Gson()
    init { queue.register(this) }
    override fun priority(plan: ContentPlanStore.Plan) = ContentQueueRuntime.BACKGROUND
    fun start(force: Boolean = false) = queue.submit(FAMILY, KIND) { gson.toJson(ProfileTagRequest(force = force)) }
    fun current() = queue.latest(FAMILY)
    fun resume(id: String) = queue.resume(id)
    fun progress(ticket: ContentPlanTicket): ProfileTagProgress? = queue.store.get(ticket.id)?.let {
        runCatching { gson.fromJson(it.checkpoint, ProfileTagProgress::class.java) }.getOrNull()
    }

    override suspend fun execute(context: ContentPlanContext) {
        val request = gson.fromJson(context.plan.request, ProfileTagRequest::class.java)
        require(request.version == 1)
        val dao = database().profileAnalyticsDao()
        var saved = runCatching { gson.fromJson(context.checkpoint, ProfileTagProgress::class.java) }.getOrNull()
            ?.takeIf { it.version == 1 && it.initialized }
            ?: ProfileTagProgress(ceiling = dao.downloadCeiling(), initialized = true)
        val total = dao.downloadCount(saved.ceiling)
        var checkpointTime = SystemClock.elapsedRealtime()
        var processedHere = false
        var tagger: ProfileTagger? = null
        try {
            while (true) {
                currentCoroutineContext().ensureActive(); context.checkStop()
                val rows = dao.downloadBatch(saved.after, saved.ceiling, ProfileRepository.BATCH)
                if (rows.isEmpty()) break
                for (row in rows) {
                    currentCoroutineContext().ensureActive(); context.checkStop()
                    val began = SystemClock.elapsedRealtime()
                    var inferred = 0; var reused = 0; var skipped = 0; var failed = 0
                    val uri = Uri.parse(row.filePath)
                    if (!staticImage(row.fileName)) skipped++ else {
                        try {
                            phases.run(context.plan.id, "profile-image", "profile-image:${row.filePath}", Pool.ENCODE) {
                                context.checkStop()
                                val stamp = sourceStamp(app, uri)
                                val cached = dao.findTags(row.filePath, LocalTaggerModel.VERSION)
                                if (!request.force && stamp.isNotEmpty() && cached?.sourceStamp == stamp) {
                                    reused++
                                } else {
                                    val hash = digest(app, uri)
                                    val identical = if (request.force) null else dao.findHash(hash, LocalTaggerModel.VERSION)
                                    val predictions = if (identical != null) { reused++; identical.tagsJson }
                                    else {
                                        if (tagger == null) tagger = try { createTagger() }
                                            catch (error: Exception) { throw ModelLoadFailure(error) }
                                        val values = tagger!!.predict(uri)
                                        currentCoroutineContext().ensureActive(); context.checkStop()
                                        inferred++; gson.toJson(values)
                                    }
                                    dao.rememberTags(LocalImageTags(row.filePath, LocalTaggerModel.VERSION, stamp,
                                        hash, predictions, SystemClock.elapsedRealtime() - began, System.currentTimeMillis()))
                                }
                            }
                        } catch (cancelled: kotlinx.coroutines.CancellationException) { throw cancelled }
                        catch (error: ModelLoadFailure) { throw error }
                        catch (error: Exception) {
                            // An unavailable model is actionable for the entire batch, not N failed files.
                            if (!modelReady()) throw IOException("Download the image tagger model first", error)
                            failed++
                            timber.log.Timber.w(error, "Local image tagging failed: %s", row.fileName)
                        }
                    }
                    val now = SystemClock.elapsedRealtime()
                    saved = saved.copy(after = row.sequence, scanned = saved.scanned + 1,
                        inferred = saved.inferred + inferred, reused = saved.reused + reused,
                        skipped = saved.skipped + skipped, failed = saved.failed + failed,
                        elapsedMs = saved.elapsedMs + now - checkpointTime)
                    checkpointTime = now; processedHere = true
                    // One completed-file checkpoint; interrupted inference is retried, committed tags are reusable.
                    context.checkpoint(gson.toJson(saved))
                    context.progress("profile-tagging", saved.scanned.toLong(), total.toLong())
                }
            }
            if (!processedHere) context.checkpoint(gson.toJson(saved))
            context.commit {}
        } finally { tagger?.close() }
    }

    companion object {
        const val KIND = "profile-tagging"
        const val FAMILY = "profile-tagging:device"
        private var instance: Pair<Context, QueuedProfileTagging>? = null
        @JvmStatic fun get(context: Context): QueuedProfileTagging = synchronized(this) {
            val app = context.applicationContext
            instance?.takeIf { it.first === app }?.second ?: QueuedProfileTagging(app).also { instance = app to it }
        }
        internal fun staticImage(name: String) = name.substringAfterLast('.', "").lowercase() in setOf("jpg", "jpeg", "png", "webp", "bmp")
        /** Empty means the provider cannot supply a reliable cache stamp; content must be checked. */
        private fun sourceStamp(context: Context, uri: Uri): String = context.contentResolver.openFileDescriptor(uri, "r")?.use {
            runCatching {
                val stat = Os.fstat(it.fileDescriptor)
                if (OsConstants.S_ISREG(stat.st_mode) && stat.st_mtime > 0)
                    "${stat.st_size}:${stat.st_mtime}:${stat.st_ctime}:${stat.st_ino}" else ""
            }.getOrDefault("")
        } ?: throw IOException("Cannot read downloaded image")
        private suspend fun digest(context: Context, uri: Uri): String {
            val hash = MessageDigest.getInstance("SHA-256")
            context.contentResolver.openInputStream(uri)?.buffered()?.use { input ->
                val bytes = ByteArray(32 * 1024)
                while (true) {
                    currentCoroutineContext().ensureActive()
                    val count = input.read(bytes); if (count < 0) break
                    hash.update(bytes, 0, count)
                }
            } ?: throw IOException("Cannot open downloaded image")
            return hash.digest().joinToString("") { "%02x".format(it) }
        }
    }

    private class ModelLoadFailure(cause: Exception) : IOException("Cannot load the local image tagger model", cause)
}
