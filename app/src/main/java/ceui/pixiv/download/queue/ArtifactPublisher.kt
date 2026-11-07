package ceui.pixiv.download.queue

import androidx.annotation.Keep
import android.net.Uri
import ceui.pixiv.download.copy.CopyDestination
import ceui.pixiv.download.copy.CopyOutput
import com.google.gson.Gson
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

@Keep
data class ArtifactCheckpoint(val version: Int = 1, val bytes: Long = 0, val sha256: String = "",
    val receipt: String? = null, val skipped: Boolean = false, val outputCommitted: Boolean = false,
    val domain: String = "")

/** Final publication shared by generated documents/media. A parent owns the generated input. */
class ArtifactPublisher(private val phases: ContentPhaseRunner, private val output: CopyOutput) {
    private val gson = Gson()
    fun checkpoint(context: ContentPlanContext): ArtifactCheckpoint =
        runCatching { gson.fromJson(context.checkpoint, ArtifactCheckpoint::class.java) }
            .getOrNull()?.takeIf { it.version == 1 } ?: ArtifactCheckpoint()
    suspend fun reconcile(plan: ContentPlanStore.Plan, destination: CopyDestination,
        recorded: suspend (Uri) -> Unit = {}): Boolean {
        val saved = runCatching { gson.fromJson(plan.checkpoint, ArtifactCheckpoint::class.java) }.getOrNull() ?: return false
        if (saved.version != 1) return false
        if (saved.skipped && saved.outputCommitted) return true
        val receipt = saved.receipt ?: return false
        if (saved.bytes <= 0 || saved.sha256.isBlank()) return false
        return phases.run(plan.id, "reconcile-output", "copy-receipt:$receipt", ContentQueueStore.Pool.COPY) {
            if (!output.matches(receipt, saved.sha256, saved.bytes)) false
            else { output.finishRecovered(receipt, destination); recorded(Uri.parse(receipt)); true }
        }
    }

    suspend fun publish(context: ContentPlanContext, source: File, destination: CopyDestination,
        recorded: suspend (Uri) -> Unit = {}): ArtifactCheckpoint = phases.run(context.plan.id, "publish",
        "copy-output:" + MessageDigest.getInstance("SHA-256").digest(
            gson.toJson(destination.copy(policy = "", mime = "")).toByteArray(Charsets.UTF_8)
        ).joinToString("") { "%02x".format(it) }, ContentQueueStore.Pool.COPY) {
        var saved = checkpoint(context)
        if (!source.isFile || source.length() <= 0) throw IOException("Generated artifact is missing or empty")
        val hash = digest(source)
        if (saved.bytes != source.length() || saved.sha256 != hash) {
            if (saved.receipt != null) throw IOException("Generated artifact changed after its output was opened")
            saved = saved.copy(bytes = source.length(), sha256 = hash)
            context.checkpoint(gson.toJson(saved))
        }
        context.checkStop()
        context.commit {
            val receipt = saved.receipt
            if (receipt != null && output.matches(receipt, hash, saved.bytes)) {
                output.finishRecovered(receipt, destination)
            } else if (!saved.skipped) {
                val handle = receipt?.let { output.resume(it, destination) } ?: output.open(destination)
                if (handle == null) saved = saved.copy(skipped = true)
                else try {
                    saved = saved.copy(receipt = handle.uri.toString())
                    context.checkpoint(gson.toJson(saved)) // exact provider identity before the first byte
                    handle.stream.use { out -> source.inputStream().use { it.copyTo(out) } }
                    handle.onFinish()
                } catch (error: Throwable) {
                    try { handle.onAbort() } catch (abort: Throwable) { if (abort !== error) error.addSuppressed(abort) }
                    throw error
                }
            }
            saved = saved.copy(outputCommitted = true)
            context.checkpoint(gson.toJson(saved))
            if (!saved.skipped) saved.receipt?.let { recorded(Uri.parse(it)) }
        }
        saved
    }
    companion object {
        suspend fun digest(file: File): String {
            val hash = MessageDigest.getInstance("SHA-256")
            file.inputStream().buffered().use { input ->
                val buffer = ByteArray(32 * 1024)
                while (true) { currentCoroutineContext().ensureActive(); val n = input.read(buffer)
                    if (n < 0) break; hash.update(buffer, 0, n) }
            }
            return hash.digest().joinToString("") { "%02x".format(it) }
        }
    }
}
