package ceui.pixiv.snapshot

import ceui.pixiv.download.queue.ContentPhaseRunner
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.imageloader.OriginalFileLease
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

/** Snapshot localization is explicit work, including low-resolution images/avatars/stamps.
 * Ordinary thumbnail rendering remains independent. Exact URLs retain the requested quality.
 */
internal class SnapshotAssetTransfers(
    private val runner: ContentPhaseRunner,
    private val acquire: suspend (String) -> OriginalFileLease,
) {
    suspend fun copyUrl(url: String, target: File, original: Boolean = false, cached: File? = null) =
        withContext(Dispatchers.IO) {
            // Original-cache consumers need a pin while waiting for COPY; the shared broker supplies it.
            // Glide previews can be evicted before opening: retire that failed local stage and re-acquire.
            if (!original && cached != null) {
                try { copyFile(cached, target); return@withContext }
                catch (missing: FileNotFoundException) { if (cached.isFile) throw missing }
            }
            acquire(url).use { source -> copyFile(source.file, target) }
        }

    suspend fun copyFile(source: File, target: File) = runner.run("snapshot", "asset-copy",
        "snapshot-file:" + target.absolutePath, Pool.COPY) {
        atomicWrite(target) { output ->
            // Opening succeeds before eviction/unlink, so an immutable cache entry stays readable by fd.
            FileInputStream(source).use { input ->
                val expected = input.channel.size()
                if (expected <= 0) throw IOException("Snapshot input is empty")
                val buffer = ByteArray(32 * 1024)
                var copied = 0L
                val context = currentCoroutineContext()
                while (true) {
                    context.ensureActive()
                    val count = input.read(buffer)
                    if (count < 0) break
                    output.write(buffer, 0, count); copied += count
                }
                if (copied != expected || input.channel.size() != expected) throw IOException("Snapshot input changed")
            }
        }
    }

    suspend fun writeText(target: File, value: String) = runner.run("snapshot", "metadata",
        "snapshot-file:" + target.absolutePath, Pool.COPY) {
        writeTextClaimed(target, value)
    }

    /** Only for a caller that already owns COPY, including final parent manifest commit. */
    internal suspend fun writeTextClaimed(target: File, value: String) =
        atomicWrite(target) { it.write(value.toByteArray(Charsets.UTF_8)) }

    private suspend fun atomicWrite(target: File, write: suspend (FileOutputStream) -> Unit) {
        target.parentFile?.let { if (!it.isDirectory && !it.mkdirs()) throw IOException("Cannot create snapshot directory") }
        val temporary = File(target.parentFile, target.name + ".queue-" + UUID.randomUUID())
        try {
            FileOutputStream(temporary).use { output -> write(output); output.fd.sync() }
            currentCoroutineContext().ensureActive()
            if (!temporary.renameTo(target)) throw IOException("Cannot commit snapshot asset")
        } finally { temporary.delete() }
    }
}
