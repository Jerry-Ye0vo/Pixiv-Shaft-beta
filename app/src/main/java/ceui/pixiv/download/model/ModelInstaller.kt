package ceui.pixiv.download.model

import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.util.zip.ZipInputStream
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

/** All methods run inside a caller-owned COPY stage. Paths and backups are private to one plan. */
internal class ModelInstaller(files: File, val request: ModelRequest, val plan: String, private val json: String,
    private val move: (File, File) -> Boolean = { from, to -> from.renameTo(to) }) {
    val target = request.target(files)
    private val token = ModelRequest.hash(plan)
    val staging = File(target.parentFile, ".${target.name}.$token.staging")
    val backup = File(target.parentFile, ".${target.name}.$token.old")
    private val gson = Gson()

    fun recover() {
        if (!backup.exists()) return
        if (!target.exists()) {
            if (!move(backup, target)) throw IOException("Cannot recover the previous model")
        } else if (receipt()?.plan != plan) {
            // Never remove a directory written by a different intention.
            throw IOException("Model recovery found an unrelated installation")
        }
    }

    suspend fun installed(): Boolean {
        val saved = receipt() ?: return false
        if (saved.version != 1 || saved.plan != plan || saved.request != ModelRequest.hash(json) ||
            saved.files.map { it.path }.sorted() != request.required.sorted()) return false
        return saved.files.all { entry ->
            val file = File(target, entry.path)
            file.isFile && file.length() > 0 && file.length() == entry.length && digest(file) == entry.sha256
        }
    }

    suspend fun prepare(source: File, sourceHash: String) {
        check(source.isFile && source.length() >= 4) { "Empty model package" }
        request.expectedSha256?.let { if (sourceHash != it) throw IOException("Model checksum does not match the pinned version") }
        if (request.rawFile != null) {
            remove(staging)
            if (!staging.mkdirs()) throw IOException("Cannot create model staging directory")
            FileOutputStream(File(staging, request.rawFile)).use { out ->
                source.inputStream().use { input ->
                    val buffer = ByteArray(32 * 1024)
                    while (true) {
                        currentCoroutineContext().ensureActive()
                        val count = input.read(buffer); if (count < 0) break
                        out.write(buffer, 0, count)
                    }
                }
                out.fd.sync()
            }
            writeReceipt(sourceHash)
            return
        }
        source.inputStream().use { input ->
            val magic = ByteArray(4)
            if (input.read(magic) != 4 || !magic.contentEquals(byteArrayOf(0x50, 0x4b, 3, 4)))
                throw IOException("Model package is not a ZIP")
        }
        remove(staging)
        if (!staging.mkdirs()) throw IOException("Cannot create model staging directory")
        var expanded = 0L
        var entries = 0
        val names = HashSet<String>()
        ZipInputStream(source.inputStream().buffered()).use { zip ->
            while (true) {
                currentCoroutineContext().ensureActive()
                val entry = zip.nextEntry ?: break
                if (++entries > 20_000) throw IOException("Too many model entries")
                val name = entry.name.removeSuffix("/")
                if (!ModelRequest.relative(name) || name == RECEIPT || !names.add(name))
                    throw IOException("Unsafe or duplicate model ZIP entry: ${entry.name}")
                val out = File(staging, name)
                if (!out.canonicalPath.startsWith(staging.canonicalPath + File.separator)) throw IOException("Model ZIP escapes staging")
                if (entry.isDirectory) {
                    if (!out.isDirectory && !out.mkdirs()) throw IOException("Cannot create model directory")
                } else {
                    out.parentFile?.mkdirs()
                    FileOutputStream(out).use { output ->
                        val buffer = ByteArray(32 * 1024)
                        while (true) {
                            currentCoroutineContext().ensureActive()
                            val count = zip.read(buffer)
                            if (count < 0) break
                            expanded += count
                            if (expanded > MAX_EXPANDED) throw IOException("Expanded model exceeds size limit")
                            output.write(buffer, 0, count)
                        }
                        output.fd.sync()
                    }
                }
                zip.closeEntry() // verifies CRC before any installed model is touched
            }
        }
        writeReceipt(sourceHash)
    }

    private suspend fun writeReceipt(sourceHash: String) {
        val files = request.required.sorted().map { path ->
            val file = File(staging, path)
            if (!file.isFile || file.length() == 0L) throw IOException("Required model file missing or empty: $path")
            ModelFileReceipt(path, file.length(), digest(file))
        }
        val receipt = ModelInstallReceipt(plan = plan, request = ModelRequest.hash(json), source = sourceHash, files = files)
        FileOutputStream(File(staging, RECEIPT)).use { output ->
            output.write(gson.toJson(receipt).toByteArray(Charsets.UTF_8)); output.fd.sync()
        }
    }

    /** Small final transaction; caller enters its non-cancellable parent commit immediately before. */
    fun install() {
        check(staging.isDirectory && File(staging, RECEIPT).isFile)
        recover()
        if (backup.exists()) throw IOException("Previous model backup still needs cleanup")
        val hadOld = target.exists()
        if (hadOld && !move(target, backup)) throw IOException("Cannot preserve previous model")
        if (!move(staging, target)) {
            if (hadOld && !move(backup, target)) throw IOException("Model installation and rollback failed; backup retained")
            throw IOException("Cannot commit validated model")
        }
    }

    fun delete() {
        recover()
        if (backup.exists()) throw IOException("Previous model backup still needs cleanup")
        if (target.exists() && !move(target, backup)) throw IOException("Cannot stage model deletion")
        // A durable delete receipt must live outside the directory being removed.
    }
    fun cleanup(success: Boolean) {
        remove(staging)
        if (success) remove(backup) else recover()
    }
    private fun receipt(): ModelInstallReceipt? = runCatching {
        gson.fromJson(File(target, RECEIPT).readText(), ModelInstallReceipt::class.java)
    }.getOrNull()
    companion object {
        const val RECEIPT = ".shaft-model-receipt.json"
        private const val MAX_EXPANDED = 2L * 1024 * 1024 * 1024
        fun remove(file: File) { if (file.exists() && !file.deleteRecursively()) throw IOException("Cannot remove owned model files") }
        suspend fun digest(file: File): String {
            val hash = MessageDigest.getInstance("SHA-256")
            file.inputStream().buffered().use { input ->
                val buffer = ByteArray(32 * 1024)
                while (true) {
                    currentCoroutineContext().ensureActive()
                    val n = input.read(buffer); if (n < 0) break
                    hash.update(buffer, 0, n)
                }
            }
            return hash.digest().joinToString("") { "%02x".format(it) }
        }
    }
}
