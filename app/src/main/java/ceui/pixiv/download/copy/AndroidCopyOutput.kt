package ceui.pixiv.download.copy

import android.content.ContentValues
import android.content.Context
import android.media.MediaScannerConnection
import android.net.Uri
import android.os.Build
import android.provider.MediaStore
import ceui.pixiv.download.Downloads
import ceui.pixiv.download.backend.StorageBackend
import ceui.pixiv.download.config.OverwritePolicy
import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.security.MessageDigest

/** URI receipts are persisted before writes, allowing a restart to finalize/reuse the same output. */
class AndroidCopyOutput(context: Context, private val downloads: () -> Downloads) : CopyOutput {
    private val app = context.applicationContext
    override fun open(destination: CopyDestination): StorageBackend.WriteHandle? = downloads().openResolved(
        destination.storageChoice(), destination.relativePath(), destination.mime, destination.overwrite())

    override fun resume(uri: String, destination: CopyDestination): StorageBackend.WriteHandle? {
        val target = Uri.parse(uri)
        val stream = try {
            when (target.scheme) {
                "file" -> File(target.path ?: return null).takeIf { it.isFile }?.let { FileOutputStream(it, false) } ?: return null
                "content" -> app.contentResolver.openOutputStream(target, "wt") ?: return null
                else -> throw IOException("Unsupported copy receipt scheme")
            }
        } catch (_: FileNotFoundException) { return null }
        return StorageBackend.WriteHandle(target, stream,
            onFinish = { finishRecovered(uri, destination) },
            onAbort = {
                // Replace may reference a pre-existing user file; never delete that file on recovery abort.
                if (destination.overwrite() != OverwritePolicy.Replace) {
                    if (target.scheme == "file") File(requireNotNull(target.path)).delete()
                    else app.contentResolver.delete(target, null, null)
                }
            })
    }

    private fun input(uri: String): InputStream? {
        val target = Uri.parse(uri)
        return when (target.scheme) {
            "file" -> File(target.path ?: return null).takeIf { it.isFile }?.inputStream()
            "content" -> app.contentResolver.openInputStream(target)
            else -> null
        }
    }

    override fun matches(uri: String, sha256: String, bytes: Long): Boolean { return try {
        var count = 0L
        val digest = MessageDigest.getInstance("SHA-256")
        input(uri)?.use { stream ->
            val buffer = ByteArray(32 * 1024)
            while (true) {
                val read = stream.read(buffer)
                if (read < 0) break
                count += read
                if (count > bytes) return false
                digest.update(buffer, 0, read)
            }
        } ?: return false
        count == bytes && digest.digest().joinToString("") { "%02x".format(it.toInt() and 255) } == sha256
    } catch (_: IOException) { false } }

    override fun finishRecovered(uri: String, destination: CopyDestination) {
        val target = Uri.parse(uri)
        if (target.scheme == "file") {
            MediaScannerConnection.scanFile(app, arrayOf(requireNotNull(target.path)), arrayOf(destination.mime), null)
        } else if (target.authority == MediaStore.AUTHORITY && Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            val values = ContentValues().apply { put(MediaStore.MediaColumns.IS_PENDING, 0) }
            if (app.contentResolver.update(target, values, null, null) <= 0) throw IOException("Cannot finalize copy receipt")
        }
        // SAF files become visible by stream close; there is no MediaStore pending bit.
    }

    override fun available(intent: CopyIntent): Boolean { return try {
        val receipt = intent.receiptUri
        if (receipt == null) {
            downloads().existsResolved(intent.request.destination.storageChoice(), intent.request.destination.relativePath())
        } else {
            val uri = Uri.parse(receipt)
            if (uri.scheme == "file") File(uri.path ?: return false).let { it.isFile && it.length() >= intent.sourceBytes && it.length() > 0 }
            else app.contentResolver.openAssetFileDescriptor(uri, "r")?.use { fd ->
                if (fd.length >= 0) fd.length >= intent.sourceBytes && fd.length > 0
                else input(receipt)?.use { it.read() >= 0 } ?: false
            } ?: false
        }
    } catch (_: Exception) { false } }
}
