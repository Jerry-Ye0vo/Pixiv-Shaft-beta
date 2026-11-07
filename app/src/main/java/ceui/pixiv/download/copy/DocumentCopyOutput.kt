package ceui.pixiv.download.copy

import android.content.Context
import android.net.Uri
import androidx.documentfile.provider.DocumentFile
import ceui.pixiv.download.backend.StorageBackend
import java.io.IOException

/** Receipt-aware explicit document/tree destination. Never deletes a caller-selected document on failure. */
class DocumentCopyOutput(private val context: Context, private val document: String?, private val tree: String?,
    private val name: String, private val mime: String) : CopyOutput {
    private val verifier = AndroidCopyOutput(context) { error("Explicit document output has no Downloads preset") }
    private fun handle(uri: Uri): StorageBackend.WriteHandle {
        val stream = context.contentResolver.openOutputStream(uri, "wt") ?: throw IOException("Cannot open export document")
        return StorageBackend.WriteHandle(uri, stream, onFinish = {}, onAbort = {})
    }
    override fun open(destination: CopyDestination): StorageBackend.WriteHandle {
        val uri = if (document != null) Uri.parse(document) else {
            val parent = DocumentFile.fromTreeUri(context, Uri.parse(requireNotNull(tree)))
                ?: throw IOException("Cannot open export directory")
            parent.createFile(mime, name)?.uri ?: throw IOException("Cannot create export document")
        }
        return handle(uri)
    }
    override fun resume(uri: String, destination: CopyDestination): StorageBackend.WriteHandle? =
        try { handle(Uri.parse(uri)) } catch (_: java.io.FileNotFoundException) { null }
    override fun matches(uri: String, sha256: String, bytes: Long) = verifier.matches(uri, sha256, bytes)
    override fun finishRecovered(uri: String, destination: CopyDestination) {}
    override fun available(intent: CopyIntent): Boolean = intent.receiptUri?.let {
        runCatching { context.contentResolver.openInputStream(Uri.parse(it))?.use { stream -> stream.read() >= 0 } ?: false }.getOrDefault(false)
    } ?: false
}
