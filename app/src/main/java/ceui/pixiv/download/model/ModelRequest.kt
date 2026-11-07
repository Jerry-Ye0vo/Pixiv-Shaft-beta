package ceui.pixiv.download.model

import androidx.annotation.Keep
import java.io.File
import java.security.MessageDigest

/** Frozen primitive schema: replay does not depend on a current enum, proxy or screen. */
@Keep
data class ModelRequest(
    val version: Int = 1,
    val directory: String,
    val asset: String,
    val required: List<String>,
    val operation: String,
    val source: String,
    val timeoutSeconds: Long = 60,
    /** A pinned standalone model. Null retains the legacy ZIP installation protocol. */
    val rawFile: String? = null,
    val expectedSha256: String? = null,
) {
    val family: String get() = "model:$directory/$asset"
    fun validate() {
        require(version == 1 && operation in setOf(DOWNLOAD, IMPORT, DELETE))
        require(directory in DIRECTORIES && segment(asset))
        require(required.isNotEmpty() && required.distinct().size == required.size)
        require(required.all { relative(it) && it != ModelInstaller.RECEIPT })
        require(timeoutSeconds in 1..600)
        rawFile?.let {
            require(segment(it) && required == listOf(it))
            require(expectedSha256?.matches(Regex("[a-f0-9]{64}")) == true)
        }
        if (operation == DOWNLOAD) require(source.startsWith("https://") || source.startsWith("http://"))
        if (operation == IMPORT) require(source.startsWith("content://") || source.startsWith("file://"))
    }
    fun target(files: File): File { validate(); return File(File(files, directory), asset) }
    companion object {
        const val DOWNLOAD = "download"; const val IMPORT = "import"; const val DELETE = "delete"
        private val DIRECTORIES = setOf("rembg-models", "rife-models", "manga-ocr-models", "comic-text-detector-models", "translation-models", "image-tagger-models")
        internal fun segment(value: String) = value.isNotBlank() && value != "." && value != ".." &&
            !value.contains('/') && !value.contains('\\') && !value.contains('\u0000')
        internal fun relative(value: String) = value.split('/').all { segment(it) }
        internal fun hash(value: String) = MessageDigest.getInstance("SHA-256")
            .digest(value.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
    }
}

@Keep
data class ModelCheckpoint(val version: Int = 1, val length: Long = 0, val sha256: String = "", val invalid: Boolean = false)

@Keep
data class ModelFileReceipt(val path: String, val length: Long, val sha256: String)

@Keep
data class ModelInstallReceipt(val version: Int = 1, val plan: String, val request: String,
    val source: String, val files: List<ModelFileReceipt>)
