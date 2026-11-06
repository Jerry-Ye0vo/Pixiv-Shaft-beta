package ceui.pixiv.download.copy

import android.net.Uri
import ceui.pixiv.download.config.OverwritePolicy
import ceui.pixiv.download.config.StorageChoice
import ceui.pixiv.download.model.RelativePath

/** Primitive-only v1 payload: no live backend, stream, Coroutine or Activity is serialized. */
data class CopyDestination(
    val storage: String,
    val collection: String? = null,
    val treeUri: String? = null,
    val segments: List<String>,
    val mime: String,
    val policy: String,
) {
    fun storageChoice(): StorageChoice = when (storage) {
        "media" -> StorageChoice.MediaStore(StorageChoice.MediaStore.Collection.valueOf(requireNotNull(collection)))
        "saf" -> StorageChoice.Saf(Uri.parse(requireNotNull(treeUri)))
        "cache" -> StorageChoice.AppCache
        else -> error("Unknown copy storage: $storage")
    }
    fun relativePath() = RelativePath(segments)
    fun overwrite() = OverwritePolicy.valueOf(policy)
    companion object {
        fun capture(storage: StorageChoice, path: RelativePath, mime: String, policy: OverwritePolicy): CopyDestination = when (storage) {
            is StorageChoice.MediaStore -> CopyDestination("media", storage.collection.name, segments = path.segments, mime = mime, policy = policy.name)
            is StorageChoice.Saf -> CopyDestination("saf", treeUri = storage.treeUri.toString(), segments = path.segments, mime = mime, policy = policy.name)
            StorageChoice.AppCache -> CopyDestination("cache", segments = path.segments, mime = mime, policy = policy.name)
        }
    }
}

data class CopyRequest(
    val kind: String,
    val subjectId: Long,
    val page: Int,
    val sourcePath: String,
    val sourceSize: Long,
    val sourceModified: Long,
    val contextJson: String,
    val destination: CopyDestination,
    val submittedAt: Long = System.currentTimeMillis(),
) {
    val family: String get() = "$kind:$subjectId:$page:$sourcePath"
    fun validate() {
        require(kind.matches(Regex("[A-Za-z0-9_.-]+"))) { "Invalid copy kind" }
        require(!sourcePath.isNullOrBlank() && java.io.File(sourcePath).isAbsolute) { "Invalid copy input path" }
        destination.storageChoice(); destination.relativePath(); destination.overwrite()
        require(!destination.mime.isNullOrBlank()) { "Missing copy MIME" }
    }
}

data class CopyIntent(
    val version: Int = 1,
    val request: CopyRequest,
    val snapshotName: String? = null,
    val sourceHash: String? = null,
    val sourceBytes: Long = 0,
    val fallbackSnapshot: String? = null,
    val receiptUri: String? = null,
    val skipped: Boolean = false,
    val outputCommitted: Boolean = false,
)
