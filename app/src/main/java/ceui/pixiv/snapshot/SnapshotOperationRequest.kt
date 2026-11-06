package ceui.pixiv.snapshot

import androidx.annotation.Keep

@Keep
data class SnapshotOperationRequest(val version: Int = 1, val operation: String, val snapshotId: String?,
    val uri: String? = null, val tree: String? = null, val fileName: String? = null, val manifestHash: String? = null)
@Keep
data class SnapshotOperationState(val version: Int = 1, val manifestJson: String? = null,
    val files: Map<String, SnapshotResource> = emptyMap(), val prepared: Boolean = false, val existed: Boolean = false)
