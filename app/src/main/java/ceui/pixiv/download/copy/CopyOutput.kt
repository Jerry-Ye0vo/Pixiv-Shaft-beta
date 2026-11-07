package ceui.pixiv.download.copy

import ceui.pixiv.download.backend.StorageBackend

/** All operations run on IO while a COPY permit is held, except read-only receipt validation. */
interface CopyOutput {
    fun open(destination: CopyDestination): StorageBackend.WriteHandle?
    fun resume(uri: String, destination: CopyDestination): StorageBackend.WriteHandle?
    fun matches(uri: String, sha256: String, bytes: Long): Boolean
    fun finishRecovered(uri: String, destination: CopyDestination)
    fun available(intent: CopyIntent): Boolean
}
