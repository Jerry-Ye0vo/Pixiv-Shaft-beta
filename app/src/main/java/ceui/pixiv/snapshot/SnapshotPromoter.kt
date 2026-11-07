package ceui.pixiv.snapshot

import android.content.Context
import com.google.gson.Gson

/** Promotion is a replayable local operation; final metadata shares the directory writer lease. */
object SnapshotPromoter {
    suspend fun promote(context: Context, autoId: String): SnapshotManifest {
        val operations = QueuedSnapshotOperations.get(context)
        val result = operations.await(operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.PROMOTE,
            snapshotId = autoId)))
        return Gson().fromJson(requireNotNull(result.manifestJson), SnapshotManifest::class.java)
    }
}
