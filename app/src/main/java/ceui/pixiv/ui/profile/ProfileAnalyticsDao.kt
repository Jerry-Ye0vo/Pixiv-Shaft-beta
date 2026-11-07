package ceui.pixiv.ui.profile

import androidx.room.Dao
import androidx.room.Entity
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query

@Entity(tableName = "local_image_tags", primaryKeys = ["uri", "modelVersion"],
    indices = [androidx.room.Index(value = ["sha256", "modelVersion"])])
data class LocalImageTags(
    val uri: String, val modelVersion: String, val sourceStamp: String,
    val sha256: String, val tagsJson: String, val elapsedMs: Long, val updatedAt: Long,
)
internal data class ProfileDownloadRow(val sequence: Long, val fileName: String, val filePath: String,
    val illustGson: String?, val illustId: Long, val downloadTime: Long)
internal data class ProfileBookmarkRow(val targetId: Long, val payloadJson: String)
internal data class ProfileTagRow(val sequence: Long, val sha256: String, val tagsJson: String)

@Dao
abstract class ProfileAnalyticsDao {
    @Query("SELECT COALESCE(MAX(rowid),0) FROM illust_download_table")
    abstract fun downloadCeiling(): Long
    @Query("SELECT COUNT(*) FROM illust_download_table WHERE rowid<=:ceiling")
    abstract fun downloadCount(ceiling: Long): Int
    @Query("SELECT rowid AS sequence,fileName,filePath,illustGson,illustId,downloadTime " +
        "FROM illust_download_table WHERE rowid>:after AND rowid<=:ceiling ORDER BY rowid LIMIT :limit")
    internal abstract fun downloadBatch(after: Long, ceiling: Long, limit: Int): List<ProfileDownloadRow>
    @Query("SELECT targetId,payloadJson FROM bookmark_mirror_table " +
        "WHERE shelfKey=:shelfKey AND targetId>:after ORDER BY targetId LIMIT :limit")
    internal abstract fun bookmarkBatch(shelfKey: String, after: Long, limit: Int): List<ProfileBookmarkRow>
    @Query("SELECT * FROM local_image_tags WHERE uri=:uri AND modelVersion=:model LIMIT 1")
    abstract fun findTags(uri: String, model: String): LocalImageTags?
    @Query("SELECT * FROM local_image_tags WHERE sha256=:hash AND modelVersion=:model LIMIT 1")
    abstract fun findHash(hash: String, model: String): LocalImageTags?
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    abstract fun rememberTags(row: LocalImageTags)
    @Query("SELECT rowid AS sequence,sha256,tagsJson FROM local_image_tags WHERE modelVersion=:model " +
        "AND rowid>:after ORDER BY rowid LIMIT :limit")
    internal abstract fun tagBatch(model: String, after: Long, limit: Int): List<ProfileTagRow>
}
