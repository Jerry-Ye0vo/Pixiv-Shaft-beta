package ceui.pixiv.db

import androidx.room.Dao
import androidx.room.Entity
import androidx.room.Query
import androidx.room.Transaction

/** Last authoritative illustration bookmark observation, independent of whether a shelf is mirrored. */
@Entity(tableName = "bookmark_knowledge", primaryKeys = ["ownerUid", "illustId"])
data class BookmarkKnowledgeEntity(val ownerUid: Long, val illustId: Long, val bookmarked: Boolean)

@Dao
interface BookmarkKnowledgeDao {
    @Query("INSERT OR REPLACE INTO bookmark_knowledge(ownerUid,illustId,bookmarked) " +
        "SELECT :uid,:id,:liked WHERE EXISTS(SELECT 1 FROM user_table WHERE userID=:uid)")
    fun rememberIfOwnerExists(uid: Long, id: Long, liked: Boolean)

    @Transaction
    fun remember(rows: List<BookmarkKnowledgeEntity>) {
        rows.forEach { rememberIfOwnerExists(it.ownerUid, it.illustId, it.bookmarked) }
    }

    @Query("SELECT * FROM bookmark_knowledge WHERE ownerUid=:ownerUid AND illustId IN (:ids)")
    fun knownAmong(ownerUid: Long, ids: List<Long>): List<BookmarkKnowledgeEntity>

    @Query("DELETE FROM bookmark_knowledge WHERE ownerUid=:ownerUid")
    fun deleteOwner(ownerUid: Long)
}
