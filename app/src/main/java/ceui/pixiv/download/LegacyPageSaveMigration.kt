package ceui.pixiv.download

import android.content.Context
import ceui.lisa.core.DownloadItem
import ceui.lisa.database.AppDatabase
import ceui.lisa.database.DownloadingEntity
import ceui.lisa.download.IllustDownload
import ceui.pixiv.api.model.Illust
import com.google.gson.Gson
import java.io.File
import java.util.UUID

/** One-time import of earlier Beta single-page saves, before Manager exposes restored tasks. */
object LegacyPageSaveMigration {
    @JvmStatic
    fun migrate(context: Context, database: AppDatabase, gson: Gson): Int {
        val journal = ImagePageSaveJournal(File(context.noBackupFilesDir, "image-page-saves.properties"))
        val entries = journal.read()
        if (entries.isEmpty()) return 0
        // Validate the entire snapshot before changing either store. An unreadable intent stays on disk.
        val items = entries.mapNotNull { entry ->
            val illust = requireNotNull(gson.fromJson(entry.illustJson, Illust::class.java))
            require(illust.id > 0 && !illust.isGif() &&
                entry.page in 0 until illust.page_count.coerceAtLeast(1))
            if (entry.page in RecordedPageProbe.usablePagesSince(context, illust.id, entry.queuedAt)) {
                null // The old saver finished writing before its journal acknowledgement.
            } else {
                DownloadItem(illust, entry.page).apply {
                    url = entry.imageUrl
                    showUrl = IllustDownload.getShowUrl(illust, entry.page)
                    uuid = UUID.nameUUIDFromBytes(
                        "${illust.id}:${entry.page}:${entry.queuedAt}:${entry.imageUrl}".toByteArray(Charsets.UTF_8)
                    ).toString()
                }
            }
        }
        var inserted = 0
        database.runInTransaction {
            for (item in items) {
                // Keep an existing live task and its uuid. A retry after a crash is idempotent,
                // including when the user changed naming templates between launches.
                val exists = database.openHelper.writableDatabase.query(
                    "SELECT taskGson FROM illust_downloading_table WHERE fileName = ? OR uuid = ?",
                    arrayOf(item.name, item.uuid),
                ).use { cursor ->
                    var matched = false
                    while (cursor.moveToNext()) {
                        val existing = requireNotNull(gson.fromJson(cursor.getString(0), DownloadItem::class.java))
                        check(existing.illust?.id == item.illust.id && existing.index == item.index &&
                            existing.url == item.url) {
                            "Pending download identity conflict: ${item.name}"
                        }
                        matched = true
                    }
                    matched
                }
                if (!exists) {
                    database.downloadDao().insertDownloading(DownloadingEntity().apply {
                        fileName = item.name
                        uuid = item.uuid
                        taskGson = gson.toJson(item)
                    })
                    inserted++
                }
            }
        }
        // Only acknowledge after Room commits. If interrupted here, the next import deduplicates.
        journal.write(emptyList())
        return inserted
    }
}
