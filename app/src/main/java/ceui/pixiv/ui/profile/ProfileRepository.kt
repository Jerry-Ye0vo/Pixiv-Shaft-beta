package ceui.pixiv.ui.profile

import android.content.Context
import android.util.AtomicFile
import ceui.lisa.database.AppDatabase
import ceui.lisa.utils.Params
import ceui.pixiv.db.mirror.BookmarkShelf
import ceui.pixiv.db.mirror.MirrorContentType
import ceui.pixiv.db.mirror.MirrorRestrict
import com.google.gson.Gson
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale

internal class ProfileRepository(private val app: Context) {
    private val db get() = AppDatabase.getAppDatabase(app)
    suspend fun cached(uid: Long): ProfileStatistics? = withContext(Dispatchers.IO) {
        runCatching { snapshotFile(app, uid).openRead().bufferedReader().use {
            Gson().fromJson(it, ProfileStatistics::class.java)
        } }.getOrNull()?.takeIf { it.ownerUid == uid }
    }
    suspend fun analyze(uid: Long, progress: (Int, Int) -> Unit): ProfileStatistics = withContext(Dispatchers.IO) {
        val dao = db.profileAnalyticsDao()
        val accumulator = ProfileAccumulator()
        val shelves = MirrorContentType.entries.filter { it != MirrorContentType.USER }.flatMap { type ->
            MirrorRestrict.entries.map { BookmarkShelf(uid, type, it) }
        }
        var completedShelves = 0
        // Current mirror metadata takes precedence over stale JSON in old downloaded rows.
        if (uid > 0) for (shelf in shelves) {
            if ((db.bookmarkMirrorDao().findState(shelf.key)?.firstCompletedAt ?: 0) > 0) completedShelves++
            var after = 0L
            while (true) {
                currentCoroutineContext().ensureActive()
                val rows = dao.bookmarkBatch(shelf.key, after, BATCH)
                if (rows.isEmpty()) break
                for (row in rows) accumulator.accept(ProfileMetadataReader.read(row.payloadJson, row.targetId,
                    shelf.contentType == MirrorContentType.NOVEL), bookmark = true)
                after = rows.last().targetId
            }
        }
        val ceiling = dao.downloadCeiling()
        val expected = dao.downloadCount(ceiling)
        var after = 0L; var records = 0; var unidentified = 0
        val format = SimpleDateFormat("yyyy-MM-dd", Locale.ROOT)
        val calendar = Calendar.getInstance().apply {
            set(Calendar.HOUR_OF_DAY, 0); set(Calendar.MINUTE, 0); set(Calendar.SECOND, 0); set(Calendar.MILLISECOND, 0)
            add(Calendar.DAY_OF_MONTH, -29)
        }
        val firstDay = calendar.timeInMillis
        val days = linkedMapOf<String, Int>()
        repeat(30) { days[format.format(calendar.time)] = 0; calendar.add(Calendar.DAY_OF_MONTH, 1) }
        while (true) {
            currentCoroutineContext().ensureActive()
            val rows = dao.downloadBatch(after, ceiling, BATCH)
            if (rows.isEmpty()) break
            for (row in rows) {
                val metadata = ProfileMetadataReader.read(row.illustGson, row.illustId,
                    row.fileName.contains(Params.NOVEL_KEY))
                if (metadata.id <= 0) unidentified++ else accumulator.accept(metadata, bookmark = false)
                if (row.downloadTime >= firstDay) {
                    val day = format.format(row.downloadTime)
                    if (day in days) days[day] = days.getValue(day) + 1
                }
                records++
            }
            after = rows.last().sequence
            progress(records, expected)
        }
        ProfileStatistics(uid, System.currentTimeMillis(), records, unidentified, accumulator.overlap,
            accumulator.snapshot(0), accumulator.snapshot(1), accumulator.snapshot(2),
            days.map { ProfileDay(it.key, it.value) }, completedShelves).also { snapshot ->
            currentCoroutineContext().ensureActive()
            synchronized(cacheLock) {
                // Account removal deletes credentials before taking this same cache lock.
                // A late analysis cannot recreate a purged account's derived snapshot.
                if (uid <= 0 || db.downloadDao().getUserByUid(uid) != null) {
                    val file = snapshotFile(app, uid)
                    val stream = file.startWrite()
                    try { stream.write(Gson().toJson(snapshot).toByteArray(Charsets.UTF_8)); file.finishWrite(stream) }
                    catch (error: Exception) { file.failWrite(stream); throw error }
                }
            }
        }
    }
    companion object {
        const val BATCH = 100
        private val cacheLock = Any()
        private fun snapshotFile(context: Context, uid: Long): AtomicFile {
            val dir = File(context.filesDir, "local_profile_statistics").apply { mkdirs() }
            return AtomicFile(File(dir, "$uid.json"))
        }
        fun forget(context: Context, uid: Long) = synchronized(cacheLock) { snapshotFile(context, uid).delete() }
    }
}
