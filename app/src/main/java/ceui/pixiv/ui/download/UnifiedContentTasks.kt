package ceui.pixiv.ui.download

import android.content.Context
import ceui.lisa.activities.Shaft
import ceui.lisa.update.ApkDownloadManager
import ceui.pixiv.download.copy.QueuedCopies
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.novel.reader.export.ExportFormat
import ceui.pixiv.ui.novel.reader.export.QueuedNovelGroups
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
import org.json.JSONObject

internal enum class TaskOwner { PLAN, COPY, APK }
internal data class ContentTaskRow(val id: String, val owner: TaskOwner, val category: String,
    val title: String, val state: State?, val phase: String = "", val error: String? = null,
    val bytes: Long = 0, val total: Long = -1, val needsFormat: Boolean = false, val group: Boolean = false) {
    val active get() = state !in setOf(State.SUCCEEDED, State.CANCELLED)
    val running get() = state in setOf(State.RUNNING, State.COMMITTED)
    val resumable get() = state in setOf(State.QUEUED, State.PAUSED, State.FAILED)
}

/** The existing artwork library remains separate from bounded recent task receipts. */
internal fun showContentParent(family: String, hasParent: Boolean) = !hasParent && !family.startsWith("ugoira-batch:")
internal fun resolveUnifiedToolbarAction(legacy: ActiveToolbarAction, hasLegacy: Boolean,
    rows: List<ContentTaskRow>): ActiveToolbarAction {
    val active = rows.filter { it.active }
    return when {
        active.any { it.running } -> ActiveToolbarAction.PAUSE
        !hasLegacy && active.isNotEmpty() -> if (active.all { it.state == State.FAILED }) ActiveToolbarAction.RETRY else ActiveToolbarAction.RESUME
        legacy == ActiveToolbarAction.RETRY && active.any { it.state != State.FAILED } -> ActiveToolbarAction.RESUME
        else -> legacy
    }
}

/** UI projection and commands only; every byte worker stays with its existing process owner. */
internal class UnifiedContentTasks(context: Context) {
    private val app = context.applicationContext
    private val plans = ContentPlanQueue.get(app)
    private val copies = QueuedCopies.get(app)
    private val apk = (app as Shaft).apkDownloadManager
    private data class Metadata(val title: String, val category: String, val visible: Boolean)
    private val metadata = java.util.concurrent.ConcurrentHashMap<String, Metadata>()
    private fun info(status: ContentPlanStatus): Metadata = metadata.getOrPut(status.id) {
        val plan = plans.store.get(status.id)
        val json = runCatching { JSONObject(plan?.request ?: "{}") }.getOrDefault(JSONObject())
        val category = when (status.kind) {
            "novel-export", "novel-series-group" -> "小说"
            "ugoira-export" -> "动图"
            "model-install" -> "模型"
            else -> when { status.kind.contains("snapshot") -> "离线快照"
                status.kind.contains("model") -> "模型"
                status.kind.contains("ugoira") -> "动图"
                else -> "本地保存" }
        }
        val nested = (json.optString("novelJson").takeIf { it.isNotBlank() } ?: json.optString("illustJson"))
            .takeIf { it.isNotBlank() }?.let { runCatching { JSONObject(it) }.getOrNull() }
        val name = json.optJSONObject("merged")?.optString("title")
            ?: nested?.optString("title") ?: json.optJSONArray("series")?.optJSONObject(0)?.optString("title")
            ?: json.optJSONObject("destination")?.optJSONArray("segments")?.let { it.optString(it.length() - 1) }
            ?: json.optString("asset").takeIf { it.isNotBlank() }
            ?: json.optString("snapshotId").takeIf { it.isNotBlank() }
            ?: status.family.substringBeforeLast(':').takeLast(64)
        Metadata(name.ifBlank { category }, category,
            showContentParent(status.family, plan?.let { plans.store.parentState(it.family) != null } ?: false))
    }
    val rows: Flow<List<ContentTaskRow>> = combine(plans.states, copies.states, apk.state) { parents, files, update ->
        withContext(Dispatchers.IO) {
            val parentRows = parents.values.mapNotNull { status ->
                val info = info(status)
                if (!info.visible) null else ContentTaskRow(status.id, TaskOwner.PLAN, info.category, info.title,
                    status.state, status.phase, status.error, status.bytes, status.total,
                    status.kind == "novel-series-group" && status.state == State.PAUSED &&
                        plans.store.selectedFormat(status.id) == null &&
                        (status.phase == "await-format" || runCatching {
                            JSONObject(plans.store.get(status.id).checkpoint).optBoolean("awaitingFormat") }.getOrDefault(false)),
                    group = status.kind == "novel-series-group")
            }
            val copyRows = files.values.map { row -> ContentTaskRow(row.id, TaskOwner.COPY, "图片保存",
                row.request?.destination?.relativePath()?.filename ?: "本地图片", row.state, error = row.error) }
            val updateRows = if (update.phase == ApkDownloadManager.Phase.IDLE) emptyList() else listOf(ContentTaskRow(
                "apk-update", TaskOwner.APK, "应用更新", update.versionName ?: update.tag ?: "APK",
                when (update.phase) {
                    ApkDownloadManager.Phase.RUNNING, ApkDownloadManager.Phase.MERGING -> State.RUNNING
                    ApkDownloadManager.Phase.COMPLETED -> State.SUCCEEDED
                    ApkDownloadManager.Phase.FAILED -> State.FAILED
                    else -> State.PAUSED
                }, if (update.phase == ApkDownloadManager.Phase.MERGING) "合并安装包" else "下载安装包",
                update.reason?.name, update.downloadedBytes, update.totalBytes))
            metadata.keys.retainAll(parents.keys)
            parentRows + copyRows + updateRows
        }
    }.conflate()
    fun pauseAll() { commands.launch { rows.first().filter { it.active && it.state in setOf(State.RUNNING, State.QUEUED) }.forEach { pause(it) } } }
    fun resumeAll(retryOnly: Boolean = false) { commands.launch {
        rows.first().filter { it.active && it.resumable && (!retryOnly || it.state == State.FAILED) }.forEach { resume(it) }
    } }
    fun cancelAll() { commands.launch { rows.first().filter { it.active }.forEach { cancel(it) } } }
    fun finishGroup(row: ContentTaskRow) { commands.launch {
        plans.ticket(row.id)?.let { plans.finishByUser(it); if (row.resumable && !row.needsFormat) plans.resume(it.id) }
    } }
    suspend fun pause(row: ContentTaskRow) = withContext(Dispatchers.IO) {
        when (row.owner) {
            TaskOwner.PLAN -> { plans.ticket(row.id)?.pause(); Unit }
            TaskOwner.COPY -> { copies.ticket(row.id)?.pause(); Unit }
            TaskOwner.APK -> apk.pause()
        }
    }
    suspend fun resume(row: ContentTaskRow, format: ExportFormat? = null) = withContext(Dispatchers.IO) {
        when (row.owner) {
            TaskOwner.PLAN -> if (format != null) plans.ticket(row.id)?.let { QueuedNovelGroups.get(app).chooseFormat(it, format) }
                else if (!row.needsFormat) plans.resume(row.id) else Unit
            TaskOwner.COPY -> copies.resume(row.id)
            TaskOwner.APK -> apk.resume()
        }; Unit
    }
    suspend fun cancel(row: ContentTaskRow) = withContext(Dispatchers.IO) {
        when (row.owner) {
            TaskOwner.PLAN -> { plans.ticket(row.id)?.cancel(); Unit }
            TaskOwner.COPY -> { copies.ticket(row.id)?.cancel(); Unit }
            TaskOwner.APK -> apk.cancel()
        }
    }
    companion object { private val commands = CoroutineScope(SupervisorJob() + Dispatchers.IO) }
}
