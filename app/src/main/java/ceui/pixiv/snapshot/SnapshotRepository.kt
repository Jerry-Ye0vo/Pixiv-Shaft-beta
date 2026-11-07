package ceui.pixiv.snapshot

import android.content.Context
import android.net.Uri
import androidx.documentfile.provider.DocumentFile
import ceui.pixiv.api.model.Illust
import ceui.pixiv.ui.bulk.UgoiraFrames
import ceui.pixiv.ui.bulk.readFramesDir
import ceui.pixiv.ui.detail.OfflineUgoira
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File

data class SnapshotSummary(
    val manifest: SnapshotManifest,
    val fileCount: Int,
    val totalSize: Long,
    val coverFile: File?,
)

data class SnapshotViewerData(
    val snapshotDir: File,
    val manifest: SnapshotManifest,
    val illust: Illust,
    val assets: Map<String, String>,
    val comments: SnapshotComments?,
    /** 动图的离线帧序列（[SNAPSHOT_UGOIRA_DIR]）；静图恒为 null。加载时在 IO 上读好，渲染侧不碰磁盘。 */
    val ugoiraFrames: UgoiraFrames? = null,
) {
    /** 交给播放器 / 大图页的离线动图源；[isAuto] 决定点开大图时按哪个库回读快照。 */
    fun offlineUgoira(isAuto: Boolean): OfflineUgoira? =
        ugoiraFrames?.let { OfflineUgoira(manifest.snapshotId, isAuto, it) }

    /**
     * 第 [index] 页在快照里的那份文件 + 它的相对路径。
     *
     * 快照一页只存一份文件，存的是 large 还是 original 由生成时的选项决定；而消费方按哪一档
     * 分辨率来问是它自己决定的（大图页恒问 ORIGINAL）。所以这里把该页**所有尺寸变体**依次拿去
     * assets 里查，命中即用——这是「第 i 页的本地文件是哪个」的唯一答案，生成、校验、渲染、
     * 大图页都只许问它，不许各自再拼一遍 URL。
     *
     * p0 额外兜一层 manifest.coverPath：老快照的 assets 里可能只有当初那一个 URL。
     */
    private fun pageAsset(index: Int): Pair<File, String>? {
        val rels = illust.snapshotPageVariantUrls(index).mapNotNull { assets[it] } +
            listOfNotNull(manifest.coverPath?.takeIf { index == 0 })
        rels.forEach { rel ->
            val file = runCatching { safeResolve(snapshotDir, rel) }.getOrNull()
            if (file != null && file.isFile) return file to rel
        }
        return null
    }

    fun pageFile(index: Int): File? = pageAsset(index)?.first

    fun pageLocalUrl(index: Int): String? =
        pageAsset(index)?.let { (_, rel) -> snapshotLocalUrl(manifest.snapshotId, rel) }
}

/**
 * 私有快照库：
 * files/ShaftSnapshots/<snapshotId>/
 *
 * 目录即快照，ZIP/SAF 只做一次性导入导出，不持久化任何 URI。
 */
object SnapshotRepository {

    /**
     * 快照库根目录。**只算路径，不建目录**：它在 Glide 的图片加载路径上被调用
     * （见 snapshotAssetFile），顺手 mkdirs 等于每加载一张快照图多一次 stat + 可能的 mkdir
     * 系统调用。建目录是写入方的事，见 [createSnapshotDir] / [import]。
     */
    fun root(context: Context): File = File(context.filesDir, SNAPSHOT_ROOT_DIR)

    fun dir(context: Context, snapshotId: String): File = File(root(context), requireSnapshotId(snapshotId))

    fun createSnapshotDir(context: Context, snapshotId: String): File {
        val target = dir(context, snapshotId)
        target.mkdirs()
        return target
    }

    fun list(context: Context): List<SnapshotSummary> {
        val root = root(context)
        return root.listFiles()
            ?.filter { it.isDirectory }
            ?.mapNotNull { snapshotDir ->
                val manifest = SnapshotValidator.readJson<SnapshotManifest>(File(snapshotDir, SNAPSHOT_MANIFEST))
                    ?: return@mapNotNull null
                // 「目录名即快照 ID」是这个库的根本不变式：卡片上的所有操作(打开/导出/删除)
                // 都拿 manifest.snapshotId 反解目录。名字对不上的目录(导入中途掉电留下的
                // 备份等)必须当作不存在，否则会出现一张指向别人的重复卡片,删它删掉的是正主。
                if (snapshotDir.name != manifest.snapshotId) return@mapNotNull null
                // 早期快照/手改 manifest 可能没记 R 级和页数；用 illust.json 回补，
                // 保证老卡片也能显示左上角 R 级与 P 角标（fileCount 是整目录文件数，不是页数）。
                val displayManifest = manifest.backfillDisplayFields(snapshotDir)
                val coverFile = displayManifest.coverPath?.let { rel -> runCatching { safeResolve(snapshotDir, rel) }.getOrNull()?.takeIf { f -> f.isFile } }
                // 文件数/体积生成时就写进 manifest 了，列表页(三个 Tab、每次 onResume 都刷)
                // 没必要再把每个快照目录整个 walk 一遍 —— 那是 O(全库文件数) 次 stat。
                // 只有老快照/manifest 没记的情况才退回真扫。
                var fileCount = manifest.fileCount
                var totalSize = manifest.totalSize
                if (fileCount <= 0 || totalSize <= 0L) {
                    fileCount = 0
                    totalSize = 0L
                    snapshotDir.walkTopDown().forEach { file ->
                        if (file.isFile) {
                            fileCount++
                            totalSize += file.length()
                        }
                    }
                }
                SnapshotSummary(displayManifest, fileCount, totalSize, coverFile)
            }
            ?.sortedByDescending { it.manifest.createdAt }
            ?: emptyList()
    }

    fun readManifest(context: Context, snapshotId: String): SnapshotManifest? =
        SnapshotValidator.readJson(File(dir(context, snapshotId), SNAPSHOT_MANIFEST))

    /** Synchronous compatibility API; existing UI callers already invoke it on Dispatchers.IO. */
    fun delete(context: Context, snapshotId: String): Boolean {
        dir(context, snapshotId) // validate before admission, retaining the old path error contract
        return try {
            kotlinx.coroutines.runBlocking(Dispatchers.IO) {
                val operations = QueuedSnapshotOperations.get(context)
                operations.await(operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.DELETE,
                    snapshotId = snapshotId))).existed
            }
        } catch (failed: Exception) {
            timber.log.Timber.w(failed, "Snapshot deletion failed")
            false
        }
    }

    fun loadViewerData(context: Context, snapshotId: String): SnapshotViewerData {
        val snapshotDir = dir(context, snapshotId)
        val manifest = SnapshotValidator.readJson<SnapshotManifest>(File(snapshotDir, SNAPSHOT_MANIFEST))
            ?: throw SnapshotException("快照不存在或 manifest 损坏: $snapshotId")
        return loadViewerData(context, snapshotId, manifest)
    }

    internal fun loadViewerData(
        context: Context,
        snapshotId: String,
        manifest: SnapshotManifest,
    ): SnapshotViewerData {
        val snapshotDir = dir(context, snapshotId)
        val illust = SnapshotValidator.readJson<Illust>(File(snapshotDir, SNAPSHOT_ILLUST_JSON))
            ?: throw SnapshotException("快照缺少 illust.json: $snapshotId")
        val assets = SnapshotValidator.readJson<SnapshotAssets>(File(snapshotDir, SNAPSHOT_ASSETS_JSON))?.assets ?: emptyMap()
        val comments = if (manifest.includeComments) {
            SnapshotValidator.readJson<SnapshotComments>(File(snapshotDir, SNAPSHOT_COMMENTS_JSON))
        } else null
        val ugoiraFrames = if (illust.isGif()) {
            readFramesDir(File(snapshotDir, SNAPSHOT_UGOIRA_DIR), false)
        } else null
        return SnapshotViewerData(snapshotDir, manifest, illust, assets, comments, ugoiraFrames)
    }

    suspend fun export(context: Context, snapshotId: String, uri: Uri): Unit = withContext(Dispatchers.IO) {
        val manifest = readManifest(context, snapshotId) ?: throw SnapshotException("快照不存在: $snapshotId")
        val hash = ceui.pixiv.download.queue.ArtifactPublisher.digest(File(dir(context, snapshotId), SNAPSHOT_MANIFEST))
        val operations = QueuedSnapshotOperations.get(context)
        operations.await(operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.EXPORT,
            snapshotId = snapshotId, uri = uri.toString(), fileName = manifest.safeExportFileName(), manifestHash = hash)))
        Unit
    }

    suspend fun exportToDirectory(context: Context, snapshotId: String, treeUri: Uri): Unit = withContext(Dispatchers.IO) {
        val manifest = readManifest(context, snapshotId) ?: throw SnapshotException("快照不存在: $snapshotId")
        val hash = ceui.pixiv.download.queue.ArtifactPublisher.digest(File(dir(context, snapshotId), SNAPSHOT_MANIFEST))
        val operations = QueuedSnapshotOperations.get(context)
        operations.await(operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.EXPORT_TREE,
            snapshotId = snapshotId, tree = treeUri.toString(), fileName = manifest.safeExportFileName(), manifestHash = hash)))
        Unit
    }

    suspend fun import(context: Context, uri: Uri): SnapshotManifest = withContext(Dispatchers.IO) {
        val operations = QueuedSnapshotOperations.get(context)
        val result = operations.await(operations.enqueue(SnapshotOperationRequest(operation = QueuedSnapshotOperations.IMPORT,
            snapshotId = null, uri = uri.toString())))
        com.google.gson.Gson().fromJson(requireNotNull(result.manifestJson), SnapshotManifest::class.java)
    }

    private fun SnapshotManifest.backfillDisplayFields(snapshotDir: File): SnapshotManifest {
        if (xRestrict != null && pageCount != null && pageCount > 0) return this
        val illust = SnapshotValidator.readJson<Illust>(File(snapshotDir, SNAPSHOT_ILLUST_JSON)) ?: return this
        return copy(
            xRestrict = xRestrict ?: illust.x_restrict,
            pageCount = if (pageCount != null && pageCount > 0) pageCount else illust.page_count.coerceAtLeast(1)
        )
    }

}

