package ceui.lisa.file

import android.content.Context
import ceui.lisa.R
import ceui.lisa.activities.Shaft
import ceui.pixiv.api.model.Illust
import ceui.lisa.utils.Common
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.queue.ContentPhases
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import kotlinx.coroutines.runBlocking
import ceui.pixiv.download.UgoiraDownloadRecord
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.model.Bucket
import ceui.pixiv.download.model.RelativePath
import timber.log.Timber
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileInputStream

/**
 * Legacy "copy local file into user storage" helpers. Rewritten to route every
 * write through the unified download facade.
 */
object OutPut {

    /**
     * 把动图成品 [from] 拷进用户存储。[asVideo] 说的是**这个文件实际是什么**(mp4 / GIF),
     * 不是设置里选了什么 —— 压制失败时保存链路会降级出 GIF,那时候名字和 mime 都得跟着是 GIF。
     */
    @JvmStatic
    fun outPutUgoira(context: Context, from: File, illust: Illust, asVideo: Boolean) {
        try {
            val saved = runBlocking(kotlinx.coroutines.Dispatchers.IO) {
                val outputs = ceui.pixiv.download.copy.QueuedLocalOutputs.get(context)
                val cache = ceui.pixiv.ui.bulk.UgoiraCacheAccess.acquireRead()
                val ticket = outputs.enqueue("legacy-ugoira:${illust.id}:${from.absolutePath}:${from.length()}:${from.lastModified()}",
                    from, cacheReference = cache) {
                    val item = DownloadItems.ugoira(illust, asVideo)
                    ceui.pixiv.download.copy.LocalOutputRequest(type = ceui.pixiv.download.copy.QueuedLocalOutputs.UGOIRA,
                        subjectId = illust.id, contextJson = Shaft.sGson.toJson(illust), asVideo = asVideo,
                        sourcePath = from.absolutePath, sourceBytes = from.length(), sourceModified = from.lastModified(),
                        destination = outputs.freeze(item.bucket, DownloadsRegistry.downloads.resolvePath(item), item.mime))
                }
                val success = outputs.await(ticket)
                Triple(success, if (success) outputs.receipt(ticket).skipped else false, ticket.status.value.error)
            }
            if (saved.first) Common.showToast(string(if (saved.second) R.string.save_gif_exists else R.string.save_gif_success))
            else Common.showToast(string(R.string.save_gif_failed, saved.third ?: "请在下载队列中查看失败原因"))
        } catch (t: Throwable) {
            Timber.e(t, "outPutUgoira failed")
            Common.showToast(string(R.string.save_gif_failed, errMsg(t)))
        }
    }

    /** @return 是否真的写进了用户存储；失败时已 toast 具体错误，调用方不要再报成功。 */
    @JvmStatic
    fun outPutBackupFile(context: Context, from: File, fileName: String): Boolean {
        // 备份文件本身就是 JSON（Shaft-Backup.json），之前写成 application/zip 会让
        // MediaStore 按 MIME 给文件补一个 .zip 后缀，看起来像压缩包实则不是（#949）。
        // 目录和文件名都按用户在「下载路径」里给 Backup 桶配的模板走，不再硬编码；
        // [fileName] 只作为模板的 {title}（区分设置备份 / 浏览历史 / 屏蔽记录 / 词典）。
        return writeRawPath(Bucket.Backup, DownloadItems.backupDestination(fileName), "application/json", from, R.string.save_backup_failed)
    }

    private fun writeRawPath(bucket: Bucket, path: RelativePath, mime: String, from: File, failedMsgId: Int): Boolean = try {
        runBlocking(kotlinx.coroutines.Dispatchers.IO) {
            val outputs = ceui.pixiv.download.copy.QueuedLocalOutputs.get(Shaft.getContext())
            val cache = ceui.pixiv.ui.bulk.UgoiraCacheAccess.acquireRead()
            val ticket = outputs.enqueue("backup-file:${from.absolutePath}:${from.length()}:${from.lastModified()}", from,
                cacheReference = cache) {
                ceui.pixiv.download.copy.LocalOutputRequest(type = ceui.pixiv.download.copy.QueuedLocalOutputs.BACKUP, subjectId = 0,
                    sourcePath = from.absolutePath, sourceBytes = from.length(), sourceModified = from.lastModified(),
                    destination = outputs.freeze(bucket, path, mime))
            }
            outputs.await(ticket)
        }
    } catch (t: Throwable) {
        Timber.e(t, "OutPut.writeRaw failed (bucket=$bucket path=${path.joinTo()})")
        Common.showToast(string(failedMsgId, errMsg(t)))
        false
    }

    private fun string(id: Int, vararg args: Any): String =
        Shaft.getContext().getString(id, *args)

    private fun errMsg(t: Throwable): String = t.message ?: t.javaClass.simpleName
}

