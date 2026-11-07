package ceui.pixiv.ui.common

import android.content.Context
import android.net.Uri
import ceui.pixiv.download.model.ModelRequest
import ceui.pixiv.download.model.QueuedModels
import ceui.pixiv.download.queue.ContentPlanTicket
import java.io.File

/** Compatibility facade. Process-owned queued workers own all bytes and installation state. */
abstract class ModelDownloadManager {
    protected abstract val storageSubDir: String
    protected abstract val logTag: String
    protected open val readTimeoutSeconds: Long = 60L

    fun modelDir(context: Context, model: DownloadableModel) = File(context.filesDir, "$storageSubDir/${model.assetDir}")
    open fun isModelReady(context: Context, model: DownloadableModel): Boolean = model.modelFiles.all {
        File(modelDir(context, model), it).let { file -> file.isFile && file.length() > 0 }
    }
    private fun request(model: DownloadableModel, operation: String, source: String) = QueuedModels.frozen(
        storageSubDir, model.assetDir, model.modelFiles, operation, source, readTimeoutSeconds)
    fun current(context: Context, model: DownloadableModel): ContentPlanTicket? =
        QueuedModels.get(context).latest("model:$storageSubDir/${model.assetDir}")
    fun startDownload(context: Context, model: DownloadableModel): ContentPlanTicket =
        QueuedModels.get(context).submit(request(model, ModelRequest.DOWNLOAD, requireNotNull(model.downloadUrl)))
    fun startImport(context: Context, model: DownloadableModel, uri: Uri): ContentPlanTicket =
        QueuedModels.get(context).submit(request(model, ModelRequest.IMPORT, uri.toString()))
    suspend fun observe(context: Context, ticket: ContentPlanTicket,
        progress: (Long, Long) -> Unit = { _, _ -> }): Boolean = QueuedModels.get(context).await(ticket, progress)
    suspend fun downloadModel(context: Context, model: DownloadableModel, onProgress: (Long, Long) -> Unit): Boolean {
        if (model.downloadUrl == null) return false
        return observe(context, startDownload(context, model), onProgress)
    }
    suspend fun importModel(context: Context, model: DownloadableModel, uri: Uri,
        onProgress: (Long, Long) -> Unit = { _, _ -> }): Boolean = observe(context, startImport(context, model, uri), onProgress)
    fun deleteModel(context: Context, model: DownloadableModel): ContentPlanTicket {
        current(context, model)?.cancel()
        return QueuedModels.get(context).submit(request(model, ModelRequest.DELETE, ""))
    }
}
