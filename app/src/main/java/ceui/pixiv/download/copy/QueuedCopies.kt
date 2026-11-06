package ceui.pixiv.download.copy

import android.content.Context
import ceui.lisa.R
import ceui.lisa.activities.Shaft
import ceui.lisa.utils.Common
import ceui.lisa.utils.PixivOperate
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.Downloads
import ceui.pixiv.download.DownloadsRegistry
import ceui.pixiv.download.config.DownloadItems
import ceui.pixiv.download.model.DownloadRecordSlim
import ceui.pixiv.download.queue.ContentQueueRuntime
import java.io.File
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** The same instance restores at startup and accepts UI submissions; no Activity owns its workers. */
object QueuedCopies {
    @Volatile private var instance: Pair<Context, DurableCopyQueue>? = null
    @JvmStatic fun get(context: Context): DurableCopyQueue {
        val app = context.applicationContext
        return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
            instance?.takeIf { it.first === app }?.second ?: DurableCopyQueue(app, ContentQueueRuntime.get(app),
                AndroidCopyOutput(app) { DownloadsRegistry.downloads }, { Shaft.sSettings.maxConcurrentDownloads },
                succeeded = { request ->
                    if (request.kind == TRANSLATION) {
                        val artwork = runCatching { Shaft.sGson.fromJson(request.contextJson, Illust::class.java) }.getOrNull()
                        withContext(Dispatchers.Main) {
                            Common.showToast(R.string.string_181)
                            if (artwork != null && Shaft.sSettings.isAutoPostLikeWhenDownload && !artwork.isBookmarked) {
                                PixivOperate.postLikeDefaultStarType(artwork)
                            }
                        }
                    }
                }).also { instance = app to it }
        }
    }

    fun family(illustId: Long, page: Int, source: File): String = "$TRANSLATION:$illustId:$page:${source.absolutePath}"

    /** Invoked by the admission actor, never by the UI thread. */
    fun translatedRequest(illust: Illust, page: Int, source: File): CopyRequest {
        val item = DownloadItems.illustTranslated(illust, page)
        val config = DownloadsRegistry.store.loadOrFallback()
        val bucket = config.resolve(item.bucket)
        val frozen = Downloads(config) { error("Destination rendering must not open storage") }
        val path = frozen.resolveDerivedPath(item, "_translated")
        return CopyRequest(TRANSLATION, illust.id, page, source.absolutePath, source.length(), source.lastModified(),
            DownloadRecordSlim.createIllustJson(illust), CopyDestination.capture(bucket.storage, path, item.mime, bucket.overwrite))
    }

    const val TRANSLATION = "translation"
}
