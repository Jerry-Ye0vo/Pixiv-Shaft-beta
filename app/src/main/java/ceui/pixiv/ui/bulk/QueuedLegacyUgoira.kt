package ceui.pixiv.ui.bulk

import android.content.Context
import ceui.lisa.cache.UgoiraMetadataCache
import ceui.lisa.file.LegacyFile
import ceui.pixiv.api.model.Illust
import ceui.pixiv.download.queue.ContentPhases
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.util.function.IntConsumer
import kotlinx.coroutines.runBlocking

/** Blocking Java bridge; called only by the existing background legacy encoder executor. */
object QueuedLegacyUgoira {
    @JvmStatic fun encode(context: Context, parent: File, illust: Illust, progress: IntConsumer): File = runBlocking {
        val target = LegacyFile.gifResultFile(context, illust)
        ContentPhases.run("ugoira", "legacy-gif", "ugoira-gif:" + target.absolutePath, Pool.ENCODE) {
            val files = sortedUgoiraFrames(parent)
            check(files.isNotEmpty()) { "No ugoira frames to encode" }
            val response = UgoiraMetadataCache.get(illust.id)
            val delays = if (response != null) ugoiraDelays(files.size, response) else List(files.size) { 60 }
            BufferedOutputStream(FileOutputStream(target)).use {
                encodeFramesToGif(files, delays, it) { percent -> progress.accept(percent) }
            }
            target
        }
    }
}
