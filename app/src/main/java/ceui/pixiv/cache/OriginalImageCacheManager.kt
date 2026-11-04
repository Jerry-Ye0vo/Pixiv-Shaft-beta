package ceui.pixiv.cache

import android.content.Context
import ceui.lisa.activities.Shaft
import ceui.lisa.file.LegacyFile
import ceui.pixiv.download.StageStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import timber.log.Timber
import java.io.File
import java.io.Closeable
import java.io.IOException

/**
 * 原图专用磁盘缓存管理：负责原图落盘提交、LRU 淘汰与孤儿分片清扫。
 */
object OriginalImageCacheManager {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val cacheLock = Any()
    private val retained = HashMap<String, Int>()

    /** Acquire before loading: quota eviction must not remove a source awaiting a save write. */
    @JvmStatic
    fun retain(url: String): Closeable {
        val key = keyForUrl(url)
        synchronized(cacheLock) { retained[key] = (retained[key] ?: 0) + 1 }
        var released = false
        return Closeable {
            synchronized(cacheLock) {
                if (!released) {
                    released = true
                    val remaining = (retained[key] ?: 1) - 1
                    if (remaining == 0) retained.remove(key) else retained[key] = remaining
                }
            }
        }
    }

    private fun isRetained(file: File): Boolean = file.name.substringBefore('.') in retained

    @JvmStatic
    fun getCacheDir(context: Context): File = LegacyFile.originalImageCacheFolder(context)

    @JvmStatic
    fun getStagingDir(context: Context): File = LegacyFile.originalStagingFolder(context)

    @JvmStatic
    fun keyForUrl(url: String): String = StageStore.keyForUrl(url)

    /**
     * 查询指定原图 URL 是否已缓存在本地。若命中则更新访问时间（LRU）并返回文件。
     */
    @JvmStatic
    fun find(context: Context, url: String): File? = findIn(getCacheDir(context), url)

    internal fun findIn(cacheDir: File, url: String): File? = synchronized(cacheLock) {
        val key = keyForUrl(url)
        val ext = fileExtensionForUrl(url)
        val directFile = File(cacheDir, "$key.$ext")
        if (directFile.isFile && directFile.length() > 0) {
            directFile.setLastModified(System.currentTimeMillis())
            return@synchronized directFile
        }

        val datFile = File(cacheDir, "$key.dat")
        if (datFile.isFile && datFile.length() > 0) {
            datFile.setLastModified(System.currentTimeMillis())
            return@synchronized datFile
        }

        val matched = cacheDir.listFiles { _, name -> name.startsWith(key) }
            ?.firstOrNull { it.isFile && it.length() > 0 }
        if (matched != null) {
            matched.setLastModified(System.currentTimeMillis())
            return@synchronized matched
        }

        null
    }

    /**
     * 将临时分片文件提交到原图缓存目录。
     */
    @JvmStatic
    @Throws(IOException::class)
    fun commit(context: Context, url: String, partFile: File): File {
        val file = commitIn(getCacheDir(context), getStagingDir(context), url, partFile)
        trimAsync(context)
        return file
    }

    internal fun commitIn(cacheDir: File, stagingDir: File, url: String, partFile: File): File =
        synchronized(cacheLock) {
            val key = keyForUrl(url)
            if (!cacheDir.isDirectory && !cacheDir.mkdirs()) throw IOException("Cannot create original cache")
            val ext = fileExtensionForUrl(url)
            val targetFile = File(cacheDir, "$key.$ext")

            if (targetFile.exists()) {
                targetFile.delete()
            }

            val renamed = partFile.renameTo(targetFile)
            if (!renamed) {
                partFile.copyTo(targetFile, overwrite = true)
                partFile.delete()
            }

            targetFile.setLastModified(System.currentTimeMillis())

            StageStore.clear(stagingDir, key)
            targetFile
        }

    /**
     * 同步按当前配额执行 LRU 淘汰。
     */
    @JvmStatic
    fun trimToQuota(
        context: Context,
        maxMb: Int = Shaft.sSettings?.originalImageCacheMaxMb ?: OriginalImageCacheQuota.DEFAULT_LIMIT_MB,
    ) = trimDirectory(getCacheDir(context), OriginalImageCacheQuota.maxBytesForLimit(maxMb))

    internal fun trimDirectory(cacheDir: File, maxBytes: Long) = synchronized(cacheLock) {
        val files = cacheDir.listFiles()?.filter { it.isFile } ?: return@synchronized

        var totalSize = files.sumOf { it.length() }
        if (totalSize <= maxBytes) return@synchronized

        val sorted = files.sortedBy { it.lastModified() }
        var evictedCount = 0
        for (file in sorted) {
            if (isRetained(file)) continue
            val length = file.length()
            if (file.delete()) {
                totalSize -= length
                evictedCount++
            }
            if (totalSize <= maxBytes) break
        }

        Timber.d("[OriginalCache] LRU trimmed $evictedCount files, remaining size: $totalSize bytes, quota: $maxBytes bytes")
    }

    /**
     * 异步触发 LRU 淘汰检查。
     */
    @JvmStatic
    fun trimAsync(
        context: Context,
        maxMb: Int = Shaft.sSettings?.originalImageCacheMaxMb ?: OriginalImageCacheQuota.DEFAULT_LIMIT_MB,
    ) {
        val appContext = context.applicationContext
        scope.launch {
            try {
                trimToQuota(appContext, maxMb)
            } catch (e: Throwable) {
                Timber.w(e, "[OriginalCache] async trim failed")
            }
        }
    }

    /**
     * 清理超过指定时间未活动的孤儿分片（.part 与 .part.meta）。
     */
    @JvmStatic
    fun sweepOrphans(context: Context, maxAgeMs: Long = 48 * 3600 * 1000L): Int {
        val stagingDir = getStagingDir(context)
        val swept = sweepDirectory(stagingDir, maxAgeMs, System.currentTimeMillis())
        if (swept > 0) {
            Timber.d("[OriginalCache] swept $swept orphaned staging files")
        }
        return swept
    }

    internal fun sweepDirectory(stagingDir: File, maxAgeMs: Long, now: Long): Int =
        synchronized(cacheLock) {
            StageStore.sweepOrphans(stagingDir, maxAgeMs, now, retained.keys)
        }

    /**
     * 冷启动后台任务：清理孤儿临时分片并按配额清理超出大小的原图缓存。
     */
    @JvmStatic
    fun sweepAndTrim(context: Context) {
        val appContext = context.applicationContext
        scope.launch {
            try {
                sweepOrphans(appContext)
                trimToQuota(appContext)
            } catch (e: Throwable) {
                Timber.w(e, "[OriginalCache] sweepAndTrim failed")
            }
        }
    }

    /**
     * 清空全部原图缓存与临时分片。
     */
    @JvmStatic
    fun clearAll(context: Context) {
        val cacheDir = getCacheDir(context)
        val stagingDir = getStagingDir(context)
        synchronized(cacheLock) {
            cacheDir.listFiles()?.filterNot(::isRetained)?.forEach { runCatching { it.delete() } }
            stagingDir.listFiles()?.filterNot(::isRetained)?.forEach { runCatching { it.delete() } }
        }
        Timber.d("[OriginalCache] clearAll finished")
    }

    /**
     * 统计当前原图缓存占用的总字节数。
     */
    @JvmStatic
    fun totalSizeBytes(context: Context): Long {
        val cacheDir = getCacheDir(context)
        return cacheDir.listFiles()?.filter { it.isFile }?.sumOf { it.length() } ?: 0L
    }

    private fun fileExtensionForUrl(url: String): String {
        val cleaned = url.substringBefore('?').substringBefore('#')
        val ext = cleaned.substringAfterLast('.', "")
        return if (ext.isNotEmpty() && ext.length in 2..5) ext else "dat"
    }
}

