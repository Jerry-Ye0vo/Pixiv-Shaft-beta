package ceui.pixiv.download.network

import ceui.pixiv.download.StageStore
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import kotlin.coroutines.coroutineContext

/** Raw byte worker. Its caller must own the shared NETWORK claim; it never dispatches itself. */
internal object ResumableFileTransfer {
    suspend fun transfer(
        url: String,
        target: File,
        client: OkHttpClient,
        headers: Map<String, String> = emptyMap(),
        maximumBytes: Long = Long.MAX_VALUE,
        onProgress: (Long, Long) -> Unit = { _, _ -> },
    ) {

    val realUrl = url
    target.parentFile?.mkdirs()
    val temp = File(target.parentFile, target.name + ".part")
    val meta = File(target.parentFile, target.name + ".part.meta")
    val manifest = StageStore.readManifest(meta)?.takeIf {
        // 切图片代理、旧版无元数据的 partial、弱校验器均不能证明字节属于同一版资源。
        it.url == realUrl && !it.validator.isNullOrBlank() &&
            (it.validatorType == StageStore.VALIDATOR_LASTMOD ||
                (it.validatorType == StageStore.VALIDATOR_ETAG && it.validator.startsWith('"'))) &&
            (it.total < 0 || temp.length() <= it.total)
    }
    // 异常 Range 响应最多回退一次全量请求，避免用户手动重试才能恢复，也避免无限循环。
    repeat(2) { attempt ->
        coroutineContext.ensureActive()
        val existing = if (attempt == 0 && manifest != null) temp.length() else 0L
        val req = Request.Builder()
            .url(realUrl)
            .apply { headers.forEach { (name, value) -> header(name, value) } }
            // offset 必须是实际保存的字节数，不能混用透明 gzip 解码前后的长度。
            .header("Accept-Encoding", "identity")
            .apply {
                if (existing > 0) {
                    header("Range", "bytes=$existing-")
                    header("If-Range", manifest!!.validator!!)
                }
            }
            .build()
        val call = client.newCall(req)
        val stopping = CoroutineScope(coroutineContext).launch(Dispatchers.IO, start = CoroutineStart.UNDISPATCHED) {
            try { awaitCancellation() } finally { call.cancel() }
        }
        val complete = try { call.execute().use { r ->
            coroutineContext.ensureActive()
            if (!r.isSuccessful && r.code != 416) {
                throw IOException("zip download HTTP ${r.code} url=$url")
            }
            val body = r.body ?: throw IOException("zip body null url=$url")
            val encoding = r.header("Content-Encoding")
            if (encoding != null && !encoding.equals("identity", ignoreCase = true)) {
                throw IOException("unexpected zip Content-Encoding: $encoding")
            }
            val range = StageStore.parseContentRange(r.header("Content-Range"))
            val responseValidator = when (manifest?.validatorType) {
                StageStore.VALIDATOR_ETAG -> r.header("ETag")
                StageStore.VALIDATOR_LASTMOD -> r.header("Last-Modified")
                else -> null
            }
            if (r.code == 416) {
                // 仅凭大小相同不能证明资源没变；还要响应校验器与原记录相同。
                // 首次 chunked 响应可能没给总长，此时以 416 的总长与已存字节数确认完整。
                return@use existing > 0 && range?.start == -1L && range.total == existing &&
                    manifest != null && (manifest.total < 0 || manifest.total == existing) &&
                    responseValidator == manifest.validator &&
                    temp.length() == existing
            }
            val append = r.code == 206
            if (append) {
                if (existing == 0L || range == null || range.start != existing ||
                    range.total <= existing || range.end != range.total - 1 ||
                    (body.contentLength() >= 0 && body.contentLength() != range.total - existing) ||
                    (manifest!!.total >= 0 && manifest.total != range.total) ||
                    (responseValidator != null && responseValidator != manifest.validator)
                ) return@use false
            } else if (r.code != 200) {
                throw IOException("unexpected zip download HTTP ${r.code}")
            }
            val total = if (append) range!!.total else body.contentLength()
            if (total > maximumBytes) throw IOException("File exceeds the accepted size limit")
            val offset = if (append) existing else 0L
            // append 前再确认文件未被清缓存删掉，避免 FileOutputStream 静默重建只含尾部的文件。
            if (append && temp.length() != offset) return@use false
            FileOutputStream(temp, append).use { out ->
                if (!append) {
                    // 先截断旧字节再替换元数据，进程被杀也不能用新 validator 续接旧字节。
                    // 元数据无法写入时，下次安全退化成全量下载。
                    if (meta.exists() && !meta.delete()) throw IOException("cannot reset zip metadata")
                    val etag = r.header("ETag")?.trim()
                    val strongEtag = etag?.takeIf { it.startsWith('"') && it.endsWith('"') }
                    val modified = r.headers.getDate("Last-Modified")?.time
                    val date = r.headers.getDate("Date")?.time
                    // RFC 9110 13.1.5：弱 ETag 不可用于 If-Range；只有没有 ETag 时才用日期。
                    // 给 Date / Last-Modified 留 60s 间隔，避免秒级精度与时钟差异。
                    val lastModified = r.header("Last-Modified")?.takeIf {
                        etag == null && modified != null && date != null && date - modified >= 60_000L
                    }
                    StageStore.writeManifest(meta, StageStore.buildManifest(realUrl, strongEtag, lastModified, total))
                }
                val input = body.byteStream()
                val buf = ByteArray(16 * 1024)
                var readTotal = offset
                fun reportProgress() { onProgress(readTotal, total) }
                reportProgress()
                while (true) {
                    coroutineContext.ensureActive()
                    val n = input.read(buf)
                    coroutineContext.ensureActive()
                    if (n < 0) break
                    if (readTotal > maximumBytes - n) throw IOException("File exceeds the accepted size limit")
                    out.write(buf, 0, n)
                    readTotal += n
                    reportProgress()
                }
                out.fd.sync()
                val storedLength = temp.length()
                if (readTotal == 0L || (total >= 0 && readTotal != total) || storedLength != readTotal) {
                    // 顺序写入的短响应仍是有效前缀，保留 validator 给下一次续传。
                    // 文件被外部清理或字节超过声明总长时，才放弃这个断点。
                    if (storedLength != readTotal || (total >= 0 && readTotal > total)) meta.delete()
                    throw IOException("zip length mismatch: $storedLength / $total")
                }
            }
            true
        }
        } catch (error: Throwable) {
            coroutineContext.ensureActive()
            throw error
        } finally { stopping.cancel() }
        if (complete) {
            coroutineContext.ensureActive()
            if (temp.length() > maximumBytes) throw IOException("File exceeds the accepted size limit")
            if (!temp.renameTo(target)) throw IOException("rename .part → ${target.name} failed")
            meta.delete()
            onProgress(target.length(), target.length())
            return
        }
    }
    throw IOException("invalid zip range response url=$url")
    }
}
