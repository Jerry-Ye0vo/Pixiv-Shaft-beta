package ceui.lisa.update

import ceui.pixiv.download.queue.ContentPhaseRunner
import java.io.File
import okhttp3.OkHttpClient

/** Byte-worker dependencies; the production manager retains the existing MMKV record format. */
internal interface ApkRecordStorage {
    fun read(): String?
    fun write(value: String): Boolean
    fun clear()
}

internal data class ApkDownloadEnvironment(
    val phases: ContentPhaseRunner,
    val records: ApkRecordStorage,
    val directory: File,
    val client: (Long) -> OkHttpClient,
    val proxyPrefix: () -> String = { "" },
    val wrap: (String) -> String = { it },
)
