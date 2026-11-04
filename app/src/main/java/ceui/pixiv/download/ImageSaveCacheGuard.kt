package ceui.pixiv.download

import androidx.annotation.VisibleForTesting
import java.io.Closeable

/** Backend-neutral lifetime of a save source, including the wait for the URL write lock. */
object ImageSaveCacheGuard {
    fun interface Provider {
        fun retain(url: String): Closeable
    }

    private val noOp = Provider { Closeable {} }
    @Volatile private var provider: Provider = noOp

    /** Install before ImagePageSaver.start(); the legacy backend needs no additional lease. */
    @JvmStatic
    fun install(provider: Provider) {
        this.provider = provider
    }

    fun retain(url: String): Closeable = provider.retain(url)

    @VisibleForTesting
    internal fun resetForTests() {
        provider = noOp
    }
}
