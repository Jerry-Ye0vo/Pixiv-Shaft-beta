package ceui.pixiv.download.model

import android.content.Context
import android.net.Uri
import android.content.Intent
import ceui.lisa.http.GithubProxy
import ceui.lisa.utils.DownloadLimitTypeUtil
import ceui.pixiv.download.queue.*
import com.google.gson.Gson
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first

/** Explicit user permission is restricted to this model family; it never releases another queue. */
class QueuedModels private constructor(private val app: Context) {
    val queue = ContentPlanQueue.get(app)
    private val handler = ModelPlanHandler(app, ContentPhases.runner(app), allowed = { DownloadLimitTypeUtil.autoStartAllowed() })
    init { queue.register(handler) }
    fun submit(request: ModelRequest): ContentPlanTicket {
        request.validate(); handler.allow(request.family)
        if (request.operation == ModelRequest.IMPORT) runCatching {
            app.contentResolver.takePersistableUriPermission(Uri.parse(request.source), Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
        return queue.submit(request.family, ModelPlanHandler.KIND, replaceActive = request.operation == ModelRequest.DELETE) { Gson().toJson(request) }
    }
    fun latest(family: String) = queue.latest(family)
    suspend fun await(ticket: ContentPlanTicket, progress: (Long, Long) -> Unit): Boolean {
        val result = ticket.status.first {
            progress(it.bytes, it.total)
            it.state in ContentPlanTicket.OBSERVABLE_END
        }
        if (result.state == ContentQueueStore.State.CANCELLED || result.state == ContentQueueStore.State.PAUSED)
            throw CancellationException("Model task stopped")
        return result.state == ContentQueueStore.State.SUCCEEDED
    }
    companion object {
        @Volatile private var instance: Pair<Context, QueuedModels>? = null
        @JvmStatic fun get(context: Context): QueuedModels {
            val app = context.applicationContext
            return instance?.takeIf { it.first === app }?.second ?: synchronized(this) {
                instance?.takeIf { it.first === app }?.second ?: QueuedModels(app).also { instance = app to it }
            }
        }
        @JvmStatic fun refreshEligibility() { instance?.second?.queue?.refreshEligibility() }
        @JvmStatic fun onNetworkGateClosed() {
            instance?.second?.let { it.handler.revokeManual(); it.queue.refreshEligibility() }
        }
        fun frozen(directory: String, asset: String, required: List<String>, operation: String, source: String, timeout: Long) =
            ModelRequest(directory = directory, asset = asset, required = required.sorted(), operation = operation,
                source = if (operation == ModelRequest.DOWNLOAD) GithubProxy.wrap(source) else source, timeoutSeconds = timeout)
    }
}
