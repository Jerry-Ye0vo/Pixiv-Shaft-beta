package ceui.pixiv.download.model

import android.content.Context
import android.net.Uri
import android.provider.OpenableColumns
import ceui.pixiv.download.network.ResumableFileTransfer
import ceui.pixiv.download.queue.*
import ceui.pixiv.download.queue.ContentQueueStore.Pool
import ceui.pixiv.download.queue.ContentQueueStore.State
import com.google.gson.Gson
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

/** One durable handler for all five model families; never retains a Fragment or UI callback. */
internal class ModelPlanHandler(
    private val app: Context,
    private val phases: ContentPhaseRunner,
    private val allowed: () -> Boolean,
    private val client: (Long) -> OkHttpClient = { timeout -> OkHttpClient.Builder().followRedirects(true)
        .connectTimeout(30, TimeUnit.SECONDS).readTimeout(timeout, TimeUnit.SECONDS).build() },
) : ContentPlanHandler {
    override val kind = KIND
    private val gson = Gson()
    private val manual = ConcurrentHashMap.newKeySet<String>()
    fun allow(family: String) { manual.add(family) }
    override fun onUserResume(plan: ContentPlanStore.Plan) { allow(plan.family) }
    fun revokeManual() { manual.clear() }
    private fun request(plan: ContentPlanStore.Plan) = gson.fromJson(plan.request, ModelRequest::class.java).also { it.validate() }
    private fun directory(plan: ContentPlanStore.Plan) = File(app.filesDir, "content_model_inputs/${ModelRequest.hash(plan.id)}")
    override fun canRun(plan: ContentPlanStore.Plan): Boolean {
        // Malformed records must fail visibly inside execute rather than remain silently ineligible.
        val request = runCatching { request(plan) }.getOrNull() ?: return true
        return request.operation != ModelRequest.DOWNLOAD || manual.contains(plan.family) || allowed() ||
            (File(directory(plan), "source.zip").isFile && runCatching {
                gson.fromJson(plan.checkpoint, ModelCheckpoint::class.java)?.invalid != true
            }.getOrDefault(false))
    }

    override suspend fun reconcile(plan: ContentPlanStore.Plan): Boolean {
        val request = request(plan)
        if (request.operation == ModelRequest.DELETE) return false
        return phases.run(plan.id, "model-reconcile", "model-install:${request.directory}/${request.asset}", Pool.COPY) {
            ModelInstaller(app.filesDir, request, plan.id, plan.request).installed()
        }
    }
    override suspend fun execute(context: ContentPlanContext) {
        val plan = context.plan
        val request = request(plan)
        check(request.family == plan.family) { "Model parent identity mismatch" }
        val installer = ModelInstaller(app.filesDir, request, plan.id, plan.request)
        suspend fun <T> copy(phase: String, body: suspend () -> T): T = phases.run(plan.id, phase,
            "model-install:${request.directory}/${request.asset}", Pool.COPY, plan.request, body)
        val installed = copy("model-recover") { installer.recover(); request.operation != ModelRequest.DELETE && installer.installed() }
        if (installed) { context.commit {}; return }
        if (request.operation == ModelRequest.DELETE) {
            context.progress("model-delete")
            copy("model-delete") { context.commit { installer.delete() } }
            return
        }
        val dir = directory(plan)
        if (!dir.isDirectory && !dir.mkdirs()) throw IOException("Cannot preserve model input")
        val source = File(dir, "source.zip")
        var checkpoint = runCatching { gson.fromJson(context.checkpoint, ModelCheckpoint::class.java) }.getOrNull()
        val reuse = copy("model-input-check") {
            if (checkpoint?.invalid == true) { ModelInstaller.remove(source); false }
            else {
                val valid = source.isFile && source.length() > 0 && (checkpoint == null || checkpoint!!.length == 0L ||
                    (checkpoint!!.version == 1 && checkpoint!!.length == source.length() && ModelInstaller.digest(source) == checkpoint!!.sha256))
                if (!valid) ModelInstaller.remove(source)
                valid
            }
        }
        if (!reuse) {
            if (request.operation == ModelRequest.DOWNLOAD) {
                if (!manual.contains(plan.family) && !allowed()) context.yieldForPolicy()
                phases.run(plan.id, "model-download", "model-input:${plan.id}", Pool.NETWORK, plan.request) {
                    context.checkStop(); context.progress("model-download")
                    ResumableFileTransfer.transfer(request.source, source, client(request.timeoutSeconds), maximumBytes = MAX_INPUT) { bytes, total ->
                        context.progress("model-download", bytes, total)
                    }
                }
            } else copy("model-import") {
                context.progress("model-import")
                val uri = Uri.parse(request.source)
                val total = runCatching { app.contentResolver.query(uri, arrayOf(OpenableColumns.SIZE), null, null, null)?.use {
                    if (it.moveToFirst()) { val index = it.getColumnIndex(OpenableColumns.SIZE)
                        if (index >= 0 && !it.isNull(index)) it.getLong(index) else -1L } else -1L
                } ?: -1L }.getOrDefault(-1L)
                if (total > MAX_INPUT) throw IOException("Model import exceeds size limit")
                val temporary = File(dir, "source.importing")
                app.contentResolver.openInputStream(uri)?.use { input ->
                    FileOutputStream(temporary).use { output ->
                        val bytes = ByteArray(32 * 1024); var read = 0L
                        while (true) {
                            currentCoroutineContext().ensureActive(); context.checkStop()
                            val count = input.read(bytes); if (count < 0) break
                            read += count; if (read > MAX_INPUT) throw IOException("Model import exceeds size limit")
                            output.write(bytes, 0, count); context.progress("model-import", read, total)
                        }
                        output.fd.sync(); if (read == 0L) throw IOException("Empty model import")
                    }
                } ?: throw IOException("Cannot open model import; select the document again")
                if (!temporary.renameTo(source)) throw IOException("Cannot preserve model import")
            }
        }
        copy("model-validate") {
            context.progress("model-validate")
            checkpoint = ModelCheckpoint(length = source.length(), sha256 = ModelInstaller.digest(source))
            context.checkpoint(gson.toJson(checkpoint))
            try { installer.prepare(source, checkpoint!!.sha256) }
            catch (error: Exception) {
                currentCoroutineContext().ensureActive()
                context.checkpoint(gson.toJson(checkpoint!!.copy(invalid = true)))
                throw error
            }
        }
        context.checkStop()
        copy("model-install") { context.progress("model-install"); context.commit { installer.install() } }
    }

    override suspend fun cleanup(plan: ContentPlanStore.Plan, state: State) {
        if (state != State.SUCCEEDED && state != State.CANCELLED) return
        val request = request(plan)
        phases.run(plan.id, "model-cleanup", "model-install:${request.directory}/${request.asset}", Pool.COPY) {
            ModelInstaller(app.filesDir, request, plan.id, plan.request).cleanup(state == State.SUCCEEDED)
            ModelInstaller.remove(directory(plan))
        }
    }
    companion object { const val KIND = "model"; const val MAX_INPUT = 512L * 1024 * 1024 }
}
