package ceui.pixiv.ui.profile

import ai.onnxruntime.OnnxJavaType
import ai.onnxruntime.OnnxTensor
import ai.onnxruntime.OrtEnvironment
import ai.onnxruntime.OrtSession
import ai.onnxruntime.TensorInfo
import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Matrix
import android.graphics.Paint
import android.graphics.RectF
import android.net.Uri
import androidx.annotation.Keep
import androidx.exifinterface.media.ExifInterface
import org.json.JSONArray
import java.io.Closeable
import java.io.IOException
import java.nio.FloatBuffer

@Keep internal data class PredictedImageTag(val name: String, val confidence: Float, val category: Int)
internal data class TaggerLabel(val name: String, val category: Int)
internal interface ProfileTagger : Closeable {
    suspend fun predict(uri: Uri): List<PredictedImageTag>
}

internal object TaggerLabels {
    fun read(json: String): List<TaggerLabel> {
        val root = JSONArray(json)
        return (0 until root.length()).map { n -> root.getJSONArray(n).let { TaggerLabel(it.getString(0), it.getInt(1)) } }
    }
    fun select(labels: List<TaggerLabel>, scores: FloatArray): List<PredictedImageTag> {
        require(labels.size == scores.size) { "Tag vocabulary does not match the model output" }
        return labels.indices.mapNotNull { n ->
            val label = labels[n]; val confidence = scores[n]
            val threshold = when (label.category) { 0 -> .35f; 4 -> .85f; else -> return@mapNotNull null }
            if (confidence.isFinite() && confidence in threshold..1f)
                PredictedImageTag(label.name, confidence, label.category) else null
        }.sortedByDescending { it.confidence }.let { selected ->
            selected.filter { it.category == 0 }.take(24) + selected.filter { it.category == 4 }.take(8)
        }
    }
}

/** One batch-owned CPU session. Never retain an Activity, full-size bitmap or global model session. */
internal class OfflineImageTagger private constructor(
    private val app: Context, private val environment: OrtEnvironment, private val session: OrtSession,
    private val inputName: String, private val labels: List<TaggerLabel>,
) : ProfileTagger {
    override suspend fun predict(uri: Uri): List<PredictedImageTag> {
        val bitmap = decode(app, uri)
        val pixels = try { preprocess(bitmap) } finally { bitmap.recycle() }
        OnnxTensor.createTensor(environment, FloatBuffer.wrap(pixels), longArrayOf(1, SIZE.toLong(), SIZE.toLong(), 3)).use { input ->
            session.run(mapOf(inputName to input)).use { output ->
                val values = (output[0].value as? Array<*>)?.firstOrNull() as? FloatArray
                    ?: throw IOException("Unsupported tagger output")
                return TaggerLabels.select(labels, values)
            }
        }
    }
    override fun close() = session.close()

    companion object {
        const val SIZE = 448
        fun open(context: Context): OfflineImageTagger {
            check(LocalTaggerModel.ready(context)) { "Download the image tagger model first" }
            val labels = context.assets.open("profile_wd_tags.json").bufferedReader().use { TaggerLabels.read(it.readText()) }
            return fromFile(context, LocalTaggerModel.file(context), labels)
        }
        internal fun fromFile(context: Context, file: java.io.File, labels: List<TaggerLabel>): OfflineImageTagger {
            val environment = OrtEnvironment.getEnvironment()
            val session = OrtSession.SessionOptions().use { options ->
                options.setIntraOpNumThreads(2); options.setInterOpNumThreads(1)
                environment.createSession(file.absolutePath, options)
            }
            try {
                val input = session.inputInfo.entries.single()
                val info = input.value.info as? TensorInfo ?: error("Unsupported tagger tensor")
                require(info.type == OnnxJavaType.FLOAT && info.shape.contentEquals(longArrayOf(-1, 448, 448, 3)) ||
                    info.type == OnnxJavaType.FLOAT && info.shape.contentEquals(longArrayOf(1, 448, 448, 3))) {
                    "Expected the standard WD NHWC float32 model"
                }
                val output = session.outputInfo.values.first().info as TensorInfo
                require(output.shape.last() == labels.size.toLong()) { "Wrong tagger vocabulary" }
                return OfflineImageTagger(context.applicationContext, environment, session, input.key, labels)
            } catch (error: Throwable) { session.close(); throw error }
        }
        private fun decode(context: Context, uri: Uri): Bitmap {
            val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            context.contentResolver.openInputStream(uri)?.use { BitmapFactory.decodeStream(it, null, options) }
            if (options.outWidth <= 0 || options.outHeight <= 0 || options.outMimeType == "image/gif")
                throw IOException("Not a supported static image")
            if (options.outMimeType == "image/webp") {
                val header = ByteArray(21)
                context.contentResolver.openInputStream(uri)?.use { stream ->
                    var count = 0
                    while (count < header.size) {
                        val read = stream.read(header, count, header.size - count)
                        if (read < 0) break
                        count += read
                    }
                    if (count == header.size && header.copyOfRange(12, 16).contentEquals("VP8X".toByteArray()) &&
                        header[20].toInt() and 2 != 0) throw IOException("Animated WebP is not supported")
                }
            }
            options.inSampleSize = 1
            while (maxOf(options.outWidth, options.outHeight) / options.inSampleSize > SIZE * 2) options.inSampleSize *= 2
            options.inJustDecodeBounds = false; options.inPreferredConfig = Bitmap.Config.ARGB_8888
            if (android.os.Build.VERSION.SDK_INT >= 26)
                options.inPreferredColorSpace = android.graphics.ColorSpace.get(android.graphics.ColorSpace.Named.SRGB)
            val bitmap = context.contentResolver.openInputStream(uri)?.use { BitmapFactory.decodeStream(it, null, options) }
                ?: throw IOException("Cannot decode local image")
            val orientation = try {
                context.contentResolver.openInputStream(uri)?.use {
                    ExifInterface(it).getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_NORMAL)
                } ?: ExifInterface.ORIENTATION_NORMAL
            } catch (_: IOException) { ExifInterface.ORIENTATION_NORMAL }
            val matrix = Matrix().apply {
                when (orientation) {
                    2 -> setScale(-1f, 1f)
                    3 -> setRotate(180f)
                    4 -> setScale(1f, -1f)
                    5 -> { setRotate(90f); postScale(-1f, 1f) }
                    6 -> setRotate(90f)
                    7 -> { setRotate(90f); postScale(1f, -1f) }
                    8 -> setRotate(270f)
                }
            }
            if (matrix.isIdentity) return bitmap
            return try { Bitmap.createBitmap(bitmap, 0, 0, bitmap.width, bitmap.height, matrix, true) }
                finally { bitmap.recycle() }
        }
        /** White square padding, BGR NHWC, raw 0..255; model already includes its normalization. */
        internal fun preprocess(bitmap: Bitmap): FloatArray {
            val square = Bitmap.createBitmap(SIZE, SIZE, Bitmap.Config.ARGB_8888)
            try {
                val scale = SIZE.toFloat() / maxOf(bitmap.width, bitmap.height)
                val width = bitmap.width * scale; val height = bitmap.height * scale
                Canvas(square).apply {
                    drawColor(Color.WHITE)
                    drawBitmap(bitmap, null, RectF((SIZE - width) / 2, (SIZE - height) / 2,
                        (SIZE + width) / 2, (SIZE + height) / 2), Paint(Paint.FILTER_BITMAP_FLAG))
                }
                val colors = IntArray(SIZE * SIZE)
                square.getPixels(colors, 0, SIZE, 0, 0, SIZE, SIZE)
                return FloatArray(colors.size * 3).also { buffer ->
                    colors.forEachIndexed { n, color ->
                        buffer[n * 3] = Color.blue(color).toFloat()
                        buffer[n * 3 + 1] = Color.green(color).toFloat()
                        buffer[n * 3 + 2] = Color.red(color).toFloat()
                    }
                }
            } finally { square.recycle() }
        }
    }
}
