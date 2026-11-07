package ceui.pixiv.ui.profile

import android.content.Context
import ceui.pixiv.download.model.ModelRequest
import ceui.pixiv.download.model.QueuedModels
import java.io.File

/** Standard ONNX input; the upstream "-mobile" graph needs extra custom operators. */
internal object LocalTaggerModel {
    const val VERSION = "wd-convnext-v3-quint8-f574b1b7-v1"
    const val DIRECTORY = "image-tagger-models"
    const val ASSET = "wd-convnext-v3-int8"
    const val FILE = "model.onnx"
    const val FAMILY = "model:$DIRECTORY/$ASSET"
    const val BYTES = 111103266L
    const val SHA256 = "1de64bf089c186a342a27c7a48c17390e5bed0bc4aed8ae0450f356c6af05bca"
    const val URL = "https://huggingface.co/Smashinfries/wd-convnext-tagger-v3-mobile/resolve/f574b1b7c24a41b275fa0169f5a6905402f58870/wd-convnext-tagger-v3-quint8.onnx"
    fun file(context: Context) = File(context.filesDir, "$DIRECTORY/$ASSET/$FILE")
    fun ready(context: Context) = file(context).let { it.isFile && it.length() == BYTES }
    fun request() = QueuedModels.frozen(DIRECTORY, ASSET, listOf(FILE), ModelRequest.DOWNLOAD, URL, 180)
        .copy(rawFile = FILE, expectedSha256 = SHA256)
    fun download(context: Context) = QueuedModels.get(context).submit(request())
    fun current(context: Context) = QueuedModels.get(context).latest(FAMILY)
}
