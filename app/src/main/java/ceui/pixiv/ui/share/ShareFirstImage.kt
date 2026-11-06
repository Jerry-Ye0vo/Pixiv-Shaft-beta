package ceui.pixiv.ui.share

import android.content.Context
import android.content.Intent
import android.net.Uri
import androidx.core.content.FileProvider
import androidx.fragment.app.Fragment
import androidx.lifecycle.lifecycleScope
import ceui.lisa.R
import ceui.lisa.download.IllustDownload
import ceui.pixiv.api.model.Illust
import ceui.lisa.utils.Params
import ceui.pixiv.imageloader.PageImageSourceResolver
import ceui.pixiv.imageloader.awaitFile
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import timber.log.Timber
import java.io.File

// 分享首图（多 P 也只发 p0，与菜单文案 R.string.string_454 一致）。
// 来源走统一的 PageImageSourceResolver：本地已下载 / 看图页已加载好的原图直接用，不再单独走一遍
// 网络；直接拿原始字节，不经过 Bitmap 解码 / PNG 再压缩 —— 既避开原图尺寸下 .asBitmap() 的 OOM
// 风险，也保留原始格式与画质。
fun Fragment.shareFirstImage(illust: Illust) {
    val ctx = context ?: return
    val url = IllustDownload.getUrl(illust, 0, Params.IMAGE_RESOLUTION_ORIGINAL) ?: return

    viewLifecycleOwner.lifecycleScope.launch {
        val uri = runCatching {
            val prepared = ceui.pixiv.download.copy.QueuedImageDerivatives.get(ctx).shareOriginal(illust, 0)
            FileProvider.getUriForFile(ctx, "${ctx.packageName}.provider", File(requireNotNull(prepared.uri.path)))
        }.onFailure {
            // 页面销毁导致的取消照样往上抛，别当成失败打 error 日志。
            if (it is CancellationException) throw it
            Timber.e(it, "[shareFirstImage] failed illustId=${illust.id}")
        }
            .getOrNull() ?: return@launch

        val mime = ctx.contentResolver.getType(uri) ?: "image/*"
        val shareIntent = Intent(Intent.ACTION_SEND).apply {
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            setDataAndType(uri, mime)
            putExtra(Intent.EXTRA_STREAM, uri)
        }
        startActivity(Intent.createChooser(shareIntent, getString(R.string.share)))
    }
}

