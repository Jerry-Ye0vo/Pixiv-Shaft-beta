package ceui.pixiv.ui.novel.reader.export

import androidx.annotation.Keep
import ceui.pixiv.download.copy.CopyDestination
import ceui.pixiv.ui.novel.reader.model.ContentToken
import ceui.pixiv.api.model.WebNovel
import com.google.gson.Gson

@Keep
data class ExportToken(val type: String, val start: Int, val end: Int, val text: String = "", val id: Long = 0, val page: Int = 0) {
    fun token(): ContentToken = when (type) {
        "paragraph" -> ContentToken.Paragraph(start, end, text)
        "chapter" -> ContentToken.Chapter(start, end, text)
        "pixiv" -> ContentToken.PixivImage(start, end, id, page)
        "upload" -> ContentToken.UploadedImage(start, end, id)
        "page" -> ContentToken.PageBreak(start, end)
        "blank" -> ContentToken.BlankLine(start, end)
        "jump" -> ContentToken.Jump(start, end, page)
        else -> error("Unknown persisted novel token: $type")
    }
    companion object {
        fun capture(token: ContentToken): ExportToken = when (token) {
            is ContentToken.Paragraph -> ExportToken("paragraph", token.sourceStart, token.sourceEnd, token.text)
            is ContentToken.Chapter -> ExportToken("chapter", token.sourceStart, token.sourceEnd, token.title)
            is ContentToken.PixivImage -> ExportToken("pixiv", token.sourceStart, token.sourceEnd, id = token.illustId, page = token.pageIndex)
            is ContentToken.UploadedImage -> ExportToken("upload", token.sourceStart, token.sourceEnd, id = token.imageId)
            is ContentToken.PageBreak -> ExportToken("page", token.sourceStart, token.sourceEnd)
            is ContentToken.BlankLine -> ExportToken("blank", token.sourceStart, token.sourceEnd)
            is ContentToken.Jump -> ExportToken("jump", token.sourceStart, token.sourceEnd, page = token.target)
        }
    }
}

@Keep
data class ExportChapter(val title: String, val text: String, val webJson: String?, val txtHeader: String, val txtSubtitle: String?) {
    fun chapter(gson: Gson) = MergedChapter(title, text, webJson?.let { gson.fromJson(it, WebNovel::class.java) }, txtHeader, txtSubtitle)
    companion object {
        fun capture(chapter: MergedChapter, gson: Gson) = ExportChapter(chapter.title, chapter.text,
            chapter.webNovel?.let(gson::toJson), chapter.txtHeaderLine, chapter.txtSubtitleLine)
    }
}

@Keep
data class ExportMerged(val title: String, val author: String?, val url: String?, val caption: String?,
    val documentId: String, val chapters: List<ExportChapter>) {
    fun content(gson: Gson) = MergedNovelContent(title, author, url, caption, chapters.map { it.chapter(gson) }, documentId)
    companion object {
        fun capture(content: MergedNovelContent, gson: Gson) = ExportMerged(content.displayTitle, content.author,
            content.sourceUrl, content.caption, content.documentId, content.chapters.map { ExportChapter.capture(it, gson) })
    }
}

@Keep
data class NovelExportRequest(val version: Int = 1, val format: String, val novelJson: String?, val novelId: Long,
    val webJson: String?, val tokens: List<ExportToken>?, val merged: ExportMerged?, val destination: CopyDestination,
    val header: String?, val legacyTxt: Boolean = false, val retryMode: String = "auto") {
    fun validate() {
        require(version == 1 && retryMode in setOf("auto", "foreground", "batch"))
        ExportFormat.valueOf(format); destination.storageChoice(); destination.relativePath(); destination.overwrite()
        require((novelId > 0 || webJson != null || merged != null) && !(merged != null && legacyTxt))
        require(legacyTxt || merged != null || webJson != null)
    }
}
