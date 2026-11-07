package ceui.pixiv.ui.novel.reader.export

import ceui.pixiv.ui.novel.reader.model.ContentToken
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test

class NovelExportRequestTest {
    @Test fun everyExportedTokenSurvivesAFrozenSchemaRoundTripWithoutReflectiveSealedClassLoading() {
        val tokens = listOf(ContentToken.Paragraph(1, 5, "翻译正文"), ContentToken.Chapter(6, 9, "Chapter"),
            ContentToken.PixivImage(10, 12, 123, 2), ContentToken.UploadedImage(13, 15, 456),
            ContentToken.PageBreak(16, 17), ContentToken.BlankLine(18, 19), ContentToken.Jump(20, 21, 3))
        val gson = Gson()
        val snapshots = tokens.map(ExportToken::capture)
        val restored = gson.fromJson(gson.toJson(snapshots), Array<ExportToken>::class.java).map { it.token() }
        assertEquals(tokens, restored)
    }
    @Test fun mergedChapterOrderAndTxtSubtitleConventionSurviveRoundTrip() {
        val content = MergedNovelContent("Series", "Author", "https://example.com", "Caption",
            listOf(MergedChapter("Full title", "one", txtHeaderLine = "第1章 short", txtSubtitleLine = "Full title"),
                MergedChapter("Second", "two")), "novel_series_123")
        val gson = Gson(); val captured = ExportMerged.capture(content, gson)
        assertEquals(content, gson.fromJson(gson.toJson(captured), ExportMerged::class.java).content(gson))
    }
    @Test fun unknownFutureTokenFailsInsteadOfSilentlyDroppingAParagraph() {
        try { ExportToken("future", 0, 0).token(); fail("Unknown token accepted") }
        catch (expected: IllegalStateException) { }
    }
}
