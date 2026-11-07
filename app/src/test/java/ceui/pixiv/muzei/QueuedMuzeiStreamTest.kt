package ceui.pixiv.muzei

import ceui.pixiv.imageloader.OriginalFileLease
import java.io.Closeable
import java.io.File
import java.io.FileNotFoundException
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class QueuedMuzeiStreamTest {
    @get:Rule val folder = TemporaryFolder()
    @Test fun cacheLeaseSurvivesReadingAndIsReleasedExactlyOnceWhenMuzeiClosesItsStream() {
        val file = folder.newFile().apply { writeText("original bytes") }
        var closes = 0
        val stream = queuedMuzeiStream(OriginalFileLease(file, Closeable { closes++ }))
        assertEquals("original bytes", stream.readBytes().toString(Charsets.UTF_8)); assertEquals(0, closes)
        stream.close(); stream.close(); assertEquals(1, closes); assertTrue(file.exists())
    }
    @Test fun aMissingFileCannotLeakTheSharedSourceReference() {
        var closes = 0
        try { queuedMuzeiStream(OriginalFileLease(File(folder.root, "missing"), Closeable { closes++ })); fail("Missing source") }
        catch (_: FileNotFoundException) { assertEquals(1, closes) }
    }
}
