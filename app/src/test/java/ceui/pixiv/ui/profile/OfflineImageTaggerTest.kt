package ceui.pixiv.ui.profile

import android.app.Application
import android.graphics.Bitmap
import android.graphics.Color
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
@org.robolectric.annotation.GraphicsMode(org.robolectric.annotation.GraphicsMode.Mode.NATIVE)
class OfflineImageTaggerTest {
    @Test fun inputUsesWhitePaddingAndRawBgrWithoutDoubleNormalization() {
        val bitmap = Bitmap.createBitmap(448, 224, Bitmap.Config.ARGB_8888)
        bitmap.eraseColor(Color.RED)
        val input = try { OfflineImageTagger.preprocess(bitmap) } finally { bitmap.recycle() }
        assertEquals(448 * 448 * 3, input.size)
        assertArrayEquals(floatArrayOf(255f, 255f, 255f), input.copyOfRange(0, 3), 0f)
        val middle = (224 * 448 + 224) * 3
        assertArrayEquals(floatArrayOf(0f, 0f, 255f), input.copyOfRange(middle, middle + 3), 0f)
    }

    @Test fun transparentPixelsAreCompositedOntoWhite() {
        val bitmap = Bitmap.createBitmap(1, 1, Bitmap.Config.ARGB_8888)
        bitmap.eraseColor(Color.TRANSPARENT)
        val input = try { OfflineImageTagger.preprocess(bitmap) } finally { bitmap.recycle() }
        assertEquals(255f, input[224 * 448 * 3], 0f)
    }

    @Test fun thresholdsPreserveGeneralAndHighConfidenceCharacterTagsAndExcludeRatingOrArtist() {
        val labels = TaggerLabels.read("""[["general",9],["dress",0],["character",4],["artist",1],["invalid",0]]""")
        val selected = TaggerLabels.select(labels, floatArrayOf(.99f, .6f, .9f, .99f, Float.NaN))
        assertEquals(listOf("dress", "character"), selected.map { it.name })
        assertTrue(TaggerLabels.select(listOf(TaggerLabel("character",4)), floatArrayOf(.8f)).isEmpty())
        assertThrows(IllegalArgumentException::class.java) { TaggerLabels.select(labels, floatArrayOf(.3f)) }
    }

    @Test fun initialEmptyQueueCheckpointIsDistinguishableFromAnInitializedEmptyLibrary() {
        val gson = com.google.gson.Gson()
        assertFalse(gson.fromJson("{}", ProfileTagProgress::class.java).initialized)
        val initialized = ProfileTagProgress(ceiling = 0, initialized = true, inferred = 2, elapsedMs = 6000)
        assertTrue(gson.fromJson(gson.toJson(initialized), ProfileTagProgress::class.java).initialized)
        assertEquals(20.0, initialized.perMinute, .001)
        assertFalse(QueuedProfileTagging.staticImage("animation.gif"))
        assertFalse(QueuedProfileTagging.staticImage("novel.epub"))
        assertTrue(QueuedProfileTagging.staticImage("image.PNG"))
    }
}
