package ceui.pixiv.ui.profile

import android.graphics.Bitmap
import android.graphics.Color
import android.net.Uri
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/** Executes a real uint8 ONNX convolution through the app's Android inference/preprocess path. */
@RunWith(AndroidJUnit4::class)
class ProfileTaggerNativeTest {
    @Test fun quantizedCpuInferenceKeepsBgrChannelsAndImageLabels() = runBlocking {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val app = instrumentation.targetContext
        val model = File(app.cacheDir, "profile-native-test.onnx")
        val image = File(app.cacheDir, "profile-native-test.png")
        try {
            model.writeBytes(android.util.Base64.decode("CAg6hQQKKwoFaW5wdXQSBG5jaHciCVRyYW5zcG9zZSoRCgRwZXJtQABAA0ABQAKgAQcKIQoEbmNodwoCeHMKAnh6EgFxIg5RdWFudGl6ZUxpbmVhcgoxCgFxCgJ4cwoCeHoKAXcKAndzCgJ3egoCeXMKAnl6EgRjb252IgtRTGluZWFyQ29udgojCgRjb252CgJ5cwoCeXoSAWYiEERlcXVhbnRpemVMaW5lYXIKHAoBZhIEbWVhbiIRR2xvYmFsQXZlcmFnZVBvb2wKIgoEbWVhbhIEZmxhdCIHRmxhdHRlbioLCgRheGlzGAGgAQIKFgoEZmxhdAoEbm9ybRIDcmVkIgNEaXYKKAoDcmVkCgR6ZXJvEgZvdXRwdXQiBkNvbmNhdCoLCgRheGlzGAGgAQISF3Byb2ZpbGUtbmF0aXZlLWNvbnRyYWN0KgwQAUICeHNKBAAAgD8qCRACQgJ4ekoBACoSCAEIAwgBCAEQAkIBd0oDAAABKgwQAUICd3NKBAAAgD8qCRACQgJ3ekoBACoMEAFCAnlzSgQAAIA/KgkQAkICeXpKAQAqDhABQgRub3JtSgQAAH9DKhIIAQgBEAFCBHplcm9KBAAAAABaIQoFaW5wdXQSGAoWCAESEgoCCAEKAwjAAwoDCMADCgIIA2IYCgZvdXRwdXQSDgoMCAESCAoCCAEKAggCQgQKABAN", android.util.Base64.DEFAULT))
            val bitmap = Bitmap.createBitmap(448,448,Bitmap.Config.ARGB_8888).apply { eraseColor(Color.RED) }
            try { image.outputStream().use { bitmap.compress(Bitmap.CompressFormat.PNG,100,it) } } finally { bitmap.recycle() }
            OfflineImageTagger.fromFile(app, model, listOf(TaggerLabel("red",0),TaggerLabel("other",0))).use { tagger ->
                val tags = tagger.predict(Uri.fromFile(image))
                assertEquals(1,tags.size); assertEquals("red",tags.single().name)
                assertEquals(1f,tags.single().confidence,.001f)
            }
        } finally { model.delete(); image.delete() }
    }
}
