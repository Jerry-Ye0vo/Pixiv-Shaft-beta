package ceui.pixiv.imageloader

import android.app.Application
import ceui.lisa.activities.Shaft
import ceui.lisa.utils.Settings
import ceui.pixiv.cache.OriginalImageCacheManager
import ceui.pixiv.download.ImageSaveCacheGuard
import java.io.File
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.util.ReflectionHelpers

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class OriginalSaveCompatibilityTest {
    private val app get() = RuntimeEnvironment.getApplication()
    private val urls = mutableListOf<String>()

    @Before fun setUp() {
        Dispatchers.setMain(UnconfinedTestDispatcher())
        ReflectionHelpers.setStaticField(Shaft::class.java, "sContext", app)
        Shaft.sSettings = Settings()
    }

    @After fun tearDown() {
        ImageTaskRegistry.customFetcherResolver = null
        ImageSaveCacheGuard.resetForTests()
        urls.forEach(ImageTaskRegistry::remove)
        OriginalImageCacheManager.clearAll(app)
        Dispatchers.resetMain()
    }

    @Test fun `save joins the original task and cancelling its waiter leaves display loading`() = runTest {
        val url = "https://example.test/compat-shared-original.jpg".also(urls::add)
        val result = CompletableDeferred<File>()
        var fetches = 0
        ImageTaskRegistry.customFetcherResolver = { request ->
            assertTrue(request.isOriginal)
            object : ImageFetcher {
                override suspend fun fetch(url: String, onProgress: (Int) -> Unit): File {
                    fetches++
                    onProgress(37)
                    return result.await()
                }
            }
        }
        val display = ImageLoaderV3.obtainOriginal(url)
        val source = PageImageSourceResolver.resolve(app, null, 0, url) as PageImageSource.Remote
        assertSame(display, source.task)
        val waiter = async { source.awaitFile(app) }
        runCurrent()
        waiter.cancel()
        runCurrent()
        assertEquals(ImageLoadState.Loading(37), display.state.value)
        val file = File(app.cacheDir, "compat-ready.jpg").apply { writeBytes(byteArrayOf(1, 2, 3)) }
        result.complete(file)
        runCurrent()
        assertEquals(file, display.awaitFile())
        assertEquals(1, fetches)
    }

    @Test fun `reopened save can use original disk cache without creating a network task`() = runTest {
        val url = "https://example.test/compat-cold-original.jpg".also(urls::add)
        ImageTaskRegistry.remove(url)
        val cached = File(OriginalImageCacheManager.getCacheDir(app),
            OriginalImageCacheManager.keyForUrl(url) + ".jpg").apply { writeText("cached original") }
        ImageTaskRegistry.customFetcherResolver = { error("No fetch expected for persisted original cache") }
        val source = PageImageSourceResolver.resolve(app, null, 0, url) as PageImageSource.Local
        assertEquals(PageImageSource.Origin.Cached, source.origin)
        assertEquals(cached, source.awaitFile(app))
        assertNull(ImageTaskRegistry.peekTask(url))
    }

    @Test fun `explicit cache clearing respects a save lease and clears after release`() {
        val url = "https://example.test/compat-clear-original.jpg".also(urls::add)
        val cached = File(OriginalImageCacheManager.getCacheDir(app),
            OriginalImageCacheManager.keyForUrl(url) + ".jpg").apply { writeText("save source") }
        OriginalImageCacheManager.installSaveProtection(app)
        ImageSaveCacheGuard.retain(url).use {
            OriginalImageCacheManager.clearAll(app)
            assertTrue(cached.exists())
        }
        OriginalImageCacheManager.clearAll(app)
        assertFalse(cached.exists())
    }
}

