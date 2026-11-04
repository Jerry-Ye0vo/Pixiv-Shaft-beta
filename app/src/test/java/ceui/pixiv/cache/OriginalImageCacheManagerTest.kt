package ceui.pixiv.cache

import ceui.pixiv.download.StageStore
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.FileOutputStream

class OriginalImageCacheManagerTest {

    private lateinit var cacheDir: File
    private lateinit var stagingDir: File

    @Before
    fun setUp() {
        cacheDir = File.createTempFile("orig_cache_", "_d").apply { delete(); mkdirs() }
        stagingDir = File.createTempFile("orig_staging_", "_d").apply { delete(); mkdirs() }
    }

    @After
    fun tearDown() {
        cacheDir.listFiles()?.forEach { it.delete() }
        cacheDir.delete()
        stagingDir.listFiles()?.forEach { it.delete() }
        stagingDir.delete()
    }

    @Test
    fun `keyForUrl produces stable sha256 prefix`() {
        val url = "https://i.pximg.net/img-original/img/2026/01/01/00/00/00/12345_p0.jpg"
        val key = OriginalImageCacheManager.keyForUrl(url)
        assertEquals(40, key.length)
        assertEquals(StageStore.keyForUrl(url), key)
    }

    @Test
    fun `LRU trim removes oldest files when exceeding max bytes`() {
        val f1 = File(cacheDir, "f1.jpg").apply {
            writeBytes(ByteArray(1024 * 1024 * 2)) // 2 MB
            setLastModified(1000L)
        }
        val f2 = File(cacheDir, "f2.jpg").apply {
            writeBytes(ByteArray(1024 * 1024 * 3)) // 3 MB
            setLastModified(2000L)
        }
        val f3 = File(cacheDir, "f3.jpg").apply {
            writeBytes(ByteArray(1024 * 1024 * 4)) // 4 MB
            setLastModified(3000L)
        }

        assertEquals(9L * 1024 * 1024, cacheDir.listFiles()?.filter { it.isFile }?.sumOf { it.length() })

        OriginalImageCacheManager.trimDirectory(cacheDir, 5L * 1024 * 1024)

        // f1 was oldest (1000L) -> deleted first, reducing to 7MB.
        // f2 was next oldest (2000L) -> deleted next, reducing to 4MB <= 5MB.
        // f3 (3000L) remains.
        assertFalse(f1.exists())
        assertFalse(f2.exists())
        assertTrue(f3.exists())
    }

    @Test fun `save source survives quota trimming until its lease is released`() {
        val url = "https://example.test/retained.jpg"
        val source = File(cacheDir, OriginalImageCacheManager.keyForUrl(url) + ".jpg").apply {
            writeBytes(ByteArray(200)); setLastModified(1000L)
        }
        val other = File(cacheDir, "other.jpg").apply { writeBytes(ByteArray(100)); setLastModified(2000L) }
        val lease = OriginalImageCacheManager.retain(url)
        try {
            OriginalImageCacheManager.trimDirectory(cacheDir, 50L)
            assertTrue(source.exists())
            assertFalse(other.exists())
        } finally { lease.close() }
        OriginalImageCacheManager.trimDirectory(cacheDir, 50L)
        assertFalse(source.exists())
    }

    @Test fun `shared leases are reference counted and release is idempotent`() {
        val url = "https://example.test/shared-lease.jpg"
        val source = File(cacheDir, OriginalImageCacheManager.keyForUrl(url) + ".jpg").apply { writeText("image") }
        val first = OriginalImageCacheManager.retain(url)
        val second = OriginalImageCacheManager.retain(url)
        try {
            first.close(); first.close()
            OriginalImageCacheManager.trimDirectory(cacheDir, 0L)
            assertTrue(source.exists())
        } finally { first.close(); second.close() }
        OriginalImageCacheManager.trimDirectory(cacheDir, 0L)
        assertFalse(source.exists())
    }

    @Test fun `directory lookup and commit reuse the same file and clear its resume metadata`() {
        val url = "https://example.test/directory-cache.jpg"
        val key = StageStore.keyForUrl(url)
        val part = StageStore.partFile(stagingDir, key).apply { writeBytes(byteArrayOf(3, 7, 12)) }
        StageStore.writeManifest(StageStore.metaFile(stagingDir, key),
            StageStore.buildManifest(url, "etag", null, 3L))
        val committed = OriginalImageCacheManager.commitIn(cacheDir, stagingDir, url, part)
        assertEquals(committed, OriginalImageCacheManager.findIn(cacheDir, url))
        assertEquals(listOf<Byte>(3, 7, 12), committed.readBytes().toList())
        assertTrue(stagingDir.listFiles().orEmpty().isEmpty())
    }

    @Test fun `restoring an old partial file is protected from startup orphan sweeping`() {
        val url = "https://example.test/old-resume.jpg"
        val key = StageStore.keyForUrl(url)
        val part = StageStore.partFile(stagingDir, key).apply { writeText("partial"); setLastModified(1000L) }
        val meta = StageStore.metaFile(stagingDir, key).apply { writeText("metadata"); setLastModified(1000L) }
        OriginalImageCacheManager.retain(url).use {
            assertEquals(0, OriginalImageCacheManager.sweepDirectory(stagingDir, 500L, 5000L))
            assertTrue(part.exists())
            assertTrue(meta.exists())
        }
        assertEquals(2, OriginalImageCacheManager.sweepDirectory(stagingDir, 500L, 5000L))
        assertFalse(part.exists())
        assertFalse(meta.exists())
    }
}

