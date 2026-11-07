package ceui.pixiv.ui.profile

import org.junit.Assert.*
import org.junit.Test

class ProfileStatisticsTest {
    private fun work(id: Long, kind: String = "illust", tags: List<Pair<String, String>> = listOf("Dress" to "服饰")) =
        ProfileMetadata(id, kind, 10, "Artist", tags)

    @Test fun multipageAndRepeatedSavesCountOncePerWorkAndSource() {
        val acc = ProfileAccumulator()
        repeat(12) { acc.accept(work(42), bookmark = false) }
        repeat(2) { acc.accept(work(42), bookmark = true) }
        assertEquals(1, acc.snapshot(0).works)
        assertEquals(1, acc.snapshot(1).works)
        assertEquals(1, acc.snapshot(2).works)
        assertEquals(1, acc.overlap)
        assertEquals(1, acc.snapshot(0).tags.single().count)
        assertEquals(1, acc.snapshot(0).authors.single().count)
    }

    @Test fun novelAndIllustrationIdsAreSeparateAndMissingMetadataCanBeFilledLater() {
        val acc = ProfileAccumulator()
        acc.accept(ProfileMetadata(42, "unknown"), false)
        acc.accept(work(42, "manga"), false)
        acc.accept(work(42, "novel"), false)
        assertEquals(2, acc.snapshot(0).works)
        assertEquals(mapOf("manga" to 1, "novel" to 1), acc.snapshot(0).types)
        assertEquals(2, acc.snapshot(0).taggedWorks)
    }

    @Test fun tagCaseAndDuplicatesDoNotInflateCountsAndMirrorWinsCombinedMetadata() {
        val acc = ProfileAccumulator()
        acc.accept(work(1, tags = listOf("DRESS" to "衣服", "dress" to "Dress")), true)
        acc.accept(work(1, tags = listOf("old_tag" to "Old")), false)
        acc.accept(work(2, tags = listOf("dress" to "Dress")), false)
        assertEquals(2, acc.snapshot(0).tags.single().count)
        assertEquals("衣服", acc.snapshot(0).tags.single().name)
        assertEquals(setOf("old_tag", "dress"), acc.snapshot(1).tags.map { it.key }.toSet())
    }

    @Test fun metadataParserSkipsNestedIdsAndLargeFieldsAndPreservesNovelIdentity() {
        val json = """{"user":{"id":8,"name":"A"},"meta_pages":[{"id":999}],"id":42,"type":"manga","caption":"ignored","tags":[{"name":"dress","translated_name":"服饰"},"solo"]}"""
        val value = ProfileMetadataReader.read(json)
        assertEquals(42L, value.id); assertEquals(8L, value.authorId)
        assertEquals("manga", value.kind)
        assertEquals(listOf("dress" to "服饰", "solo" to "solo"), value.tags)
        assertEquals("novel", ProfileMetadataReader.read(json, novel = true).kind)
    }

    @Test fun malformedOldJsonDoesNotPublishPartialTagsOrDropIndexedIdentity() {
        val broken = ProfileMetadataReader.read("""{"id":42,"user":{"id":10},"tags":[{"name":"dress"}""", 99)
        assertEquals(99L, broken.id); assertEquals(0L, broken.authorId)
        assertTrue(broken.tags.isEmpty())
        assertEquals(7L, ProfileMetadataReader.read(null, 7).id)
    }
}
