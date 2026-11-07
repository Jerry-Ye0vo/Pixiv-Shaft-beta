package ceui.pixiv.ui.profile

import androidx.annotation.Keep
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import java.io.StringReader
import java.util.Locale

@Keep internal data class ProfileRank(val key: String, val name: String, val count: Int)
@Keep internal data class ProfileSlice(
    val works: Int, val taggedWorks: Int, val tags: List<ProfileRank>,
    val authors: List<ProfileRank>, val types: Map<String, Int>,
)
@Keep internal data class ProfileDay(val date: String, val count: Int)
@Keep internal data class ProfileStatistics(
    val ownerUid: Long, val builtAt: Long, val downloadRecords: Int, val unidentifiedRecords: Int,
    val overlap: Int, val all: ProfileSlice, val downloads: ProfileSlice, val bookmarks: ProfileSlice,
    val days: List<ProfileDay>, val completedShelves: Int, val totalShelves: Int = 4,
)

internal data class ProfileMetadata(
    val id: Long, val kind: String, val authorId: Long = 0, val authorName: String = "",
    val tags: List<Pair<String, String>> = emptyList(),
)

/** Read only metadata; multi-page image URLs, captions and other large fields are skipped. */
internal object ProfileMetadataReader {
    fun read(json: String?, fallbackId: Long = 0, novel: Boolean = false): ProfileMetadata {
        var id = fallbackId
        var kind = if (novel) "novel" else "unknown"
        var authorId = 0L
        var authorName = ""
        val tags = mutableListOf<Pair<String, String>>()
        if (json.isNullOrBlank()) return ProfileMetadata(id, kind)
        try {
            JsonReader(StringReader(json)).use { r ->
                r.beginObject()
                while (r.hasNext()) when (r.nextName()) {
                    "id" -> if (r.peek() == JsonToken.NUMBER) id = r.nextLong() else r.skipValue()
                    "type" -> if (r.peek() == JsonToken.STRING) {
                        val value = r.nextString()
                        if (!novel && value in TYPES) kind = value
                    } else r.skipValue()
                    "user" -> if (r.peek() == JsonToken.BEGIN_OBJECT) {
                        r.beginObject()
                        while (r.hasNext()) when (r.nextName()) {
                            "id" -> if (r.peek() == JsonToken.NUMBER) authorId = r.nextLong() else r.skipValue()
                            "name" -> if (r.peek() == JsonToken.STRING) authorName = r.nextString() else r.skipValue()
                            else -> r.skipValue()
                        }
                        r.endObject()
                    } else r.skipValue()
                    "tags" -> if (r.peek() == JsonToken.BEGIN_ARRAY) {
                        r.beginArray()
                        while (r.hasNext()) {
                            if (r.peek() == JsonToken.STRING) {
                                r.nextString().takeIf { it.isNotBlank() }?.let { tags += it to it }
                            } else if (r.peek() == JsonToken.BEGIN_OBJECT) {
                                var name = ""; var translated = ""
                                r.beginObject()
                                while (r.hasNext()) when (r.nextName()) {
                                    "name" -> if (r.peek() == JsonToken.STRING) name = r.nextString() else r.skipValue()
                                    "translated_name" -> if (r.peek() == JsonToken.STRING) translated = r.nextString() else r.skipValue()
                                    else -> r.skipValue()
                                }
                                r.endObject()
                                if (name.isNotBlank()) tags += name to translated.ifBlank { name }
                            } else r.skipValue()
                        }
                        r.endArray()
                    } else r.skipValue()
                    else -> r.skipValue()
                }
                r.endObject()
            }
        } catch (_: Exception) {
            // Legacy/imported JSON can be absent or damaged. Retain only the indexed identity;
            // never publish a partially parsed author/tag observation.
            return ProfileMetadata(fallbackId, if (novel) "novel" else "unknown")
        }
        return ProfileMetadata(id, kind, authorId, authorName, tags)
    }
    val TYPES = listOf("unknown", "illust", "manga", "ugoira", "novel")
}

/** One small flags entry per work; no retained Illust objects or per-work tag lists. */
internal class ProfileAccumulator {
    private val seen = HashMap<Long, Int>()
    private val slices = Array(3) { Counts() }
    var overlap = 0; private set

    fun accept(metadata: ProfileMetadata, bookmark: Boolean) {
        if (metadata.id <= 0) return
        val key = if (metadata.kind == "novel") -metadata.id else metadata.id
        val source = if (bookmark) 2 else 1
        var flags = seen[key] ?: 0
        val previousSources = flags and 6
        val tags = metadata.tags.distinctBy { it.first.lowercase(Locale.ROOT) }
        for (slot in intArrayOf(0, source)) {
            val count = slices[slot]
            val present = 1 shl slot
            val typeOffset = 9 + slot * 3
            val oldKind = (flags ushr typeOffset) and 7
            val newKind = ProfileMetadataReader.TYPES.indexOf(metadata.kind).coerceAtLeast(0)
            if (flags and present == 0) {
                count.works++
                count.types[metadata.kind] = (count.types[metadata.kind] ?: 0) + 1
                flags = flags or present or (newKind shl typeOffset)
            } else if (oldKind == 0 && newKind != 0) {
                count.types["unknown"] = (count.types["unknown"] ?: 0) - 1
                count.types[metadata.kind] = (count.types[metadata.kind] ?: 0) + 1
                flags = flags or (newKind shl typeOffset)
            }
            val tagsSeen = 1 shl (3 + slot)
            if (tags.isNotEmpty() && flags and tagsSeen == 0) {
                count.taggedWorks++
                for ((name, display) in tags) count.tag(name.lowercase(Locale.ROOT), display)
                flags = flags or tagsSeen
            }
            val authorSeen = 1 shl (6 + slot)
            if (metadata.authorId > 0 && flags and authorSeen == 0) {
                count.author(metadata.authorId, metadata.authorName)
                flags = flags or authorSeen
            }
        }
        if (previousSources != 6 && flags and 6 == 6) overlap++
        seen[key] = flags
    }
    fun snapshot(slot: Int) = slices[slot].snapshot()

    private class Counts {
        var works = 0
        var taggedWorks = 0
        val types = linkedMapOf<String, Int>()
        private val tags = HashMap<String, ProfileRank>()
        private val authors = HashMap<String, ProfileRank>()
        fun tag(key: String, display: String) {
            val old = tags[key]
            tags[key] = ProfileRank(key, old?.name ?: display, (old?.count ?: 0) + 1)
        }
        fun author(id: Long, name: String) {
            val key = id.toString(); val old = authors[key]
            authors[key] = ProfileRank(key, old?.name?.takeIf { it != key } ?: name.ifBlank { key }, (old?.count ?: 0) + 1)
        }
        private fun top(values: Collection<ProfileRank>) = values.sortedWith(
            compareByDescending<ProfileRank> { it.count }.thenBy { it.key }).take(30)
        fun snapshot() = ProfileSlice(works, taggedWorks, top(tags.values), top(authors.values), types.filterValues { it > 0 })
    }
}
