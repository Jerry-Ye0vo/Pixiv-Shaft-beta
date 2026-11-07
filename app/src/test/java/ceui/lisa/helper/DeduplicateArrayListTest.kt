package ceui.lisa.helper

import ceui.lisa.models.Deduplicatable
import org.junit.Assert.*
import org.junit.Test

class DeduplicateArrayListTest {
    private data class Row(val id: Int, val text: String = "") : Deduplicatable {
        override fun getDuplicateKey(): Any = id
    }

    @Test fun bulkDedupKeepsTheFirstEqualElementAndStillAcceptsOtherKeys() {
        val first = Row(1)
        val rows = DeduplicateArrayList(listOf(first, first, Row(2)))
        assertEquals(listOf(first, Row(2)), rows)
        assertFalse(rows.add(first))
        rows.addAll(1, listOf(Row(3), Row(3), Row(2)))
        assertEquals(listOf(1, 3, 2), rows.map { it.id })
    }

    @Test fun mutationsKeepTheMembershipIndexConsistent() {
        val rows = DeduplicateArrayList(listOf(Row(1), Row(2), Row(3)))
        assertFalse(rows.remove(Row(1, "different object")))
        assertFalse(rows.add(Row(1)))
        rows.removeAll(listOf(Row(1)))
        assertTrue(rows.add(Row(1)))
        rows.retainAll(listOf(Row(1)))
        assertTrue(rows.add(Row(2)))
        rows.removeIf { it.id == 2 }
        rows[0] = Row(3)
        assertTrue(rows.add(Row(1)))
        rows.subList(0, 1).clear()
        assertTrue(rows.add(Row(3)))
    }

    @Test fun detailPagerAppendDedupsWithinTheNewPageAndAgainstTheRetainedPage() {
        val rows = arrayListOf(Row(1))
        DeduplicateArrayList.addAllWithNoRepeat(rows, listOf(Row(1), Row(2), Row(2), Row(3)))
        assertEquals(listOf(1, 2, 3), rows.map { it.id })
    }
}
