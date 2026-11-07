package ceui.pixiv.cache

import ceui.pixiv.api.model.Illust
import ceui.pixiv.db.BookmarkKnowledgeEntity
import ceui.pixiv.ui.common.IllustFeedItem
import ceui.pixiv.ui.common.overlayIllustBookmarks
import kotlinx.coroutines.CancellationException
import org.junit.Assert.*
import org.junit.Test

class BookmarkLedgerTest {
    @Test fun detailConfirmationUpdatesOnlyBookmarkInRetainedSnapshot() {
        val ledger = BookmarkLedger { 1 }
        val cached = IllustFeedItem(Illust(id = 42, title = "retained title", is_bookmarked = false), true)
        val list = listOf(cached)
        ledger.response(Illust(id = 42, title = "detail title", is_bookmarked = true), ledger.request())
        val updated = overlayIllustBookmarks(list, ledger).single() as IllustFeedItem
        assertTrue(updated.illust.isBookmarked)
        assertEquals("retained title", updated.illust.title)
        assertTrue(updated.hidden)
        assertFalse(cached.illust.isBookmarked)
        assertSame(list, overlayIllustBookmarks(list, BookmarkLedger { 2 }))
    }

    @Test fun oldResponseCannotUndoALocalActionButANewerServerRequestCanConfirmCancellation() {
        val ledger = BookmarkLedger { 1 }
        val inFlight = ledger.request()
        ledger.local(42, true)
        assertTrue(ledger.response(Illust(id = 42, is_bookmarked = false), inFlight).isBookmarked)
        val later = ledger.request()
        assertFalse(ledger.response(Illust(id = 42, is_bookmarked = false), later).isBookmarked)
    }

    @Test fun unchangedConfirmationStillFencesAnOlderRequest() {
        val ledger = BookmarkLedger { 1 }
        ledger.local(42, true)
        // An omitted server field is unknown, rather than evidence of an unbookmark.
        assertTrue(ledger.response(Illust(id = 42, is_bookmarked = null), ledger.request()).isBookmarked)
        val old = ledger.request()
        ledger.response(Illust(id = 42, is_bookmarked = true), ledger.request())
        assertTrue(ledger.response(Illust(id = 42, is_bookmarked = false), old).isBookmarked)
    }

    @Test fun mirrorIsPositiveEvidenceAndCannotOverrideAnExplicitUnbookmarkOrRollback() {
        val ledger = BookmarkLedger { 1 }
        ledger.mirror(listOf(42, 43), setOf(42), 1)
        assertEquals(true, ledger.state(42)?.value)
        assertNull(ledger.state(43)?.value)
        ledger.local(42, false)
        ledger.mirror(listOf(42), setOf(42), 1)
        assertEquals(false, ledger.state(42)?.value)
        ledger.mirror(listOf(43), setOf(43), 1)
        ledger.mirror(listOf(43), emptySet(), 1)
        assertNull(ledger.state(43)?.value)
    }

    @Test fun restoredKnowledgeWorksWithoutARegisteredMirrorAndCannotOverwriteNewerMemory() {
        val ledger = BookmarkLedger { 1 }
        ledger.restore(listOf(BookmarkKnowledgeEntity(1, 42, true)), 1)
        assertTrue(ledger.overlay(Illust(id = 42, is_bookmarked = false)).isBookmarked)
        ledger.local(42, false)
        ledger.restore(listOf(BookmarkKnowledgeEntity(1, 42, true)), 1)
        assertEquals(false, ledger.state(42)?.value)
    }

    @Test fun loadingOldDiskKnowledgeDoesNotFenceAnAlreadyInFlightFreshRequest() {
        val ledger = BookmarkLedger { 1 }
        val network = ledger.request()
        ledger.restore(listOf(BookmarkKnowledgeEntity(1, 42, false)), 1)
        assertTrue(ledger.response(Illust(id = 42, is_bookmarked = true), network).isBookmarked)
    }

    @Test fun accountSwitchDoesNotExposeOrAcceptThePreviousViewersState() {
        var uid = 1L
        val ledger = BookmarkLedger { uid }
        ledger.local(42, true)
        val request = ledger.request()
        uid = 2
        assertNull(ledger.state(42))
        assertThrows(CancellationException::class.java) { ledger.response(Illust(id = 42, is_bookmarked = true), request) }
        ledger.local(42, false)
        uid = 1
        assertEquals(true, ledger.state(42)?.value)
    }
}
