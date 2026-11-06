package ceui.pixiv.ui.download

import ceui.pixiv.download.queue.ContentQueueStore.State
import org.junit.Assert.*
import org.junit.Test

class UnifiedContentTasksTest {
    private fun row(state: State, owner: TaskOwner = TaskOwner.PLAN) = ContentTaskRow("id", owner, "Novel", "Book", state)
    @Test fun aGroupOutputAndABatchUgoiraNeverDoubleCountTheDisplayedParent() {
        assertFalse(showContentParent("plan:group:output:0", true))
        assertFalse(showContentParent("ugoira-batch:42", false))
        assertTrue(showContentParent("ugoira:42", false))
        assertTrue(showContentParent("novel-group:42", false))
    }
    @Test fun finishedReceiptsDoNotEnterActiveCountsOrChangeArtworkPageCounts() {
        assertFalse(row(State.SUCCEEDED).active); assertFalse(row(State.CANCELLED).active)
        assertTrue(row(State.FAILED).active); assertTrue(row(State.PAUSED).active)
    }
    @Test fun aRunningModelOrApkMakesTheGlobalActionPause() {
        assertEquals(ActiveToolbarAction.PAUSE, resolveUnifiedToolbarAction(ActiveToolbarAction.RESUME, false, listOf(row(State.RUNNING))))
        assertEquals(ActiveToolbarAction.PAUSE, resolveUnifiedToolbarAction(ActiveToolbarAction.RETRY, true, listOf(row(State.RUNNING, TaskOwner.APK))))
    }
    @Test fun aPageWithOnlyPausedExtraTasksCanResumeWithoutALegacyImage() {
        assertEquals(ActiveToolbarAction.RESUME, resolveUnifiedToolbarAction(ActiveToolbarAction.PAUSE, false, listOf(row(State.PAUSED))))
    }
    @Test fun retryOnlyAppliesWhenAllRemainingExtraTasksFailed() {
        assertEquals(ActiveToolbarAction.RETRY, resolveUnifiedToolbarAction(ActiveToolbarAction.PAUSE, false, listOf(row(State.FAILED))))
        assertEquals(ActiveToolbarAction.RESUME, resolveUnifiedToolbarAction(ActiveToolbarAction.RETRY, true, listOf(row(State.QUEUED))))
    }
    @Test fun acceptingAStopHasItsOwnStateAndDoesNotOfferAnotherResume() {
        assertFalse(row(State.CANCEL_REQUESTED).resumable); assertFalse(row(State.PAUSE_REQUESTED).resumable)
        assertTrue(row(State.QUEUED).resumable)
    }
}
