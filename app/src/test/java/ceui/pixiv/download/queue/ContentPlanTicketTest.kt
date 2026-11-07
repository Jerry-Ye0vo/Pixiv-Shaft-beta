package ceui.pixiv.download.queue

import ceui.pixiv.download.queue.ContentQueueStore.State
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test

class ContentPlanTicketTest {
    @Test fun acknowledgementCannotEraseACancelAcceptedBeforeItsSignalIsDelivered(): Unit = runBlocking {
        for (state in listOf(State.PAUSED, State.FAILED, State.QUEUED)) {
            val ticket = ContentPlanTicket("parent", "family", "model")
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            ticket.signal = { entered.countDown(); check(release.await(5, TimeUnit.SECONDS)) }
            val cancelled = async(Dispatchers.Default) { ticket.cancel() }
            try {
                withContext(Dispatchers.IO) { assertTrue(entered.await(5, TimeUnit.SECONDS)) }
                ticket.finished(state)
            } finally { release.countDown() }
            assertTrue(cancelled.await())
            assertEquals(ContentPlanTicket.CANCEL, ticket.stop())
            assertFalse(ticket.reopen())
        }
    }
}
