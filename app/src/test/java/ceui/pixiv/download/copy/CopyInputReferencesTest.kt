package ceui.pixiv.download.copy

import java.io.File
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test

class CopyInputReferencesTest {
    private fun scenario(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking { withTimeout(10_000, block) }
    @Test fun twoConsumersAndRepeatedCloseCannotDeleteAnInputStillInUse() = scenario {
        val file = File.createTempFile("copy-pins-", ".png").apply { writeText("translated image") }
        val first = CopyInputReferences.reserve(file)
        val second = CopyInputReferences.reserve(file)
        try {
            CopyInputReferences.requestDelete(file).join()
            assertTrue(file.exists())
            first.close(); first.close()
            CopyInputReferences.requestDelete(file).join()
            assertTrue(file.exists())
            second.close()
            withTimeout(5_000) { while (file.exists()) delay(10) }
        } finally { first.close(); second.close(); file.delete() }
    }

    @Test fun slowFilesystemDeletionDoesNotHoldTheReservationMonitor() = scenario {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val path = File(System.getProperty("java.io.tmpdir"), "slow-delete-${UUID.randomUUID()}.png").absolutePath
        val slow = object : File(path) {
            override fun getAbsoluteFile(): File = this
            override fun delete(): Boolean {
                entered.countDown(); check(release.await(5, TimeUnit.SECONDS)); return super.delete()
            }
        }.apply { writeText("old source") }
        val deletion = CopyInputReferences.requestDelete(slow)
        val other = File.createTempFile("other-pin-", ".png")
        try {
            withContext(Dispatchers.IO) { check(entered.await(5, TimeUnit.SECONDS)) }
            val reservation = async(Dispatchers.Default) { CopyInputReferences.reserve(other) }
            withTimeout(1_000) { reservation.await().close() }
            assertTrue(runCatching { CopyInputReferences.reserve(slow) }.isFailure)
        } finally { release.countDown(); deletion.join(); other.delete() }
    }
}
