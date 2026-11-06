package ceui.pixiv.download.model

import com.google.gson.Gson
import java.io.File
import java.io.IOException
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ModelInstallerTest {
    @get:Rule val temporary = TemporaryFolder()
    private val request = ModelRequest(directory = "rife-models", asset = "test", required = listOf("model.onnx"),
        operation = ModelRequest.DOWNLOAD, source = "https://example.com/model.zip")
    private fun installer(id: String = "one", spec: ModelRequest = request): ModelInstaller =
        ModelInstaller(temporary.root, spec, id, Gson().toJson(spec))
    private fun zip(vararg entries: Pair<String, String>): File {
        val file = temporary.newFile()
        ZipOutputStream(file.outputStream()).use { out -> entries.forEach { (path, data) ->
            out.putNextEntry(ZipEntry(path)); out.write(data.toByteArray()); out.closeEntry()
        } }
        return file
    }
    private fun old(worker: ModelInstaller) { worker.target.mkdirs(); File(worker.target, "model.onnx").writeText("previous") }
    private suspend fun fails(block: suspend () -> Unit) {
        try { block(); fail("Expected invalid input to fail") } catch (expected: IOException) { }
    }
    @Test fun missingRequiredFilesNeverReplaceAnExistingInstallation() = runBlocking {
        val worker = installer(); old(worker)
        fails { worker.prepare(zip("other" to "bytes"), "hash") }
        assertEquals("previous", File(worker.target, "model.onnx").readText()); assertFalse(worker.backup.exists())
    }
    @Test fun emptyRequiredFilesNeverCountAsReady() = runBlocking {
        val worker = installer(); old(worker)
        fails { worker.prepare(zip("model.onnx" to ""), "hash") }
        assertEquals("previous", File(worker.target, "model.onnx").readText())
    }
    @Test fun zipSlipCannotWriteOutsideOwnedStagingOrRemoveOldModel() = runBlocking {
        val worker = installer(); old(worker)
        fails { worker.prepare(zip("../escape" to "bad", "model.onnx" to "new"), "hash") }
        assertFalse(File(worker.target.parentFile, "escape").exists())
        assertEquals("previous", File(worker.target, "model.onnx").readText())
    }
    @Test fun reservedReceiptCannotBeSuppliedByAnArchive() = runBlocking {
        val worker = installer()
        fails { worker.prepare(zip(ModelInstaller.RECEIPT to "{}", "model.onnx" to "new"), "hash") }
        assertFalse(worker.target.exists())
    }
    @Test fun validSwapRetainsOldDirectoryUntilDurableParentAcknowledgement() = runBlocking {
        val worker = installer(); old(worker)
        worker.prepare(zip("model.onnx" to "new", "extra.txt" to "allowed"), "hash")
        assertEquals("previous", File(worker.target, "model.onnx").readText())
        worker.install(); assertTrue(worker.installed())
        assertEquals("previous", File(worker.backup, "model.onnx").readText())
        worker.cleanup(true); assertFalse(worker.backup.exists()); assertTrue(worker.installed())
    }
    @Test fun interruptedSwapRestoresPreviousInstallationBeforeRetry() = runBlocking {
        val worker = installer(); old(worker)
        assertTrue(worker.target.renameTo(worker.backup)); assertFalse(worker.target.exists())
        worker.recover(); assertEquals("previous", File(worker.target, "model.onnx").readText())
    }
    @Test fun finalRenameFailureRollsBackAndKeepsValidatedStagingForInspection() = runBlocking {
        val worker = ModelInstaller(temporary.root, request, "one", Gson().toJson(request)) { from, to ->
            !from.name.endsWith(".staging") && from.renameTo(to)
        }; old(worker)
        worker.prepare(zip("model.onnx" to "new"), "hash")
        fails { worker.install() }
        assertEquals("previous", File(worker.target, "model.onnx").readText()); assertFalse(worker.backup.exists())
        assertTrue(worker.staging.isDirectory)
    }
    @Test fun receiptRequiresTheSamePlanRequestAndActualFileDigest() = runBlocking {
        val worker = installer(); worker.prepare(zip("model.onnx" to "new"), "hash"); worker.install()
        assertTrue(worker.installed()); assertFalse(installer("other").installed())
        File(worker.target, "model.onnx").writeText("bad")
        assertFalse(worker.installed())
    }
    @Test fun validatedDeleteMovesOldDirectoryBeforeRemovingItsBytes() = runBlocking {
        val worker = installer(spec = request.copy(operation = ModelRequest.DELETE)); old(worker)
        worker.delete(); assertFalse(worker.target.exists()); assertTrue(worker.backup.exists())
        worker.cleanup(true); assertFalse(worker.backup.exists())
    }
    @Test fun allFrozenPathsAreValidatedBeforeAccessingTheFilesystem() {
        for (bad in listOf(request.copy(asset = "../test"), request.copy(directory = "elsewhere"),
            request.copy(required = listOf("../model")), request.copy(required = listOf("a\\b")), request.copy(version = 9))) {
            try { bad.target(temporary.root); fail("Unsafe frozen request accepted") } catch (expected: IllegalArgumentException) { }
        }
    }
}
