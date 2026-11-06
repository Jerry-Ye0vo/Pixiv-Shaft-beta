package ceui.pixiv.snapshot

import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

/**
 * .shaftsnap 本质是 ZIP。这里只负责目录 <=> ZIP 字节的转换，
 * 不碰 SAF、不碰快照库目录规划。
 */
object SnapshotArchive {

    /** 把整个快照目录（含 manifest/json/图片）打成 ZIP 写到 [out]。 */
    fun zipDirectory(dir: File, out: OutputStream, check: () -> Unit = {}) {
        if (!dir.isDirectory) throw SnapshotException("快照目录不存在: ${dir.absolutePath}")
        ZipOutputStream(out.buffered()).use { zip ->
            dir.walkTopDown()
                .filter { it.isFile }
                .forEach { file ->
                    check()
                    val relative = file.relativeTo(dir).invariantSeparatorsPath
                    zip.putNextEntry(ZipEntry(relative))
                    file.inputStream().buffered().use { input -> copyChecked(input, zip, check) }
                    zip.closeEntry()
                }
        }
    }

    /** 解压 ZIP 到 [targetDir]，过滤掉路径穿越条目。 */
    fun unzip(input: InputStream, targetDir: File, check: () -> Unit = {}) {
        targetDir.mkdirs()
        val names = HashSet<String>()
        var total = 0L
        var count = 0
        ZipInputStream(input.buffered()).use { zip ->
            var entry = zip.nextEntry
            while (entry != null) {
                check()
                if (++count > 100_000) throw SnapshotException("快照文件数量过多")
                val name = sanitizeEntryName(entry.name)
                if (!names.add(name)) throw SnapshotException("快照包含重复条目: $name")
                if (name.isNotEmpty() && !entry.isDirectory) {
                    val target = File(targetDir, name)
                    if (!target.canonicalPath.startsWith(targetDir.canonicalPath + File.separator)) throw SnapshotException("非法快照路径")
                    target.parentFile?.mkdirs()
                    java.io.FileOutputStream(target).use { out ->
                        total += copyChecked(zip, out, check, 128L * 1024 * 1024 * 1024 - total)
                        out.fd.sync()
                    }
                }
                zip.closeEntry()
                entry = zip.nextEntry
            }
        }
    }

    private fun sanitizeEntryName(name: String): String {
        val normalized = name.replace('\\', '/')
        if (normalized.startsWith('/') || normalized.split('/').any { it == ".." || it.contains('\u0000') })
            throw SnapshotException("非法快照条目: $name")
        val cleaned = normalized
            .split('/')
            .filter { it.isNotEmpty() && it != "." && it != ".." }
            .joinToString("/")
        if (cleaned.startsWith("/") || cleaned.contains("../")) {
            throw SnapshotException("非法快照条目: $name")
        }
        return cleaned
    }
    private fun copyChecked(input: InputStream, output: OutputStream, check: () -> Unit, maximum: Long = Long.MAX_VALUE): Long {
        val buffer = ByteArray(32 * 1024); var total = 0L
        while (true) {
            check(); val n = input.read(buffer); if (n < 0) break
            if (total > maximum - n) throw SnapshotException("快照展开大小超过上限")
            output.write(buffer, 0, n); total += n
        }
        return total
    }
}
