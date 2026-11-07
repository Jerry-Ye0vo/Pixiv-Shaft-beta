package ceui.pixiv.ui.download

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.ProgressBar
import android.widget.TextView
import androidx.recyclerview.widget.DiffUtil
import androidx.recyclerview.widget.ListAdapter
import androidx.recyclerview.widget.RecyclerView
import ceui.lisa.R
import ceui.pixiv.download.queue.ContentQueueStore.State
import androidx.fragment.app.Fragment
import androidx.lifecycle.lifecycleScope
import ceui.pixiv.ui.novel.reader.export.ExportFormat
import ceui.pixiv.witstudio.dialog.WitDialog
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first

/** Reuses the native download card, colors, typography and touch behavior. */
internal class ContentTaskAdapter(private val action: (ContentTaskRow) -> Unit,
    private val cancel: (ContentTaskRow) -> Unit, private val details: (ContentTaskRow) -> Unit) :
    ListAdapter<ContentTaskRow, ContentTaskAdapter.Holder>(object : DiffUtil.ItemCallback<ContentTaskRow>() {
        override fun areItemsTheSame(a: ContentTaskRow, b: ContentTaskRow) = a.id == b.id
        override fun areContentsTheSame(a: ContentTaskRow, b: ContentTaskRow) = a == b
    }) {
    class Holder(view: View) : RecyclerView.ViewHolder(view)
    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) = Holder(LayoutInflater.from(parent.context)
        .inflate(R.layout.cell_download_active_v3, parent, false))
    override fun onBindViewHolder(holder: Holder, position: Int) {
        val row = getItem(position); val view = holder.itemView
        view.findViewById<ImageView>(R.id.thumb).apply {
            setImageResource(R.drawable.ic_file_download_black_24dp); scaleType = ImageView.ScaleType.CENTER_INSIDE
        }
        view.findViewById<TextView>(R.id.taskName).text = "${row.category} · ${row.title}"
        view.findViewById<TextView>(R.id.sizeText).apply {
            isSingleLine = false; maxLines = 3
            text = row.error ?: if (row.needsFormat) context.getString(R.string.content_task_choose_format) else when {
                row.itemProgress && row.total > 0 -> context.getString(R.string.profile_task_items, row.bytes, row.total)
                row.itemProgress -> context.getString(R.string.profile_task_running)
                row.total > 0 -> "${row.bytes / 1024} / ${row.total / 1024} KB"
                row.phase.startsWith("series:") -> row.phase.replace("series:", "系列 ").replace(":chapter:", " · 章节 ").replace(":page:", " · 列表 ")
                row.running -> context.getString(R.string.content_task_processing)
                else -> context.getString(R.string.content_task_persisted)
            }
        }
        view.findViewById<TextView>(R.id.stateBadge).setText(when (row.state) {
            State.RUNNING, State.COMMITTED -> R.string.content_task_running
            State.PAUSED -> R.string.content_task_paused
            State.FAILED -> R.string.content_task_failed
            State.SUCCEEDED -> R.string.content_task_done
            State.CANCELLED -> R.string.content_task_cancelled
            State.PAUSE_REQUESTED, State.CANCEL_REQUESTED, State.YIELD_REQUESTED -> R.string.content_task_stopping
            else -> R.string.content_task_waiting
        })
        view.findViewById<ProgressBar>(R.id.progress).apply {
            visibility = if (row.running) View.VISIBLE else View.GONE
            isIndeterminate = row.total <= 0
            if (row.total > 0) progress = ((row.bytes.toDouble() / row.total) * 100).toInt().coerceIn(0, 100)
        }
        view.findViewById<TextView>(R.id.percentText).text = if (row.total > 0) "${((row.bytes.toDouble() / row.total) * 100).toInt()}%" else ""
        view.findViewById<ImageView>(R.id.pauseBtn).apply {
            visibility = if (row.active) View.VISIBLE else View.GONE
            setImageResource(if (row.resumable) R.drawable.ic_v3_resume_all_24 else R.drawable.ic_baseline_pause_24)
            isEnabled = row.resumable || row.state == State.RUNNING
            contentDescription = context.getString(if (row.resumable) R.string.content_task_resume else R.string.content_task_pause)
            setOnClickListener { action(row) }
        }
        view.findViewById<ImageView>(R.id.cancelBtn).apply {
            visibility = if (row.active) View.VISIBLE else View.GONE
            isEnabled = row.state != State.COMMITTED
            contentDescription = context.getString(R.string.content_task_cancel)
            setOnClickListener { cancel(row) }
        }
        view.setOnClickListener { details(row) }
    }
}

internal fun Fragment.showContentTaskFormat(tasks: UnifiedContentTasks, row: ContentTaskRow) {
    val host = activity ?: return
    val builder = WitDialog.MenuDialogBuilder(host).setTitle(R.string.content_task_choose_format)
    ExportFormat.entries.forEach { format ->
        builder.addItem(format.extension.uppercase()) { dialog, _ ->
            dialog.dismiss(); viewLifecycleOwner.lifecycleScope.launch { tasks.resume(row, format) }
        }
    }
    builder.show()
}
internal fun Fragment.showContentTaskDetails(tasks: UnifiedContentTasks, row: ContentTaskRow) {
    val host = activity ?: return
    val builder = WitDialog.MessageDialogBuilder(host).setTitle(row.title)
        .setMessage("${row.category}\n${row.error ?: if (row.needsFormat) getString(R.string.content_task_choose_format) else getString(if (row.active) R.string.content_task_persisted else if (row.state == State.CANCELLED) R.string.content_task_cancelled else R.string.content_task_done)}")
        .addAction(R.string.sure) { dialog, _ -> dialog.dismiss() }
    if (row.group && row.active) builder.addAction(R.string.content_task_finish_partial) { dialog, _ ->
        dialog.dismiss(); tasks.finishGroup(row)
        if (row.needsFormat) showContentTaskFormat(tasks, row)
    }
    builder.show()
}
internal fun Fragment.showContentTaskHistory(tasks: UnifiedContentTasks) {
    viewLifecycleOwner.lifecycleScope.launch {
        val rows = tasks.rows.first().filter { !it.active }.takeLast(128).asReversed()
        val host = activity ?: return@launch
        if (rows.isEmpty()) {
            WitDialog.MessageDialogBuilder(host).setTitle(R.string.content_task_recent)
                .setMessage(R.string.content_task_recent_empty).addAction(R.string.sure) { d, _ -> d.dismiss() }.show()
        } else {
            val builder = WitDialog.MenuDialogBuilder(host).setTitle(R.string.content_task_recent)
            rows.forEach { row -> builder.addItem("${row.category} · ${row.title}") { d, _ -> d.dismiss(); showContentTaskDetails(tasks, row) } }
            builder.show()
        }
    }
}
