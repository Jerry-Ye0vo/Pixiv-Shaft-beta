package ceui.pixiv.ui.profile

import android.content.Intent
import android.content.res.ColorStateList
import android.graphics.Canvas
import android.graphics.Paint
import android.graphics.drawable.GradientDrawable
import android.net.Uri
import android.os.Bundle
import android.view.Gravity
import android.view.View
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.widget.Toolbar
import androidx.core.content.ContextCompat
import androidx.core.content.res.ResourcesCompat
import androidx.core.view.isVisible
import androidx.core.view.updatePadding
import androidx.fragment.app.Fragment
import androidx.fragment.app.viewModels
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import ceui.lisa.R
import ceui.lisa.activities.TemplateActivity
import ceui.pixiv.download.model.QueuedModels
import ceui.pixiv.download.queue.ContentPlanQueue
import ceui.pixiv.download.queue.ContentQueueStore.State
import ceui.pixiv.ui.navigation.BottomSafeInsets
import ceui.pixiv.ui.navigation.TemplateRoute
import ceui.pixiv.witstudio.theme.V3Palette
import ceui.pixiv.witstudio.theme.pressScale
import com.blankj.utilcode.util.BarUtils
import android.widget.RadioGroup
import androidx.appcompat.widget.AppCompatRadioButton
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.text.DateFormat
import java.util.Date

/** Retained statistics, local-only analysis, and explicitly requested offline model jobs. */
class ProfileFragment : Fragment(R.layout.fragment_local_profile) {
    private val model by viewModels<ProfileViewModel>()
    private var ui: Ui? = null
    private var source = 0

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        source = savedInstanceState?.getInt("profile_source") ?: source
        view.findViewById<Toolbar>(R.id.toolbar).apply {
            fitsSystemWindows = false
            updatePadding(top = BarUtils.getStatusBarHeight())
            setNavigationOnClickListener { requireActivity().finish() }
        }
        view.findViewById<TextView>(R.id.toolbar_title).setText(R.string.profile_title)
        BottomSafeInsets.applyTo(view.findViewById(R.id.profile_scroll))
        val content = view.findViewById<LinearLayout>(R.id.profile_content)
        ui = Ui(content)
        ui!!.refresh.setOnClickListener { model.load(force = true) }
        ui!!.library.setOnClickListener { route(TemplateRoute.MY_ILLUST_COLLECTION) }
        ui!!.source.setOnCheckedChangeListener { _, id ->
            if (id != -1) { source = id - SOURCE_ID; renderStatistics(model.state.value) }
        }
        ui!!.modelAction.setOnClickListener {
            val app = requireContext().applicationContext
            val ticket = LocalTaggerModel.current(app)
            when (ticket?.status?.value?.state) {
                State.RUNNING, State.QUEUED -> ticket.pause()
                State.PAUSED, State.FAILED -> QueuedModels.get(app).queue.resume(ticket.id)
                else -> LocalTaggerModel.download(app)
            }
        }
        ui!!.tagAction.setOnClickListener {
            val worker = QueuedProfileTagging.get(requireContext())
            val ticket = worker.current()
            when (ticket?.status?.value?.state) {
                State.RUNNING, State.QUEUED -> ticket.pause()
                State.PAUSED, State.FAILED -> worker.resume(ticket.id)
                else -> worker.start()
            }
        }
        ui!!.forceAction.setOnClickListener { QueuedProfileTagging.get(requireContext()).start(force = true) }
        ui!!.attribution.setOnClickListener {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse("https://huggingface.co/Smashinfries/wd-convnext-tagger-v3-mobile")))
        }
        viewLifecycleOwner.lifecycleScope.launch {
            viewLifecycleOwner.repeatOnLifecycle(Lifecycle.State.STARTED) {
                launch { model.state.collect { renderStatistics(it) } }
                launch {
                    var previousDone: String? = null
                    ContentPlanQueue.get(requireContext()).states.collect {
                        val app = requireContext().applicationContext
                        val worker = QueuedProfileTagging.get(app)
                        val ticket = worker.current()
                        val detail = withContext(Dispatchers.IO) { ticket?.let(worker::progress) }
                        renderTasks(detail)
                        if (ticket?.status?.value?.state == State.SUCCEEDED && ticket.id != previousDone) {
                            previousDone = ticket.id; model.refreshPredictions()
                        }
                        delay(500)
                    }
                }
            }
        }
    }
    override fun onResume() { super.onResume(); model.load() }
    override fun onSaveInstanceState(outState: Bundle) { super.onSaveInstanceState(outState); outState.putInt("profile_source", source) }
    override fun onDestroyView() { ui = null; super.onDestroyView() }
    private fun route(route: TemplateRoute) = startActivity(Intent(requireContext(), TemplateActivity::class.java)
        .putExtra(TemplateActivity.EXTRA_FRAGMENT, route.key))

    private var rendered: ProfileStatistics? = null
    private var renderedSource = -1
    private var renderedTags: List<ProfileRank>? = null
    private fun renderStatistics(state: ProfileUiState) {
        val u = ui ?: return
        u.refresh.isEnabled = !state.loading
        u.refresh.alpha = if (state.loading) .5f else 1f
        u.progress.isVisible = state.loading
        u.status.text = when {
            state.error != null -> getString(R.string.profile_error, state.error)
            state.loading -> if (state.expected > 0) getString(R.string.profile_refreshing, state.scanned, state.expected)
                else getString(R.string.profile_mirror_loading)
            state.statistics != null -> getString(R.string.profile_updated,
                DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(Date(state.statistics.builtAt)), state.uid)
            else -> getString(R.string.profile_empty)
        }
        val stats = state.statistics
        if (stats !== rendered || renderedSource != source || u.statistics.childCount == 0) {
            rendered = stats; renderedSource = source; u.statistics.removeAllViews()
            if (stats == null) { if (!state.loading) u.statistics.addView(u.text(getString(R.string.profile_empty))) }
            else {
                u.card(u.statistics, R.string.profile_overview) {
                    addView(u.text(getString(R.string.profile_summary, stats.downloads.works, stats.downloadRecords,
                        stats.bookmarks.works, stats.overlap), 18f, true))
                    addView(u.text(getString(R.string.profile_coverage, stats.completedShelves, stats.totalShelves)))
                    addView(u.text(getString(R.string.profile_records_note, stats.unidentifiedRecords)))
                }
                val slice = when (source) { 1 -> stats.downloads; 2 -> stats.bookmarks; else -> stats.all }
                u.card(u.statistics, R.string.profile_preference) {
                    addView(u.text(getString(R.string.profile_metadata_coverage, slice.works, slice.taggedWorks)))
                    addView(u.text(getString(R.string.profile_tag_title), 17f, true))
                    u.ranks(this, slice.tags, slice.works, R.string.profile_no_tags)
                    addView(u.text(getString(R.string.profile_author_title), 17f, true))
                    u.ranks(this, slice.authors, slice.works, R.string.profile_no_authors)
                }
                u.card(u.statistics, R.string.profile_work_types) {
                    for ((kind, count) in slice.types) addView(u.text("${getString(when (kind) {
                        "illust" -> R.string.profile_type_illust; "manga" -> R.string.profile_type_manga
                        "ugoira" -> R.string.profile_type_ugoira; "novel" -> R.string.profile_type_novel
                        else -> R.string.profile_type_unknown
                    })}  ·  $count"))
                    if (slice.works == 0) addView(u.text(getString(R.string.profile_empty)))
                }
                u.card(u.statistics, R.string.profile_trend) {
                    addView(u.text(getString(R.string.profile_trend_total, stats.days.first().date,
                        stats.days.last().date, stats.days.sumOf { it.count })))
                    addView(ProfileTrendView(requireContext(), stats.days), LinearLayout.LayoutParams(-1, u.dp(100)))
                }
            }
        }
        if (renderedTags !== state.predictedTags || u.predictions.childCount == 0) {
            renderedTags = state.predictedTags; u.predictions.removeAllViews()
            u.predictions.addView(u.text(getString(R.string.profile_tagging_cache, state.analyzedImages)))
            u.ranks(u.predictions, state.predictedTags, state.analyzedImages, R.string.profile_tagging_empty)
        }
    }

    private fun renderTasks(progress: ProfileTagProgress?) {
        val u = ui ?: return
        val app = requireContext().applicationContext
        val ready = LocalTaggerModel.ready(app)
        val download = LocalTaggerModel.current(app)?.status?.value
        val job = QueuedProfileTagging.get(app).current()?.status?.value
        val active = setOf(State.RUNNING, State.QUEUED, State.PAUSE_REQUESTED, State.CANCEL_REQUESTED)
        u.modelState.text = when {
            ready -> getString(R.string.profile_model_ready)
            download?.error != null -> getString(R.string.profile_error, download.error)
            download != null && download.total > 0 -> getString(R.string.profile_model_progress,
                (download.bytes * 100 / download.total).toInt().coerceIn(0, 100))
            download != null -> getString(R.string.profile_model_waiting)
            else -> getString(R.string.profile_tagging_intro)
        }
        u.modelAction.isVisible = !ready
        u.modelAction.setText(when (download?.state) {
            State.RUNNING, State.QUEUED -> R.string.profile_pause_model
            State.PAUSED, State.FAILED -> R.string.profile_resume_model
            else -> R.string.profile_model_download
        })
        u.tagAction.isEnabled = ready && job?.state !in setOf(State.PAUSE_REQUESTED, State.CANCEL_REQUESTED)
        u.tagAction.alpha = if (u.tagAction.isEnabled) 1f else .5f
        u.tagAction.setText(when (job?.state) {
            State.RUNNING, State.QUEUED -> R.string.profile_tagging_pause
            State.PAUSED, State.FAILED -> R.string.profile_tagging_resume
            else -> R.string.profile_tagging_start
        })
        u.forceAction.isVisible = ready && job?.state !in active
        u.tagProgress.text = if (job?.error != null) getString(R.string.profile_error, job.error)
            else progress?.let { getString(R.string.profile_tagging_result, it.scanned, it.inferred,
                it.reused, it.skipped, it.failed, it.perMinute) } ?: ""
    }

    private inner class Ui(val content: LinearLayout) {
        val palette = V3Palette.from(content.context)
        val refresh: TextView; val status: TextView; val progress: ProgressBar; val library: TextView
        val statistics: LinearLayout; val source: RadioGroup
        val modelState: TextView; val modelAction: TextView; val tagAction: TextView
        val forceAction: TextView; val tagProgress: TextView; val attribution: TextView; val predictions: LinearLayout
        init {
            content.addView(text(getString(R.string.profile_title), 28f, true))
            content.addView(text(getString(R.string.profile_intro), 15f))
            content.addView(text(getString(R.string.profile_scope)))
            refresh = button(R.string.profile_refresh, primary = true); content.addView(refresh)
            progress = ProgressBar(content.context, null, android.R.attr.progressBarStyleHorizontal).apply { isIndeterminate = true }
            content.addView(progress, LinearLayout.LayoutParams(-1, dp(4)).apply { topMargin = dp(12) })
            status = text(""); content.addView(status)
            library = button(R.string.profile_open_bookmarks); content.addView(library)
            source = RadioGroup(content.context).apply {
                orientation = LinearLayout.HORIZONTAL
                listOf(R.string.profile_all, R.string.profile_downloads, R.string.profile_bookmarks).forEachIndexed { n, label ->
                    addView(AppCompatRadioButton(context).apply {
                        id = SOURCE_ID + n; setText(label); minHeight = dp(48)
                        setTextColor(palette.textSecondary)
                        buttonTintList = ColorStateList.valueOf(palette.textAccent)
                    }, LinearLayout.LayoutParams(0, -2, 1f))
                }
                check(SOURCE_ID + this@ProfileFragment.source)
            }; content.addView(source)
            statistics = column(); content.addView(statistics)
            val tagCard = card(content, R.string.profile_tagging_title) {}
            modelState = text(getString(R.string.profile_tagging_intro)); tagCard.addView(modelState)
            modelAction = button(R.string.profile_model_download); tagCard.addView(modelAction)
            tagAction = button(R.string.profile_tagging_start, primary = true).apply { isEnabled = false }; tagCard.addView(tagAction)
            forceAction = button(R.string.profile_tagging_force).apply { isVisible = false }; tagCard.addView(forceAction)
            tagCard.addView(text(getString(R.string.profile_tagging_note)))
            tagProgress = text(""); tagCard.addView(tagProgress)
            predictions = column(); tagCard.addView(predictions)
            attribution = button(R.string.profile_model_source); tagCard.addView(attribution)
        }
        fun dp(value: Int) = (value * resources.displayMetrics.density).toInt()
        fun column() = LinearLayout(content.context).apply { orientation = LinearLayout.VERTICAL }
        fun text(value: String, size: Float = 14f, bold: Boolean = false) = TextView(content.context).apply {
            text = value; textSize = size
            setTextColor(if (bold) ContextCompat.getColor(context, R.color.v3_text_1) else palette.textSecondary)
            if (bold) typeface = ResourcesCompat.getFont(context, ceui.pixiv.witstudio.R.font.montserrat_semi_bold)
            setPadding(0, dp(8), 0, dp(8))
            layoutParams = LinearLayout.LayoutParams(-1, -2)
        }
        fun button(label: Int, primary: Boolean = false) = text(getString(label), 14f, true).apply {
            gravity = Gravity.CENTER; minHeight = dp(48); isClickable = true; isFocusable = true
            setPadding(dp(18), dp(10), dp(18), dp(10))
            background = if (primary) palette.pillPrimary(dp(24).toFloat()) else palette.pillSecondary(dp(24).toFloat(), dp(1))
            setTextColor(if (primary) palette.onPrimary else palette.textAccent)
            pressScale()
            layoutParams = LinearLayout.LayoutParams(-1, -2).apply { topMargin = dp(8) }
        }
        fun card(parent: LinearLayout, title: Int, body: LinearLayout.() -> Unit): LinearLayout = column().apply {
            background = GradientDrawable().apply { setColor(palette.cardFill); cornerRadius = dp(22).toFloat(); setStroke(dp(1), palette.cardHairline) }
            setPadding(dp(18), dp(12), dp(18), dp(16))
            layoutParams = LinearLayout.LayoutParams(-1, -2).apply { topMargin = dp(16) }
            addView(text(getString(title), 20f, true)); body(); parent.addView(this)
        }
        fun ranks(parent: LinearLayout, ranks: List<ProfileRank>, total: Int, empty: Int) {
            if (ranks.isEmpty()) { parent.addView(text(getString(empty))); return }
            for (rank in ranks.take(12)) {
                parent.addView(text("${rank.name}  ·  ${getString(R.string.profile_rank_count, rank.count,
                    if (total > 0) rank.count * 100 / total else 0)}"))
                parent.addView(ProgressBar(content.context, null, android.R.attr.progressBarStyleHorizontal).apply {
                    max = total.coerceAtLeast(1); progress = rank.count
                    progressTintList = ColorStateList.valueOf(palette.textAccent)
                    importantForAccessibility = View.IMPORTANT_FOR_ACCESSIBILITY_NO
                }, LinearLayout.LayoutParams(-1, dp(4)))
            }
        }
    }
    private companion object { const val SOURCE_ID = 0x00c54000 }
}

/** Small native chart, with a text alternative containing every date/count for screen readers. */
internal class ProfileTrendView(context: android.content.Context, private val days: List<ProfileDay>) : View(context) {
    private val paint = Paint(Paint.ANTI_ALIAS_FLAG).apply { color = V3Palette.from(context).textAccent }
    init { contentDescription = days.joinToString("; ") { "${it.date}: ${it.count}" }; importantForAccessibility = IMPORTANT_FOR_ACCESSIBILITY_YES }
    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)
        if (days.isEmpty()) return
        val step = width.toFloat() / days.size
        val maximum = days.maxOf { it.count }.coerceAtLeast(1)
        days.forEachIndexed { n, day ->
            val bar = (height - 8) * day.count.toFloat() / maximum
            canvas.drawRoundRect(n * step + 2, height - bar - 4, (n + 1) * step - 2,
                height - 4f, 3f, 3f, paint)
        }
    }
}

/** Constrain content before measuring so scroll views and large text share the same width. */
class ProfileContentLayout @JvmOverloads constructor(
    context: android.content.Context, attrs: android.util.AttributeSet? = null,
) : LinearLayout(context, attrs) {
    override fun onMeasure(widthMeasureSpec: Int, heightMeasureSpec: Int) {
        val cap = (720 * resources.displayMetrics.density).toInt()
        val width = if (MeasureSpec.getMode(widthMeasureSpec) == MeasureSpec.UNSPECIFIED) cap
            else minOf(MeasureSpec.getSize(widthMeasureSpec), cap)
        super.onMeasure(MeasureSpec.makeMeasureSpec(width, MeasureSpec.EXACTLY), heightMeasureSpec)
    }
}
