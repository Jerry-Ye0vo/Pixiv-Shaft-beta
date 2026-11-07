package ceui.pixiv.ui.profile

import android.app.Application
import android.graphics.Bitmap
import android.graphics.Canvas
import android.os.Looper
import android.view.View
import android.widget.LinearLayout
import android.widget.TextView
import androidx.fragment.app.FragmentActivity
import androidx.lifecycle.Lifecycle
import ceui.lisa.R
import ceui.lisa.activities.Shaft
import ceui.lisa.utils.Settings
import ceui.pixiv.session.SessionManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.Robolectric
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.annotation.GraphicsMode
import org.robolectric.util.ReflectionHelpers
import java.io.File

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
@GraphicsMode(GraphicsMode.Mode.NATIVE)
class ProfileScreenTest {
    private val dispatcher = StandardTestDispatcher()
    private var oldSettings: Settings? = null
    @Before fun setup() { Dispatchers.setMain(dispatcher); oldSettings = Shaft.sSettings; Shaft.sSettings = Settings() }
    @After fun cleanup() { Shaft.sSettings = oldSettings; Dispatchers.resetMain() }

    @Test fun phoneDayAndWideNightLargeTextKeepReadableControlsAndBoundedContent() = runTest(dispatcher) {
        for ((qualifiers, scale) in listOf("zh-rCN-w411dp-h891dp-notnight-mdpi" to 1f, "zh-rTW-w1000dp-h900dp-night-mdpi" to 1.6f)) {
            RuntimeEnvironment.setQualifiers(qualifiers)
            val app: Application = RuntimeEnvironment.getApplication()
            app.resources.configuration.let { config ->
                config.fontScale = scale
                @Suppress("DEPRECATION") app.resources.updateConfiguration(config, app.resources.displayMetrics)
            }
            val controller = Robolectric.buildActivity(FragmentActivity::class.java)
            val activity = controller.get().apply { setTheme(R.style.AppTheme) }
            controller.create().start().resume()
            val fragment = ProfileFragment()
            activity.supportFragmentManager.beginTransaction().add(android.R.id.content,fragment)
                .setMaxLifecycle(fragment,Lifecycle.State.CREATED).commitNow()
            val model = ProfileViewModel(app)
            ReflectionHelpers.getField<Job>(model,"analysis").cancel()
            val rank = listOf(ProfileRank("dress","服饰 / Clothing with a long readable label",1))
            val slice = ProfileSlice(2,1,rank,listOf(ProfileRank("10","Artist",2)),mapOf("illust" to 1,"manga" to 1))
            val stats = ProfileStatistics(SessionManager.loggedInUid,100,3,0,1,slice,slice,slice,
                (1..30).map { ProfileDay("2026-11-${it.toString().padStart(2,'0')}",it%5) },2)
            ReflectionHelpers.getField<MutableStateFlow<ProfileUiState>>(model,"mutable").value =
                ProfileUiState(SessionManager.loggedInUid,stats,loading=false)
            fragment.viewModelStore.put("androidx.lifecycle.ViewModelProvider.DefaultKey:${ProfileViewModel::class.java.name}",model)
            activity.supportFragmentManager.beginTransaction().setMaxLifecycle(fragment,Lifecycle.State.RESUMED).commitNow()
            dispatcher.scheduler.runCurrent(); shadowOf(Looper.getMainLooper()).idle()
            val root = fragment.requireView()
            val width = if (qualifiers.contains("1000")) 1000 else 411
            root.measure(View.MeasureSpec.makeMeasureSpec(width,View.MeasureSpec.EXACTLY),
                View.MeasureSpec.makeMeasureSpec(900,View.MeasureSpec.EXACTLY)); root.layout(0,0,width,900)
            dispatcher.scheduler.runCurrent(); shadowOf(Looper.getMainLooper()).idle()
            root.measure(View.MeasureSpec.makeMeasureSpec(width,View.MeasureSpec.EXACTLY),
                View.MeasureSpec.makeMeasureSpec(900,View.MeasureSpec.EXACTLY)); root.layout(0,0,width,900)
            assertEquals(activity.getString(R.string.profile_title),root.findViewById<TextView>(R.id.toolbar_title).text.toString())
            val content = root.findViewById<LinearLayout>(R.id.profile_content)
            assertTrue("width=${content.measuredWidth}, density=${root.resources.displayMetrics.density}", content.measuredWidth<=720 * root.resources.displayMetrics.density)
            fun texts(view: View): List<TextView> = if (view is android.view.ViewGroup)
                (0 until view.childCount).flatMap { texts(view.getChildAt(it)) } else listOfNotNull(view as? TextView)
            val source = content.getChildAt(7) as android.widget.RadioGroup
            assertEquals("The active statistics source must be checked", 0x00c54000, source.checkedRadioButtonId)
            assertTrue((source.getChildAt(0) as android.widget.RadioButton).isChecked)
            source.getChildAt(1).performClick()
            assertEquals(0x00c54001, source.checkedRadioButtonId)
            source.getChildAt(0).performClick()
            val controls = texts(root).filter { it.isClickable && it.visibility == View.VISIBLE && it.text.isNotBlank() }
            assertTrue(controls.any { it.text.toString()==activity.getString(R.string.profile_refresh) })
            assertTrue(controls.any { it.text.toString()==activity.getString(R.string.profile_model_download) })
            assertTrue(controls.joinToString { "${it.text}: ${it.measuredHeight}" }, controls.all { it.measuredHeight>=48 })
            val bitmap = Bitmap.createBitmap(width,900,Bitmap.Config.ARGB_8888)
            try {
                root.draw(Canvas(bitmap))
                val file = File("build/profile-previews/${if (scale==1f) "phone-day" else "wide-night-large-text"}.png")
                file.parentFile!!.mkdirs(); file.outputStream().use { bitmap.compress(Bitmap.CompressFormat.PNG,100,it) }
            } finally { bitmap.recycle() }
            controller.pause().stop().destroy(); dispatcher.scheduler.runCurrent()
            ceui.pixiv.download.queue.ContentPlanQueue.get(app).shutdown()
        }
    }
}
