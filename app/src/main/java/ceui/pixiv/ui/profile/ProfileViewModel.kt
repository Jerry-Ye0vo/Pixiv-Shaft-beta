package ceui.pixiv.ui.profile

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import ceui.lisa.database.AppDatabase
import ceui.pixiv.session.SessionManager
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

internal data class ProfileUiState(
    val uid: Long = -1, val statistics: ProfileStatistics? = null, val loading: Boolean = true,
    val scanned: Int = 0, val expected: Int = 0, val error: String? = null,
    val predictedTags: List<ProfileRank> = emptyList(), val analyzedImages: Int = 0,
)

internal class ProfileViewModel(application: Application) : AndroidViewModel(application) {
    private val repository = ProfileRepository(application)
    private val mutable = MutableStateFlow(ProfileUiState())
    val state = mutable.asStateFlow()
    private var analysis: Job? = null
    init { load() }

    fun load(force: Boolean = false) {
        val uid = SessionManager.loggedInUid
        if (!force && analysis != null && mutable.value.uid == uid) return
        analysis?.cancel()
        if (mutable.value.uid != uid) mutable.value = ProfileUiState(uid = uid)
        analysis = viewModelScope.launch {
            try {
                if (!force) repository.cached(uid)?.let { snapshot ->
                    if (uid == SessionManager.loggedInUid) mutable.update { it.copy(statistics = snapshot, loading = false) }
                }
                if (force || mutable.value.statistics == null) {
                    mutable.update { it.copy(loading = true, error = null) }
                    val snapshot = repository.analyze(uid) { count, total ->
                        mutable.update { it.copy(scanned = count, expected = total) }
                    }
                    if (uid == SessionManager.loggedInUid) mutable.update { it.copy(statistics = snapshot, loading = false) }
                }
                refreshPredictions()
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (error: Exception) { mutable.update { it.copy(loading = false, error = error.message ?: error.javaClass.simpleName) } }
        }
    }

    suspend fun refreshPredictions() = withContext(Dispatchers.IO) {
        val dao = AppDatabase.getAppDatabase(getApplication()).profileAnalyticsDao()
        val hashes = HashSet<String>()
        val tags = HashMap<String, Int>()
        val type = object : TypeToken<List<PredictedImageTag>>() {}.type
        var after = 0L
        while (true) {
            kotlinx.coroutines.currentCoroutineContext().ensureActive()
            val rows = dao.tagBatch(LocalTaggerModel.VERSION, after, ProfileRepository.BATCH)
            if (rows.isEmpty()) break
            for (row in rows) if (hashes.add(row.sha256)) {
                val values: List<PredictedImageTag> = Gson().fromJson(row.tagsJson, type)
                for (tag in values.distinctBy { it.name }) tags[tag.name] = (tags[tag.name] ?: 0) + 1
            }
            after = rows.last().sequence
        }
        val ranked = tags.map { ProfileRank(it.key, it.key.replace('_', ' '), it.value) }
            .sortedWith(compareByDescending<ProfileRank> { it.count }.thenBy { it.key }).take(30)
        mutable.update { it.copy(predictedTags = ranked, analyzedImages = hashes.size) }
    }
}
