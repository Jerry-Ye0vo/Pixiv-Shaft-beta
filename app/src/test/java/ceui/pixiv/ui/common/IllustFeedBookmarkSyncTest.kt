package ceui.pixiv.ui.common

import android.app.Application
import androidx.arch.core.executor.testing.InstantTaskExecutorRule
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import androidx.recyclerview.widget.RecyclerView
import ceui.pixiv.api.model.Illust
import ceui.pixiv.cache.BookmarkLedger
import ceui.pixiv.cache.IllustBookmarks
import ceui.pixiv.cache.ObjectPool
import ceui.pixiv.feeds.FeedPage
import ceui.pixiv.feeds.FeedSource
import ceui.pixiv.feeds.FeedViewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.*
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28], application = Application::class)
class IllustFeedBookmarkSyncTest {
    @get:Rule val instant = InstantTaskExecutorRule()
    private val dispatcher = StandardTestDispatcher()
    private val owners = mutableListOf<Owner>()
    private class Owner : LifecycleOwner {
        val registry = LifecycleRegistry(this)
        override val lifecycle: Lifecycle get() = registry
    }
    @Before fun setup() { Dispatchers.setMain(dispatcher) }
    @After fun cleanup() {
        owners.forEach { it.registry.currentState = Lifecycle.State.DESTROYED }
        Dispatchers.resetMain()
    }
    private fun model() = FeedViewModel(object : FeedSource<String> {
        override suspend fun load(cursor: String?): FeedPage<String> = error("bookmark sync must not reload content")
    }, autoLoad = false)
    private fun bind(sync: IllustFeedBookmarkSync, ids: List<Long>): Owner {
        val owner = Owner().also { owners += it }
        owner.registry.currentState = Lifecycle.State.CREATED
        sync.bind(owner, RecyclerView(RuntimeEnvironment.getApplication())) { ids }
        owner.registry.currentState = Lifecycle.State.STARTED
        return owner
    }

    @Test fun fullDetailPoolConfirmationReplaysIntoTheCachedFeedWithoutAnActionBroadcast() = runTest(dispatcher) {
        val id = 81_112L
        val vm = model()
        vm.appendItems(listOf(IllustFeedItem(Illust(id = id, title = "cached", is_bookmarked = false), true)))
        val sync = IllustFeedBookmarkSync(vm, IllustBookmarks.state, { _, _ -> emptyList() }, { emptySet() }, MutableStateFlow(0))
        bind(sync, listOf(id)); runCurrent()
        ObjectPool.update(Illust(id = id, title = "detail", is_bookmarked = true), isFullVersion = true)
        runCurrent()
        val result = vm.uiState.value.items.single() as IllustFeedItem
        assertTrue(result.illust.isBookmarked)
        assertEquals("cached", result.illust.title)
        assertTrue(result.hidden)
    }

    @Test fun mirrorCompletionOnlyQueriesVisibleIdsAndViewRecreationReplaysMissedUnbookmark() = runTest(dispatcher) {
        val ledger = BookmarkLedger { 1 }
        val vm = model()
        vm.appendItems((1L..100L).map { IllustFeedItem(Illust(id = it, is_bookmarked = false)) })
        var hits = emptySet<Long>()
        val changes = MutableStateFlow(0)
        val queries = mutableListOf<List<Long>>()
        fun sync() = IllustFeedBookmarkSync(vm, ledger, { _, ids -> queries += ids; emptyList() }, { hits }, changes)
        val first = bind(sync(), listOf(42)); runCurrent()
        assertEquals(listOf(listOf(42L)), queries)
        hits = setOf(42); changes.value++
        runCurrent()
        assertTrue((vm.uiState.value.items[41] as IllustFeedItem).illust.isBookmarked)
        first.registry.currentState = Lifecycle.State.DESTROYED
        ledger.local(42, false)
        bind(sync(), listOf(42)); runCurrent()
        assertFalse((vm.uiState.value.items[41] as IllustFeedItem).illust.isBookmarked)
        assertTrue(queries.all { it == listOf(42L) })
    }
}
