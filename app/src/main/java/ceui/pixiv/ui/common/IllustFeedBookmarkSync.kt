package ceui.pixiv.ui.common

import android.content.Context
import android.view.View
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import androidx.recyclerview.widget.RecyclerView
import ceui.lisa.database.AppDatabase
import ceui.pixiv.cache.BookmarkLedger
import ceui.pixiv.cache.IllustBookmarks
import ceui.pixiv.db.BookmarkKnowledgeEntity
import ceui.pixiv.db.mirror.MirrorContentType
import ceui.pixiv.feeds.FeedItem
import ceui.pixiv.feeds.FeedViewModel
import ceui.pixiv.services.appServices
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import timber.log.Timber

/** Replay viewer state into retained items; never write old item metadata back into ObjectPool. */
internal fun overlayIllustBookmarks(items: List<FeedItem>, ledger: BookmarkLedger): List<FeedItem> {
    var changed: ArrayList<FeedItem>? = null
    items.forEachIndexed { index, item ->
        if (item is IllustFeedItem) {
            val next = ledger.overlay(item.illust)
            if (next !== item.illust) {
                if (changed == null) changed = ArrayList(items)
                changed!![index] = IllustFeedItem(next, item.hidden)
            }
        }
    }
    return changed ?: items
}

/** Only viewport IDs touch Room on layout/scroll or mirror invalidation, not the entire deep feed. */
internal class IllustFeedBookmarkSync(
    private val model: FeedViewModel<String>,
    private val ledger: BookmarkLedger,
    private val loadKnown: suspend (Long, List<Long>) -> List<BookmarkKnowledgeEntity>,
    private val mirrored: suspend (List<Long>) -> Set<Long>,
    private val mirrorChanges: Flow<*>,
) {
    private val viewportChanges = Channel<Unit>(Channel.CONFLATED)
    fun visibleChanged() { viewportChanges.trySend(Unit) }

    fun bind(owner: LifecycleOwner, list: RecyclerView, visibleIds: () -> List<Long>) {
        val scroll = object : RecyclerView.OnScrollListener() {
            override fun onScrolled(recyclerView: RecyclerView, dx: Int, dy: Int) = visibleChanged()
        }
        val layout = View.OnLayoutChangeListener { _, _, _, _, _, _, _, _, _ -> visibleChanged() }
        list.addOnScrollListener(scroll)
        list.addOnLayoutChangeListener(layout)
        owner.lifecycle.addObserver(object : DefaultLifecycleObserver {
            override fun onDestroy(owner: LifecycleOwner) {
                list.removeOnScrollListener(scroll)
                list.removeOnLayoutChangeListener(layout)
                viewportChanges.close()
            }
        })
        owner.lifecycleScope.launch {
            owner.repeatOnLifecycle(Lifecycle.State.STARTED) {
                val checked = HashSet<Long>()
                var mirrorRevision = 0L
                fun applyKnown() = model.mutateItems { overlayIllustBookmarks(it, ledger) }
                launch { ledger.changes.collect { applyKnown() } }
                launch {
                    var lastItems: List<FeedItem>? = null
                    model.uiState.collect {
                        if (it.items !== lastItems) {
                            lastItems = it.items
                            applyKnown()
                            visibleChanged()
                        }
                    }
                }
                launch { mirrorChanges.collect { mirrorRevision++; checked.clear(); visibleChanged() } }
                launch {
                    visibleChanged()
                    for (ignored in viewportChanges) {
                        val ids = visibleIds().distinct().filter { it !in checked }
                        if (ids.isEmpty()) continue
                        val stamp = ledger.request()
                        val queryRevision = mirrorRevision
                        try {
                            ledger.restore(loadKnown(stamp.ownerUid, ids), stamp.ownerUid)
                            val hits = mirrored(ids)
                            if (queryRevision != mirrorRevision) { visibleChanged(); continue }
                            ledger.mirror(ids, hits, stamp.ownerUid)
                            if (ledger.request().ownerUid == stamp.ownerUid) checked.addAll(ids)
                            applyKnown()
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (error: Exception) {
                            Timber.w(error, "收藏状态回填失败，保留当前卡片状态")
                        }
                    }
                }
            }
        }
    }

    companion object {
        fun create(context: Context, model: FeedViewModel<String>): IllustFeedBookmarkSync {
            val db = AppDatabase.getAppDatabase(context.applicationContext)
            val mirror = context.appServices().bookmarkMirror
            return IllustFeedBookmarkSync(model, IllustBookmarks.state,
                loadKnown = { uid, ids -> withContext(Dispatchers.IO) { db.bookmarkKnowledgeDao().knownAmong(uid, ids) } },
                mirrored = { ids -> mirror.bookmarkedAmong(MirrorContentType.ILLUST, ids) },
                mirrorChanges = db.invalidationTracker.createFlow("bookmark_mirror_table"))
        }
    }
}
