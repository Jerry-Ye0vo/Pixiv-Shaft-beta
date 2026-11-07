package ceui.pixiv.cache

import android.content.Context
import ceui.lisa.database.AppDatabase
import ceui.pixiv.api.model.Illust
import ceui.pixiv.db.BookmarkKnowledgeEntity
import ceui.pixiv.session.SessionManager
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow

/** Viewer state is separate from retained artwork/feed snapshots. No image or caption lives here. */
internal class BookmarkLedger(private val owner: () -> Long) {
    data class Request(val ownerUid: Long, val revision: Long)
    data class Key(val ownerUid: Long, val illustId: Long)
    data class State(val value: Boolean?, val revision: Long, val authoritative: Boolean)

    private var revision = 0L
    private val states = object : LinkedHashMap<Key, State>(256, .75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, State>) = size > 8192
    }
    private val ticks = MutableSharedFlow<Unit>(replay = 1, onBufferOverflow = BufferOverflow.DROP_OLDEST)
        .apply { tryEmit(Unit) }
    val changes = ticks.asSharedFlow()
    var persist: ((Key, Boolean) -> Unit)? = null

    @Synchronized fun request() = Request(owner(), revision)
    @Synchronized fun state(id: Long): State? = states[Key(owner(), id)]
    @Synchronized fun local(id: Long, value: Boolean) { write(Key(owner(), id), value, true, true) }

    /** Only current-viewer network responses enter here; borrowed/shared payloads never do. */
    @Synchronized fun response(illust: Illust, request: Request): Illust {
        if (request.ownerUid != owner()) throw CancellationException("Bookmark response belongs to a previous account")
        val key = Key(request.ownerUid, illust.id)
        val known = states[key]
        if (known != null && known.authoritative && known.revision > request.revision) return overlay(illust)
        illust.is_bookmarked?.let { write(key, it, true, true) }
        return overlay(illust)
    }

    @Synchronized fun confirmed(id: Long, value: Boolean) { write(Key(owner(), id), value, true, true) }

    @Synchronized fun mirror(ids: Collection<Long>, bookmarked: Set<Long>, ownerUid: Long) {
        if (ownerUid != owner()) return
        for (id in ids) {
            val key = Key(ownerUid, id)
            if (states[key]?.authoritative != true) write(key, true.takeIf { id in bookmarked }, false, false)
        }
    }

    @Synchronized fun restore(rows: List<BookmarkKnowledgeEntity>, ownerUid: Long) {
        if (ownerUid != owner()) return
        for (row in rows) {
            val key = Key(ownerUid, row.illustId)
            if (states[key]?.authoritative != true) {
                // A disk observation predates every request in this process. Loading it later
                // must not make it newer than an already in-flight server request.
                states[key] = State(row.bookmarked, 0L, true)
                ticks.tryEmit(Unit)
            }
        }
    }

    @Synchronized fun overlay(illust: Illust): Illust {
        val known = states[Key(owner(), illust.id)] ?: return illust
        return if (known.value == illust.is_bookmarked) illust else illust.copy(is_bookmarked = known.value)
    }

    @Synchronized fun forget(ownerUid: Long) {
        states.keys.removeAll { it.ownerUid == ownerUid }
        ticks.tryEmit(Unit)
    }

    private fun write(key: Key, value: Boolean?, authoritative: Boolean, durable: Boolean) {
        val old = states[key]
        // Every authoritative observation advances the request fence, even if the boolean is unchanged.
        states[key] = State(value, ++revision, authoritative)
        if (durable && value != null && (old?.value != value || old?.authoritative != true)) persist?.invoke(key, value)
        if (old == null || old.value != value || old.authoritative != authoritative) ticks.tryEmit(Unit)
    }
}

internal object IllustBookmarks {
    val state = BookmarkLedger { SessionManager.loggedInUid }

    /** One serial writer preserves confirmation/rollback order without putting Room on the UI thread. */
    fun initialize(context: Context) {
        val app = context.applicationContext
        val writes = Channel<BookmarkKnowledgeEntity>(Channel.UNLIMITED)
        state.persist = { key, value ->
            if (key.ownerUid > 0L) writes.trySend(BookmarkKnowledgeEntity(key.ownerUid, key.illustId, value))
        }
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            val dao = AppDatabase.getAppDatabase(app).bookmarkKnowledgeDao()
            for (first in writes) {
                val batch = linkedMapOf(BookmarkLedger.Key(first.ownerUid, first.illustId) to first)
                while (batch.size < 200) {
                    val row = writes.tryReceive().getOrNull() ?: break
                    batch[BookmarkLedger.Key(row.ownerUid, row.illustId)] = row
                }
                try {
                    dao.remember(batch.values.toList())
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Exception) {
                    timber.log.Timber.w(error, "保存已确认收藏状态失败，保留进程内状态")
                }
            }
        }
    }
}
