package ceui.pixiv.ui.detail

import ceui.lisa.core.DownloadItem
import ceui.lisa.activities.Shaft
import com.blankj.utilcode.util.NetworkUtils

/** Project mutable queue items into an immutable state before Flow.distinctUntilChanged. */
internal fun resolvePageDownloadFabState(
    items: List<DownloadItem>,
    illustId: Long,
    page: Int,
    isWifiConnected: Boolean = NetworkUtils.isWifiConnected(),
    downloadLimitType: Int = Shaft.sSettings?.downloadLimitType ?: 0,
    dispatchScheduled: Boolean = false,
): DownloadFab? {
    val current = items.filter { it.illust.id == illustId && it.index == page }
    // An empty live queue does not prove the page was saved: the caller probes the done table.
    if (current.isEmpty()) return null
    return resolveDownloadFabState(current, 1, isWifiConnected, downloadLimitType, dispatchScheduled)
}
