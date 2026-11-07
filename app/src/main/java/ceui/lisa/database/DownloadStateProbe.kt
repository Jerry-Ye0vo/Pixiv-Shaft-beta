package ceui.lisa.database

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi

/**
 * 单车道 dispatcher，专门给 [DownloadDao.hasDownloadRecordByIllustId] 这类
 * “这幅画下过没” 的探测串行化。
 *
 * 常规查询使用 illustId 索引；回填窗口内仍可能分批解析未索引行的 JSON。
 * 保留单车道限制，避免多 P ViewPager 同时探测占满 Room 读连接、阻塞界面查库。
 */
@OptIn(ExperimentalCoroutinesApi::class)
val downloadProbeDispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(1)

/**
 * “这幅画下过没” 的统一探测入口。优先走 v38 的 illustId 索引（O(log n)）；只有在存量
 * [DownloadIdBackfill] 尚未跑完时，才分批解析 illustId=0 旧行的顶层 JSON ID。
 * 回填完成后直接返回索引结果；小说同号及嵌套作者 ID 不算插画已下载。
 *
 * 阻塞式 DAO 调用，必须在 IO / [downloadProbeDispatcher] 上调。
 */
fun DownloadDao.hasDownloadRecord(illustId: Long): Boolean {
    if (hasDownloadRecordByIllustIdIndexed(illustId)) return true
    if (!DownloadIdBackfill.isComplete()) {
        return hasDownloadRecordByIllustId(illustId)
    }
    return false
}
