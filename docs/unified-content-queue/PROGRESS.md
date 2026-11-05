# 统一内容队列迁移进度

基线已发布：[4.9.9-beta.14](https://github.com/Jerry-Ye0vo/Pixiv-Shaft-beta/releases/tag/v4.9.9-beta.14)，源码 79eea653，原 APK 和固定签名。
开发分支：feat/unified-content-queue。这里的阶段状态不表示全量迁移已完成。

| 子流程 | 当前状态 |
| --- | --- |
| 0 原版本留档 | 已完成，校验原 artifact ZIP、APK checksum 及构建身份后发布 |
| 1 整理入口与分类 | 清单如下；按每类的真实网络/文件入口继续审计 |
| 2 单项恢复调度规则 | 本提交将直接派发改为待执行资格；新增满额连续恢复测试 |
| 3 持久化任务核心、原子资源许可与旧记录迁移 | 独立任务存储和原子领取核心已实现，测试中；入口接管、资源阶段编排与旧记录迁移待实施 |
| 4 原图共享传输、缓存与执行前复查 | 待实施 |
| 5 译图、动图、快照接入 | 待实施 |
| 6 APK/模型/小说导出接入 | 待实施 |
| 7 全链路恢复/并发/真机回归与新 Beta 构建 | 待实施 |

## 入口与改造需求

| 入口或组件 | 当前职责 | 改造 |
| --- | --- | --- |
| IllustDownload / ImageDetailActivity / PageDownloadSheet | 单页、选页、整部及恢复 | 只提交/恢复任务；所有启动走有界调度 |
| Manager / DownloadTask | 页面传输、状态、恢复与落盘 | 保留执行能力，拆出持久化任务与统一许可；处理过期回调 |
| QueueDownloadManager | 批量拆页、并发计数、动图及重试 | 改为分组/子任务计划与结果汇总，撤销独立派发权 |
| ImageLoaderV3 / ImageTaskRegistry / ResumableOkHttpFetcher | 原图浏览与缓存 | 原图请求入队，显示/保存共享同一资源；轻量缩略图仍即时加载 |
| ImagePageSaver | 译图本地复制 | 持久化输出意图、保护源文件，本地复制也受有界 IO 调度 |
| UgoiraEngine / UgoiraTask / IllustDownload.downloadGif | ZIP/帧/编码/导出 | 静态封面例外，完整资源统一传输；编码单独许可 |
| SnapshotGenerator / AutoSnapshotEngine / SnapshotBackfill | 快照复制、补下载、自动生成 | 父作业与资源依赖；保留画质、配额、manifest；不能误用低清快照充原图 |
| ApkDownloadManager | 更新下载与分片 | 作业和每个分片受统一授权，保留校验 |
| ModelDownloadManager 及各模型入口 | 模型文件获取 | 同一任务服务，保留模型验证和安装 |
| DownloadNovelTask / 系列任务 | 小说离线导出 | 导出入队；日常阅读和预读例外 |
| DownloadsRegistry / StorageBackend / StageStore | 命名、覆盖、SAF/MediaStore、断点 | 尽量复用；最终提交需与任务完成状态核对 |

## 本阶段限制

单页手动恢复现在遵守页面调度容量，但尚未建立覆盖动图、快照、模型等的全局原子许可。
原有异步持久化窗口、共享缓存探测和进程退出诊断仍需后续阶段处理。
不能把本阶段修正称为全部下载统一，亦不能据此宣称已修复日志中的进程退出。

## 第二阶段：持久化核心

ContentQueueStore 使用独立 content_queue.db，避免在尚未接管入口时改动现有业务数据库。任务事务提交后才返回 ID；领取名额与写入 RUNNING/attempt 在同一事务内。NETWORK/COPY/ENCODE 分别控制容量，相同资源不能被两个执行器同时领取。

暂停/取消执行中的任务先记录请求，名额保持到执行器确认停止；完成回调必须匹配当前 attempt。重启恢复保留暂停与取消意图，撤销旧执行标识。数据库打开本身不会重置任务，恢复仅允许由进程唯一所有者在启动 worker 前调用。

本阶段核心尚未接入旧 Manager 或特殊下载入口，不能称为全应用调度已统一。接管阶段必须实现：唯一进程所有者、后台调用、既有记录幂等导入、按任务类型派发、输出提交核对、缓存复查、浏览优先级/公平调度及旧执行路径停用。同一输出已写入但完成状态未提交的场景，由执行器在重放前核对文件；不能依靠数据库事务宣称文件系统与数据库严格只执行一次。

## 接管前执行器修正

审计发现旧 DownloadTask.cancel() 在 Body 尚未退出时立即运行 onFinally，导致 Manager 提前释放 stage 锁/句柄；暂停状态也使 activeCount 少算仍在写入的任务。本次将运行中取消的终结回调延迟到 Body 与输出提交返回，Manager 在清理结束前保留句柄与名额，准备阶段同样保持占位，快速恢复只登记资格。增加取消延迟退出、提交中取消、执行器拒绝、暂停后满额继续及准备阶段重入测试。

此修正仍使用旧图片执行路径，属于迁移前兼容加固；不是新任务核心已经接管入口的声明。正式入口接管与幂等旧记录导入仍待后续提交。
