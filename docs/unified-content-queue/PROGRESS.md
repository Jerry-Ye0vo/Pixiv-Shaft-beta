# 统一内容队列迁移进度

基线已发布：[4.9.9-beta.14](https://github.com/Jerry-Ye0vo/Pixiv-Shaft-beta/releases/tag/v4.9.9-beta.14)，源码 79eea653，原 APK 和固定签名。
开发分支：feat/unified-content-queue。这里的阶段状态不表示全量迁移已完成。

| 子流程 | 当前状态 |
| --- | --- |
| 0 原版本留档 | 已完成，校验原 artifact ZIP、APK checksum 及构建身份后发布 |
| 1 整理入口与分类 | 清单如下；按每类的真实网络/文件入口继续审计 |
| 2 单项恢复调度规则 | 本提交将直接派发改为待执行资格；新增满额连续恢复测试 |
| 3 持久化任务核心、原子资源许可与旧记录迁移 | 待实施 |
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
