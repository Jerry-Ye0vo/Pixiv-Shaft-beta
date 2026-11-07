# 下载记录兼容性约束与验收

本阶段保留现有 Room v45 schema、表名、主键和索引。独立 ContentQueueStore 尚未接管执行；先给实际使用中的 Manager 增加持久化入队门槛，并修正后续迁移会依赖的旧记录操作。不得将本阶段描述为全部下载入口统一完成。

## 存储边界

| 数据 | 现有存储与读取方式 | 本阶段处理 |
| --- | --- | --- |
| 未完成页面 | illust_downloading_table；fileName 主键、uuid、taskGson | 保留布局；后台按序入队/清除，成功写库后才允许派发；删除和状态更新同时匹配 fileName + uuid |
| 批量作品分组 | download_queue；seq、status、illustId、illustGson | 保留分组、重试、分页和汇总；既有 consumer 暂未被新核心替换 |
| 已完成文件 | illust_download_table；原字段及 illustId/page/downloadTime 索引 | 不迁移、不改名、不清空；继续由 insertDownload 写入 |
| 已下载徽标 | DownloadStateProbe 的索引查询及未回填时的旧 JSON 查询 | 接口保持；对未回填旧行保留原查询路径 |
| 本地页复用与查重 | DownloadedPageIndex、RecordedPageProbe；id/page 及文件名查询 | 接口保持；记录存在不等于文件可用，仍验证 URI；新记录失效时继续查找同页旧文件 |
| 已完成列表、搜索、计数 | DownloadDao.getAll/flowAll/searchDownloads/countDoneCardsInWindow | 查询和 Room InvalidationTracker 保持 |
| 扫描导入 | insertIgnoreAll | 保留 IGNORE，不用导入结果覆盖完整历史记录 |
| 浏览历史、画像资料源 | illust_table 与已有下载记录查询 | 本阶段不修改这些表；队列清空不能清除历史/已完成记录 |
| 旧单页保存日志 | image-page-saves.properties → 旧页面队列表 | 整批事务成功后才确认；同名不同资源或无法辨识的目标行保留日志并报错 |

## 入队与恢复

内存中先保留待写入项，供批量生产者计算待处理数量；这不授予下载资格。pendingPersistence 阻止普通派发、手动继续和重试绕过数据库提交。持久化失败时显示失败并保留重试入口，不能边报错边执行。已接入回调的单页、整部及快捷选页提示在提交后才显示。

新 taskGson 增加 queueRecordVersion=1，仍是 DownloadItem JSON。Gson 的旧读取端可忽略额外字段；未带版本标记的旧记录沿用原有 INIT/未暂停恢复规则。新记录保留已提交的暂停和失败意图，运行态只在进程内有效。状态更新仅 UPDATE 已存在的相同任务，不能把已删除行插回。

恢复时以数据库列的文件名和 uuid 为准；老行缺少 uuid 时生成稳定身份并用原行内容作条件修复，避免覆盖恢复期间的新写入。无法解析的行保留在数据库，不静默删除。大批旧记录仍以 keyset 分页读取，保持顺序与作品元数据共享。

成功执行发生在入队提交之后，完成记录仍按原路径写入已完成表；未完成记录在 worker IO 上按身份及时删除，不排在大批新入队写入之后。用户清除命令则与入队写入按序执行，防止迟到的插入让已清除任务复活。

## 已知边界

- 旧未完成表只有文件名主键，无法同时表示同名的不同资源。本阶段明确报冲突并保留旧记录，未来切换稳定任务 ID 的新表后解除此限制。
- 当前记录查重、不同画质的缓存核验和文件提交幂等性仍需后续统一资源层完善；保留既有 API 不意味着这些历史局限已经解决。
- 独立队列的旧数据导入、唯一所有者切换、原图/动图/快照等执行器接入仍待实施；本阶段不让两套消费者同时执行同一批任务。
- 自动化验证覆盖 Room 查询和状态/并发边界；真机 SAF、撤销权限、长时间大队列和进程被系统终止仍需后续实测。

## 回归入口

新增 ManagerAdmissionTest、PageQueueCompatibilityTest，扩展 LegacyPageSaveMigrationTest，并运行已有 ManagerRestoreTest、DownloadRecordStateSourceTest、DownloadedPageIndexTest、DownloadQueueDaoTest、DownloadDaoDoneCardCountTest、DownloadDaoDeleteAllTest，以及前两阶段的并发与执行器测试。测试链接和实际结果记录在 PR 描述，避免把未运行的场景写成已通过。
