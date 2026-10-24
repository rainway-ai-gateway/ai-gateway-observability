# TC-01 全新安装 schema

## 用例编号与名称

TC-01 全新安装 schema

## 所属场景

DORIS01 报表二期（v0.8）Doris 侧对齐缓存/镜像/意图字段

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 二期）
- `Doris`：本机 WSL2 单机 3.0.8（`environment/doris-installation.md`）

## 测试目的

验证全新安装路径下，仓库 SQL 资产（`sqls/`）建出的对象 schema 与设计稿一致：明细表含缓存/镜像/意图 10 列与限流打平 3 列、聚合表含 3 个新 KEY 维度（40 维）、INSERT JOB 可被 Doris 创建（CREATE JOB 时即校验 DO 语句合法性）。

## 运行模式

单组件模式：直连真实 Doris FE（`127.0.0.1:9030`），在独立测试库中执行仓库 SQL 文件（仅做 `${VAR}` 变量替换）。

## 前置条件

1. Doris FE/BE 已启动（`wsl bash /mnt/d/doris/bin/start-all.sh`），root 无密码可连。
2. Go 1.23+ 可用，依赖 `github.com/go-sql-driver/mysql v1.9.0`。

## 测试数据构造

- 独立测试库：`bfe_observability_it_<用例名>_<纳秒后缀>`（`INIT_PARTITION_DATE` 取明天，保证当刻数据落入初始分区）。
- 执行仓库文件：`bfe_observability.sql` → `bfe_ai_request_log.sql` → `bfe_ai_metrics_1m.sql` → `bfe_ai_metrics_1m_job.sql`。
- 注：INSERT JOB 名（`bfe_ai_metrics_1m_job`）Doris 全局唯一，创建前先 `DROP JOB WHERE JobName=...` 清理历史残留。

## 执行步骤

1. 连接 Doris（DSN 带 `multiStatements=true`），创建独立测试库。
2. 按序执行 4 个仓库 SQL 文件（每条会话以 `USE <库>;` 开头单连接执行）。
3. 查询 `information_schema.columns` 断言明细表 13 个新列存在且类型正确（`ai_cache_status varchar(16)`、`mirror_hit tinyint`（Doris 将 BOOLEAN 报告为 tinyint）、`ai_intent_confidence double` 等，对照测试内 `newDetailColumns`/`flattenColumns`）。
4. `SHOW CREATE TABLE bfe_ai_metrics_1m`：断言含 `ai_cache_status`/`mirror_hit`/`ai_intent_answer`；解析 `AGGREGATE KEY(...)` 列表计数 = 40（37 既有 + 3 新）。
5. 通过 `jobs("type"="insert")` 表函数查询 `Name = 'bfe_ai_metrics_1m_job'`，断言 JOB 存在且能读到 `Status`。

## 预期结果

- 明细表 13 个新列全部存在，类型与期望逐项相等。
- 聚合表 AGGREGATE KEY 维度数 = 40。
- `jobs()` 查询返回 1 行（JOB 创建成功，DO 语句通过 Doris 校验）。

## 清理

`t.Cleanup`：`DROP JOB WHERE JobName='bfe_ai_metrics_1m_job'`（JOB 名全局唯一），随后 `DROP DATABASE <测试库> FORCE`；`DORIS_IT_KEEP_DB=1` 时保留现场。
