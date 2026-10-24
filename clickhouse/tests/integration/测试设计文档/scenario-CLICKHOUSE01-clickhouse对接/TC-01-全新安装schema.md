# TC-01 全新安装 schema

## 用例编号与名称

TC-01 全新安装 schema

## 所属场景

CLICKHOUSE01 ClickHouse 对接（报表存储新引擎）

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 ClickHouse 对接）
- `ClickHouse`：本机 WSL2 单文件 26.10.1.1149（`environment/clickhouse-installation.md`）

## 测试目的

验证全新安装路径下，仓库 SQL 资产按 setup.sh 六步顺序建出的对象与设计稿一致：明细表（MergeTree）、Kafka 引擎暂存表（Kafka）、消费 MV（MaterializedView）、聚合表（SummingMergeTree）、聚合 MV（MaterializedView）共 5 个对象；且全部 `CREATE ... IF NOT EXISTS`，重复应用幂等（setup.sh 可重跑）。

## 运行模式

单组件模式：直连真实 ClickHouse（`127.0.0.1:9000`，clickhouse-go stdlib），在独立测试库中执行仓库 SQL 文件（仅做 `${VAR}` 变量替换）。ClickHouse 不可达时自动 `SKIP`。

## 前置条件

1. ClickHouse 已启动（`wsl bash /mnt/d/clickhouse/bin/start.sh`），default 无密码可连。
2. Go 1.23+ 可用，依赖 `github.com/ClickHouse/clickhouse-go/v2`。

## 测试数据构造

- 独立测试库：`<CLICKHOUSE_DATABASE>_it_<用例名>_<纳秒后缀>`（默认基名 `bfe_observability_test`）。
- 执行仓库文件（setup.sh 六步顺序）：`bfe_observability.sql`（建库）→ `bfe_ai_request_log.sql`（明细表）→ `bfe_ai_log_kafka.sql`（Kafka 表）→ `bfe_ai_log_load_mv.sql`（消费 MV）→ `bfe_ai_metrics_1m.sql`（聚合表）→ `bfe_ai_metrics_1m_mv.sql`（聚合 MV）。
- Kafka 变量替换为测试默认值（`127.0.0.1:9092` / `bfe_ai_log_it`），仅建对象不消费，无 Kafka 环境依赖。

## 执行步骤

1. 连接 ClickHouse（DSN `clickhouse://default@127.0.0.1:9000/default?dial_timeout=10s`），创建独立测试库。
2. 按序执行六份仓库 SQL 文件（单语句全限定，无需 USE）。
3. 查询 `system.tables`（ClickHouse 中表与 MV 同列）：断言 5 个对象存在，且引擎逐一对齐——`bfe_ai_request_log`=MergeTree、`bfe_ai_log_kafka`=Kafka、`bfe_ai_log_load_mv`=MaterializedView、`bfe_ai_metrics_1m`=SummingMergeTree、`bfe_ai_metrics_1m_mv`=MaterializedView；对象总数恰为 5。
4. 再次全部六份 SQL 重复应用一遍（幂等验证，等价 setup.sh 重跑）。
5. 重复应用后复查对象集合不变。

## 预期结果

- 步骤 3：5 个对象全部存在，引擎与期望逐项相等，对象总数 = 5。
- 步骤 4–5：重复应用全部成功（`IF NOT EXISTS` 不报错），对象集合无变化。

## 清理

`t.Cleanup`：`DROP DATABASE <测试库>`（级联删除库内表与 MV）；`CLICKHOUSE_IT_KEEP_DB=1` 时保留现场。
