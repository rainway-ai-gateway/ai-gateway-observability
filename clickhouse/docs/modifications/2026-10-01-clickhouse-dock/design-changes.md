# ClickHouse 对接（报表存储新引擎）设计变更

- **日期**：2026-10-01
- **变更范围**：ai-gateway-observability 新增 `clickhouse/` 资产目录，本次变更全部交付：目录骨架、本变更记录、六份 SQL 资产（§3 #2–#7）、部署/清理脚本与配置（#8）、demo 样例（#9）、HOWTO 与 TABLE_DESIGN 文档（#10）、clickhouse-it 集成测试（#11，TC01–TC05 在真实 ClickHouse 26.10 + Kafka 4.3 环境三连跑通过）
- **变更原因**：v0.8 数据统计在 Doris 之外新增 ClickHouse / StarRocks 两个可选报表存储引擎（客户现场数仓存量以二者为主，Doris 单选项构成标案准入障碍）。ClickHouse 无 Routine Load / CREATE JOB，导入与聚合需用 Kafka 表引擎 + 物化视图的库内 SQL 机制重新落地，与 Doris 侧资产平行
- **设计依据**：《数据报表-ClickHouse 与 StarRocks 对接设计方案（v0.8）》`迭代系统设计/v0.8/starrocks和clickhouse对接/数据报表-ClickHouse和StarRocks对接设计方案.md`（本变更实现其 §5 ClickHouse 侧；StarRocks 侧与本仓其它改动另行落地）

---

## 1. 背景

既有 Doris 链路（本仓 `doris/`）：Kafka（topic `bfe_ai_log`）→ Routine Load（COLUMNS 内完成 JSON 打平 + UTC 墙钟换算）→ 明细表 `bfe_ai_request_log`（UNIQUE KEY，动态分区 7 天）→ `CREATE JOB` 每分钟聚合 → `bfe_ai_metrics_1m`（AGGREGATE KEY，40 维 + 24 指标）。

ClickHouse 26.10 不具备上述两项库内调度机制，但提供等价的声明式手段：

| Doris 机制 | ClickHouse 等价物 |
|------------|-------------------|
| Routine Load（Kafka 消费 + COLUMNS 映射） | `ENGINE = Kafka` 暂存表 + 消费物化视图（解析 / 打平 / UTC 换算在 MV 的 SELECT 内完成） |
| `CREATE JOB ... ON SCHEDULE EVERY 1 MINUTE`（分钟聚合 INSERT SELECT） | 明细表上的物化视图 `TO bfe_ai_metrics_1m`（随插入同步触发，`toStartOfMinute` 天然对齐分钟桶） |
| 动态分区 `start=-7/end=3`（保留期） | 两表 `TTL ... + INTERVAL 7 DAY` |

字段契约不变：`api/depends_api/req_log.md`（Kafka JSON 80 字段）继续是唯一权威，两表与 Doris 同名同列（表契约见设计稿 §3.2 类型映射表）。

## 2. 目标与非目标

**目标**：

1. 新增 `clickhouse/` 目录，结构平移 `doris/` 既定模式（`sqls/` + `demo/` + `docs/{user,design,modifications}/` + `tests/integration/` + setup/cleanup 脚本）；
2. 六份 SQL 资产按 §4 落地：建库、明细表、Kafka 引擎表、消费 MV、聚合表、聚合 MV；
3. 导入与聚合全链路为库内 SQL 机制，**不引入常驻消费服务**，保持本仓"声明式资产"定位；
4. 口径与 Doris 逐条对齐：UTC 墙钟、apikeytags 十列打平、限流三列打平、聚合 40 维 + 24 指标、错误/限流/认证拒绝计数语义、7 天保留期。

**非目标**：

- 不改 `doris/`、`grafana/` 任何既有资产（Grafana 维持 Doris 形态，本期不交付 ClickHouse dashboard）；
- 不改 log-reader / BFE / ai-gateway-api（查询层 `storage/clickhousereport` 属 api 仓，另行落地）；
- 不做历史数据迁移：新消费组（`bfe_ai_log_clickhouse`）从分区起始加载既有 Kafka 消息，更早历史不回灌；
- 不做 ClickHouse 生产运维手册（HOWTO 给最小验证路径，生产调优建议见设计稿 §9.5）。

## 3. 变更总览

| # | 变更 | 涉及文件 | 方式 |
|---|------|----------|------|
| 1 | 目录骨架 | `clickhouse/{sqls,demo,docs/user,docs/design,docs/modifications,tests/integration}/` | 本次交付 |
| 2 | 建库 | `sqls/bfe_observability.sql` | `CREATE DATABASE`，sed 变量替换库名（同 doris 版） |
| 3 | 明细表 | `sqls/bfe_ai_request_log.sql` | MergeTree，按天分区，TTL 7 天 |
| 4 | Kafka 引擎暂存表 | `sqls/bfe_ai_log_kafka.sql` | `ENGINE = Kafka`，`JSONEachRow` + `input_format_skip_unknown_fields = 1` |
| 5 | 消费 MV（导入打平） | `sqls/bfe_ai_log_load_mv.sql` | `TO bfe_ai_request_log`，JSONExtract 打平 + `toDateTime(ts,'UTC')` |
| 6 | 聚合表 | `sqls/bfe_ai_metrics_1m.sql` | SummingMergeTree，40 维 + 24 指标逐列同名 |
| 7 | 聚合 MV（分钟聚合） | `sqls/bfe_ai_metrics_1m_mv.sql` | `TO bfe_ai_metrics_1m`，`toStartOfMinute` + 全维 GROUP BY |
| 8 | 部署/清理脚本与配置 | `setup.sh` / `cleanup.sh` / `setup.conf` / `setup_test.conf` | clickhouse-client 直连执行 SQL（sed 变量替换，同 doris 版模式） |
| 9 | 样例消息 | `demo/` | 拷贝 `doris/demo/` 三个样例（normal / rate_limit / auth_reject），目录自包含 |
| 10 | 文档 | `docs/user/HOWTO.md`、`docs/design/TABLE_DESIGN.md` | 随 SQL 资产落地时编写 |
| 11 | 集成测试 | `tests/integration/`（Go module `clickhouse-it`） | 仿 `doris/tests/integration/`，真实实例不可达自动 Skip |

## 4. 详细设计

### 4.1 导入链路（等价 Doris Routine Load 的 COLUMNS 映射）

Kafka 引擎表 schema 与 Kafka JSON 消息同构（80 字段同名同型）；`ai_apikeytags` 源 JSON 对象以 `String` 承载，MV 内再打平；嵌套数组直映射 ClickHouse 复合类型：

```
req_headers            Nested(`key` String, `value` String)
ai_route_rule_hits     Array(Tuple(rule_owner String, rule_owner_type String, rule_name String))
ai_cluster_key_names   Array(Tuple(cluster_name String, key_name String))
ai_rate_limit_hits     Array(Tuple(rate_limit_policy_id String, rate_limit_type String,
                                   rule_names Array(String)))   -- 唯一三层嵌套
ai_auth_*_quota_plans  Array(String)
```

引擎设置三要点：独立消费组 `"bfe_ai_log_clickhouse"`（与 Doris 链路位点互不影响）；`kafka_format = 'JSONEachRow'`；`input_format_skip_unknown_fields = 1`（容忍 PB 新增字段先于 DDL 到达，超时降级见 §5）。

消费 MV 中 Doris ↔ ClickHouse 口径对照：

| 口径 | Doris Routine Load | ClickHouse 消费 MV |
|------|--------------------|--------------------|
| UTC 墙钟 | `DATE_SUB(FROM_UNIXTIME(timestamp), INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND)` | `toDateTime(timestamp, 'UTC')`（列固定 UTC，无时区歧义） |
| level 打平（×10） | `json_unquote(json_extract(ai_apikeytags, '$.level1.tagname'))` | `JSONExtractString(ai_apikeytags, 'level1', 'tagname')` |
| 限流打平（×3） | `json_unquote(json_extract(ai_rate_limit_hits, '$[0].rate_limit_policy_id'))` 等 | `if(length(ai_rate_limit_hits) >= 1, ai_rate_limit_hits[1].rate_limit_policy_id, '')` 等（Tuple 具名字段访问） |
| 源字段不落表 | `ai_apikeytags` 仅参与表达式求值 | 同（MV SELECT 不投影该列） |

### 4.2 明细表

- `ENGINE = MergeTree`，`PARTITION BY toDate(log_time)`（按天，对齐 Doris）；
- `ORDER BY (hostid, log_time, ai_apikey_id, ai_requested_model)`（对齐 Doris UNIQUE KEY 四列）；
- `TTL toDateTime(log_time) + INTERVAL 7 DAY`；
- **不设 `ReplacingMergeTree`**：日志 append-only 无更新语义（StarRocks 侧同款决策，设计稿 §4.2）；
- `Nullable` 仅限 Doris 语义允许 NULL 的列（`err_msg`、`ai_intent_confidence`、`ai_intent_latency_us`、`ai_intent_cache_hit` 等），其余列 `DEFAULT ''` / `0`，保证 `col != ''` 等既有谓词行为一致；类型映射遵循设计稿 §3.2。

### 4.3 分钟聚合（等价 Doris CREATE JOB）

聚合 MV 随明细插入同步触发（秒级延迟，优于 Doris JOB 的分钟级），分钟桶对齐由 `toStartOfMinute(log_time)` 天然保证，**无需 Doris JOB 的时间窗谓词**（`WHERE log_time >= 上一分钟起点 AND < 本分钟起点`）。

- 聚合表 `ENGINE = SummingMergeTree`，`ORDER BY` 为全 40 个维度列（**必须为全维**：merge 时排序键相同的行会被折叠求和，非排序键维度只保留一行值，前缀子集排序键会丢失维度粒度——冒烟实测证实），TTL 7 天；
- 指标口径逐一对齐 Doris JOB：`request_count = count()`、`error_count = sum(err_code != '')`、`auth_reject_count = sum(ai_auth_reject_reason != '')`、`rate_limit_hits = sum(length(ai_rate_limit_hits) > 0)`（命中限流的**请求数**）、`quota_plans_slot1~5 = ifNull(ai_auth_reject_quota_plans[n], '')`、其余 SUM 指标 `sum(ifNull(col, 0))`；
- **SummingMergeTree 查询纪律**：相同完整排序键的行仅在被 merge 时求和，查询侧必须 `GROUP BY <40 维> + sum(<指标>)` 兜底（api 查询层按维度聚合天然兼容）；禁止 `SELECT *` 直读聚合表；
- 默认不挂 `POPULATE`，从上线起积累（同 Doris 口径）；历史窗口可选按明细重算近 7 天。

### 4.4 setup/cleanup 脚本约定

沿用 doris 版模式：`setup.conf` 集中变量（`CLICKHOUSE_HOST` / `CLICKHOUSE_PORT`（9000 native TCP）/ `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DATABASE` / `KAFKA_BROKER_LIST` / `KAFKA_TOPIC` / `KAFKA_GROUP_ID` / `INIT_PARTITION_DATE` 不需要——CH 无预建分区概念，去掉），`setup.sh` 用 sed 替换 `${VAR}` 后按序喂给 `clickhouse-client`（`--multiquery`），执行顺序：建库 → 明细表 → Kafka 表 → 消费 MV → 聚合表 → 聚合 MV；`cleanup.sh` 逆序 DROP（MV 先于其目标表删除）。

## 5. 影响与兼容

| 项 | 影响 |
|----|------|
| Doris 链路 | 零影响：同一 Kafka topic 三个消费组（doris / starrocks / clickhouse）位点独立，互不干扰 |
| 回滚 | 删除 `clickhouse/` 资产并 DROP 库即可；重部署时新消费组从分区起始重新加载 |
| 数据准确性 | ClickHouse Kafka 引擎 + MV 为 at-least-once：位点提交前失败会重投，重复行被 SummingMergeTree 重复累计（Doris Routine Load 事务性更强）。日志统计场景惯例接受小概率重复，运维上监控消费滞后与 `system.parts` 异常增长，可用 §4.3 重算语句兜底 |
| PB 新增字段 | `input_format_skip_unknown_fields = 1` 默认容忍；若 Kafka 引擎表缺列导致消费停摆，按 doris-it 同款"升级 ALTER"模式补列 |
| 演进约束 | 明细表加列后聚合 MV 需重建才能纳入新维度（与 Doris AGGREGATE KEY 重建心智一致） |

## 6. 验证计划

1. **资产冒烟**：测试环境（`environment/clickhouse-installation.md`，WSL2 单文件实例，HTTP 8123 / Native 9000）执行 setup.sh，六份 SQL 全部成功；
2. **集成测试 `clickhouse-it`**：仿 `doris-it` 加载真实 SQL 资产；`demo/` 三个样例消息落库后断言——level 打平十列、限流打平三列、`log_time` UTC 墙钟（与样例 `timestamp` epoch 换算一致）、分钟聚合 40 维 + 24 指标与 doris-it 既有断言逐项一致；实例不可达自动 Skip；
3. **与 Doris 比对**：同一份样例集分别经 Doris 链路与 ClickHouse 链路落库，按分钟窗口比对 `bfe_ai_metrics_1m` 全指标误差为 0；
4. **回归**：doris-it 与 grafana 资产不受影响（本变更纯新增目录）。
