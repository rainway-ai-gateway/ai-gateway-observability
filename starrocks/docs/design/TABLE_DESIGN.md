# BFE AI Gateway 可观测性 — StarRocks 表设计说明

本文档描述 `bfe_observability` 数据库中明细表 `bfe_ai_request_log` 与分钟聚合物化视图 `bfe_ai_metrics_1m` 的结构与语义。事实来源为 `starrocks/sqls/` 下四份 SQL 资产，设计依据见修改说明 [2026-10-02-starrocks-dock](../modifications/2026-10-02-starrocks-dock/design-changes.md) 与《数据报表-ClickHouse 与 StarRocks 对接设计方案（v0.8）》。

> 数据库名可通过 `STARROCKS_DATABASE` 参数化（生产 `bfe_observability`，测试 `bfe_observability_test`）。下文中涉及数据库名的查询示例统一用 `bfe_observability`。

---

## 1. 总体设计

| 对象 | 类型 | 粒度 | 用途 |
|------|------|------|------|
| `bfe_ai_request_log` | 明细表（DUPLICATE KEY） | 单次请求 | 原始请求日志，用于明细检索、问题排查 |
| `bfe_ai_log_load` | Routine Load | — | Kafka → 明细表的导入通道（COLUMNS 内完成 JSON 打平与 UTC 墙钟换算） |
| `bfe_ai_metrics_1m` | 异步物化视图（聚合） | 1 分钟 | 分钟级聚合，用于报表、趋势、告警 |

- 明细表由 **Routine Load** 实时写入，语法与 Doris 版同构（`starrocks/sqls/bfe_ai_log_load_routine.sql`）。
- 分钟聚合由 **异步物化视图** 维护（`REFRESH ASYNC EVERY 1 MINUTE`），MV 名直接取 `bfe_ai_metrics_1m`，api 查询契约与 Doris 侧完全一致（api 无需感知 MV 存在）。
- StarRocks 本期**不交付 Grafana 数据源与 dashboard**；报表可视化统一走 ai-gateway-web 报表页与 `/open-api/v1/report/*` API。趋势/告警类查询推荐优先使用 `bfe_ai_metrics_1m`（数据量小、查询快）；需要单条明细时再用 `bfe_ai_request_log`。

### 1.1 与 Doris 机制对照

| Doris 机制 | StarRocks 等价物 | 说明 |
|------------|------------------|------|
| `UNIQUE KEY` 明细表 | `DUPLICATE KEY` 明细表 | 排序键四列一致 `(hostid, log_time, ai_apikey_id, ai_requested_model)`；日志 append-only、`logid` 天然唯一，去重语义无实际作用，DUPLICATE KEY 写入最快（建模决策见修改说明 §4.2） |
| `AGGREGATE KEY` 聚合表 + `CREATE JOB` 每分钟 INSERT SELECT | 异步物化视图 `CREATE MATERIALIZED VIEW ... REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)` | MV 即一张可直查的表，名字即 `bfe_ai_metrics_1m`；基表增量刷新（分区粒度按天）+ `date_trunc('minute')` 分钟桶，语义等价 Doris JOB 的滑窗，端到端延迟 1~2 分钟 |
| Routine Load COLUMNS 映射 | Routine Load COLUMNS 映射（两处方言改写） | level/限流打平用 `get_json_string`（§2.3）；UTC 墙钟算术式原样适用 |
| 动态分区 `start=-7`（保留期） | 明细表动态分区 `start=-7` + MV 分区列 `ts_day` + `partition_ttl=7 DAY` | `partition_ttl` 在 3.5.21 实测通过；不支持的老版本由 cleanup 脚本兜底 |

### 1.2 设计原则（与 Doris 逐条对齐的口径）

1. 明细表与 Doris 同名同列（104 列，含 level×10、rate_limit×3 打平列、缓存/镜像/意图 10 列与缺价信号 2 列）；其中 5 个嵌套列（`req_headers`/`res_headers`/`ai_route_rule_hits`/`ai_cluster_key_names`/`ai_rate_limit_hits`）在 SR 侧以 VARCHAR 承载 JSON 文本（类型偏差及原因见 §2.2），列名契约不变；字段语义全量沿用 `doris/docs/design/TABLE_DESIGN.md`，本文档不重复枚举。
2. 聚合 40 维 + 24 指标，逐列同名同口径（见 §3）。
3. UTC 墙钟：`log_time` 存 UTC，由 Routine Load COLUMNS 内的可移植算术式换算（不依赖 FE 会话时区）。
4. 7 天保留：明细表动态分区 + MV `partition_ttl`。
5. **布尔字段一律 `BOOLEAN`**（`ai_stream`/`is_trust_src_ip`/`mirror_hit`/`ai_intent_cache_hit`/`ai_price_miss`）：SR 的 JSON 导入只在 `BOOLEAN` 目标列上把 JSON `true/false` 转成 `1/0`，`TINYINT` 会静默落 NULL；`TINYINT` 只用于真正的整数语义（`res_status_code` 等）。跨引擎按"语义"对齐（Doris `TINYINT` / CH `UInt8` / SR `BOOLEAN`），不按字面类型对齐（§2.2、修改说明 2026-10-22）。`BOOLEAN` 同时吸收 JSON `1/0`，上游改写口径也不需要再改本侧 DDL。

---

## 2. 明细表 `bfe_ai_request_log`

### 2.1 表属性

| 属性 | 值 |
|------|----|
| 模型 | `DUPLICATE KEY`（对齐 Doris UNIQUE KEY 四列排序键，不约束唯一性） |
| 排序键 | `(hostid, log_time, ai_apikey_id, ai_requested_model)` |
| 分区 | `PARTITION BY RANGE(log_time)`，动态分区按天（`start=-7` 保留 7 天，`end=3` 预建 3 天） |
| 分桶 | `DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32` |
| 压缩 | zstd |
| 副本 | `replication_num=1`（单 BE 测试环境；生产按集群规模调整） |

> **DUPLICATE KEY ≠ 唯一键**：排序键只做索引与数据局部性，不拒绝重复行。日志 append-only 无更新语义，此模型与 Doris UNIQUE KEY 在该场景下查询行为等价，写入代价更低。

### 2.2 字段说明

列契约与 Doris 版同名同列（104 列）；其中 5 个嵌套列**按 StarRocks 能力以 VARCHAR 承载 JSON 文本**（类型偏差，见下表），其余列同型。全量字段语义（分组说明、取值口径、与 Kafka JSON 字段的对应关系）见 `doris/docs/design/TABLE_DESIGN.md` §2，此处仅列出 StarRocks 侧需要注意的点：

| 字段 | 类型 | StarRocks 侧说明 |
|------|------|------------------|
| `req_headers` / `res_headers` | VARCHAR(65533) | **JSON 文本存储**（Doris 侧为 `ARRAY<STRUCT<...>>`）。原因：SR 3.5 的 Routine Load 不支持 VARCHAR 直转 ARRAY<STRUCT>（任务提交报 Not support cast），且派生列不允许自引用（`col = CAST(parse_json(col)...)` 报 can't be found in column list）。列名契约不变；需结构化访问时查询侧 `CAST(parse_json(col) AS ARRAY<STRUCT<...>>)` 还原（MV 的 rate_limit_hits 即此用法） |
| `ai_route_rule_hits` / `ai_cluster_key_names` / `ai_rate_limit_hits` | VARCHAR(4096) | 同上前三个 AI 嵌套列；`ai_rate_limit_hits` 的打平列由 Routine Load 按 JSON 路径取首个命中 |
| `ai_auth_reject_quota_plans` / `ai_auth_hit_quota_plans` | `ARRAY<VARCHAR(128)>` | **保持数组类型**（标量数组的 VARCHAR 直转 SR 原生支持，Routine Load 裸映射即可） |
| `logid` | BIGINT | BFE 请求唯一标识；**SR 对超 Int64 域的值直接拒绝**（INSERT 报 Number out of range；Routine Load 计错误行跳过）——与 Doris 静默截断、CH UInt64 原值保留为三引擎差异点（上游实际 logid 在 Int64 范围内） |
| `level1Name~level5` | VARCHAR(128) ×10 | 由 Routine Load 从 `ai_apikeytags` JSON 打平（`get_json_string`），源字段不落表；未打平的层级为 NULL |
| `log_time` | DATETIME | **UTC 墙钟**，由 COLUMNS 算术式写入；时间过滤一律用此字段 |
| `ai_stream` / `is_trust_src_ip` / `mirror_hit` / `ai_intent_cache_hit` / `ai_price_miss` | **BOOLEAN** | payload 里是 **JSON `true`/`false`**，落库 `0/1`（`NULL` = 未上报）。**必须声明 `BOOLEAN`**：SR 的 JSON 导入只在 `BOOLEAN` 目标列上做布尔→`1/0` 转换，声明成 `TINYINT` 会**静默写 NULL**（`NumberFilteredRows=0`、RL 不报错，只能靠列级非空统计发现）——Doris/CH 分别用 `TINYINT`/`UInt8` 也能正确吸收，属引擎差异，见 [2026-10-22-issue-10-boolean-columns](../modifications/2026-10-22-issue-10-boolean-columns/design-changes.md) |

### 2.3 导入通道：Routine Load `bfe_ai_log_load`

与 Doris 版差异点（starrocks-it TC02 真实消费链路逐条核对）：

| 项 | Doris 版 | StarRocks 版 |
|----|----------|--------------|
| level 打平列表达式 | `json_unquote(json_extract(ai_apikeytags, '$.level1.tagname'))` | `get_json_string(ai_apikeytags, '$.level1.tagname')`——**SR 3.5 无 `json_extract(varchar, varchar)` 签名**（任务运行时报 no matching function）；get_json_string 直接返回去引号字符串 |
| 限流打平列表达式 | `json_unquote(json_extract(ai_rate_limit_hits, '$[0].rate_limit_policy_id'))` 等 | `get_json_string(ai_rate_limit_hits, '$[0].rate_limit_policy_id')` 等——COLUMNS 表达式上下文中复杂列以 VARCHAR（JSON 文本）承载，按 JSON 路径取首个命中（数组下标形式会报 cannot subscript VARCHAR） |
| UTC 墙钟 | `DATE_SUB(FROM_UNIXTIME(timestamp), INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND)` | 原样平移（已实测：FE 会话时区 Asia/Shanghai 下结果即 UTC 墙钟） |
| 消费组 | `doris_bfe_ai_log` | `starrocks_bfe_ai_log`（**必须不同**，位点互不影响） |
| 起始偏移 | `OFFSET_BEGINNING` | 同（历史消息全部加载） |
| 错误容忍 | `max_error_number=1000` | 同（SR Routine Load 同名参数） |

> **教训**：`CREATE ROUTINE LOAD` 只校验语法，列表达式在 BE 任务创建/运行时才分析——表达式兼容性必须以真实消费链路（starrocks-it TC02）验证，不能只看任务创建成功。

---

## 3. 分钟聚合物化视图 `bfe_ai_metrics_1m`

### 3.1 定义

```sql
CREATE MATERIALIZED VIEW bfe_ai_metrics_1m
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
PARTITION BY ts_day
DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 16
PROPERTIES ("replication_num" = "1", "partition_ttl" = "7 DAY")
AS
SELECT date_trunc('day', log_time) AS ts_day, date_trunc('minute', log_time) AS ts_min,
       <40 维>, <24 指标>
FROM bfe_ai_request_log
GROUP BY ts_day, ts_min, <40 维>;
```

- **MV 名即表契约名**：api 查询层按普通表查询，无需感知 MV 存在（对齐 Doris 侧 `bfe_ai_metrics_1m` 表契约）。
- **无时间窗谓词**：Doris JOB 用 `WHERE log_time >= 上一分钟起点 AND < 本分钟起点` 滑窗；MV 为基表增量刷新（分区粒度按天），分钟桶由 `date_trunc('minute')` 天然对齐，语义等价且无需调度器。
- **延迟口径**：刷新间隔 1 分钟 + 分区对齐，端到端 1~2 分钟，与 Doris JOB 同量级。
- **分区列 `ts_day`（首列，按天粒度）**：SR 3.5 要求 `PARTITION BY` 引用 SELECT 输出列且可溯源基表分区列，`ts_min`（minute 桶）无法通过校验，故显式暴露按天的 `ts_day` 作为分区列（比 `ts_min` 粗，不改变聚合行数）。代价是 MV 比 Doris 聚合表多一列；api 查询用显式列名不受影响，仅 `SELECT *` 多出一列。设计稿原定的 `PARTITION BY date_trunc('day', ts_min)` 在 3.5.21 实测被拒绝（见修改说明 §4.4）。

### 3.2 维度与指标口径

40 维 + 24 指标与 Doris 版逐列同名同口径，聚合表达式差异仅一处：

| 口径 | Doris JOB | StarRocks MV |
|------|-----------|--------------|
| 配额计划槽位打平 | `COALESCE(ELEMENT_AT(ai_auth_reject_quota_plans, n), '')` | `COALESCE(ai_auth_reject_quota_plans[n], '')`（SR 数组下标从 1 开始，越界返回 NULL） |
| 限流命中计数 | `ARRAY_SIZE(ai_rate_limit_hits) > 0` | `array_length(CAST(parse_json(ai_rate_limit_hits) AS ARRAY<STRUCT<...>>)) > 0`——SR 无 `array_size`（等价函数 `array_length`）；且 `ai_rate_limit_hits` 在 SR 侧为 VARCHAR（JSON 文本，见 §2.2），MV 内 parse_json 还原后取长度（3.5.21 实测口径等价） |
| 错误/认证拒绝计数 | `SUM(CASE WHEN col != '' AND col IS NOT NULL THEN 1 ELSE 0 END)` | 同 |
| 分钟桶 | `DATE_TRUNC(log_time, 'minute')` | `date_trunc('minute', log_time)` |
| NULL 兜底 | `COALESCE(维度, ''/0)` | 同 |

维度清单（40）：`ts_min, hostid, ai_apikey_id, ai_requested_model, ai_target_model, ai_stream, product, cluster, sub_cluster, backend_info, method, res_status_code, err_code, header_host, ai_provider, ai_protocol, ai_mode, ai_cost_currency, level1Name, level1, ..., level5Name, level5, rate_limit_policy_id, rate_limit_type, rate_limit_rule_name, ai_auth_reject_reason, ai_auth_reject_quota_plans_slot1~5, ai_cache_status, mirror_hit, ai_intent_answer`。

指标清单（24）：`request_count, error_count, auth_reject_count, input_tokens, output_tokens, total_tokens, ttft_us_sum, tpot_us_sum, req_header_bytes, req_body_bytes, res_header_bytes, res_body_bytes, rate_limit_hits, backend_retries, all_time_sum, cluster_serve_sum, backend_serve_sum, ai_retry_count_sum, ai_cost_value_sum, cache_read_tokens, cache_write_tokens, ai_audio_input_tokens, ai_audio_output_tokens, ai_image_count`。

### 3.3 演进约束

基表 `bfe_ai_request_log` 加列后，本 MV **不会自动携带**新列，需重建 MV 才能进聚合口径（`DROP MATERIALIZED VIEW` 后重新执行 `sqls/bfe_ai_metrics_1m.sql`）——与 Doris 侧"重建聚合表"的既有约束对齐，不新增心智负担。

### 3.4 保留期

- 明细表：动态分区 `start=-7`，过期分区自动删除。
- 聚合 MV：`partition_ttl=7 DAY`（版本确认项：StarRocks 3.5.21 实测通过；若部署版本不支持，由 cleanup 脚本或定时任务 `ALTER MATERIALIZED VIEW ... DROP PARTITION` 兜底）。

---

## 4. 变更历史

| 日期 | 变更 | 记录 |
|------|------|------|
| 2026-10-02 | 新增 StarRocks 对接：明细表（DUPLICATE KEY）、Routine Load、异步 MV `bfe_ai_metrics_1m` | [2026-10-02-starrocks-dock](../modifications/2026-10-02-starrocks-dock/design-changes.md) |
| 2026-10-22 | 明细表 `ai_stream` / `is_trust_src_ip` 由 `TINYINT` 改为 `BOOLEAN`（修复 JSON 布尔静默落 NULL，issue #10）；布尔字段类型映射规则固化 | [2026-10-22-issue-10-boolean-columns](../modifications/2026-10-22-issue-10-boolean-columns/design-changes.md) |
