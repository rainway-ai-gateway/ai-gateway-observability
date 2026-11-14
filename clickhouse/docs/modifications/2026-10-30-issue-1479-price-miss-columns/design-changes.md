# 明细表新增缺价信号列 `ai_price_miss` / `ai_price_warn`（bfe #1479）

- **日期**：2026-10-30
- **上游**：[bfenetworks/bfe#1479](https://github.com/bfenetworks/bfe/issues/1479)（缺价动作三态 + 形态化信号）、`bfe-access-pb v0.3.17`（field 763/766）、`ai-gateway-api 508e552`（MySQL 明细表 99→101 列）
- **状态**：**已实施**（SQL 资产 + 集成测试；实施记录见 §8）
- **结论**：ClickHouse 链路补两列——`bfe_ai_log_kafka`（暂存表）与 `bfe_ai_request_log`（明细表）各加 `ai_price_miss Nullable(UInt8)` / `ai_price_warn Nullable(String)`，`bfe_ai_log_load_mv`（消费 MV）补两条投影。**聚合表 `bfe_ai_metrics_1m` 不加维度**（§5）。

---

## 1. 背景

### 1.1 缺价信号为什么需要入仓

模型缺少某接口定价时，BFE 的既有行为是"回退按该模型 chat 价计费"，v0.7.0.1 测试暴露的问题是：**该接口的请求不扣费且没有任何可核对痕迹**。bfe #1408 先落了 `ai_price_miss` 标记，#1479 补齐三态动作（`fallback_chat` / `bill_zero` / `reject`）与形态化信号 `ai_price_warn`。

对报表链路的要求是：缺价行必须能在明细里被筛出来对账，且**不能把"未发生缺价"读成"发生了但形态未知"**。因此两列的 NULL 语义是硬约束：

| 取值 | 含义 |
|------|------|
| `ai_price_warn` = `fallback` | 非 chat 接口按该模型 chat 价计（少收风险） |
| `ai_price_warn` = `subitem_missing` | 计价条目缺子项价键，缺失子项按 0（少收） |
| `ai_price_warn` = `no_entry` / `no_baseline` | (模型, 接口) 条目缺失 / 且无 chat 价可回退 |
| `ai_price_warn` = `no_model_table` | 集群 `ModelTable` 整体缺失 |
| `ai_price_warn` = `reject` | 接口未定价且 `PriceMissAction=reject`，请求被拒（`res_status_code=503`，请求未转发但**行仍写出**） |
| `ai_price_miss` = `1` / `0` | 本请求发生过 / 显式未发生缺价回退 |
| 两列 **NULL** | 未上报（旧版 bfe / 字段缺省）——**≠ 未发生** |

### 1.2 现状缺口

截至本改动，ClickHouse 三份 SQL 均无这两列：`mod_kafka` 已把它们放进默认输出（log-reader `2ef0777`），但 `bfe_ai_log_kafka` 的 DDL 没有对应列，`input_format_skip_unknown_fields=1` 会把它们**静默跳过**——不报错、不落库，报表侧永远查不到缺价行。

## 2. 改动点

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_log_kafka.sql` | 暂存表末尾追加 `ai_price_miss Nullable(UInt8)`、`ai_price_warn Nullable(String)`（必须 `Nullable`，见 §3.1） |
| `sqls/bfe_ai_request_log.sql` | 明细表末尾追加同两列 + `COMMENT`；`Nullable` 承载 NULL 语义 |
| `sqls/bfe_ai_log_load_mv.sql` | SELECT 列表末尾追加两个裸列名（直接投影，无需 `JSONExtract*`：payload 里是 JSON 标量而非嵌套对象） |
| `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/bfe_ai_request_log_alter.sql` | 存量升级：**只含明细表** `ADD COLUMN IF NOT EXISTS`（单语句、幂等）；Kafka 暂存表不能 ALTER，需 DROP + 按主 DDL 重建（脚本头部注释给出完整四步顺序） |
| `tests/integration/`（clickhouse-it） | TC01 补列类型断言；TC02 补消费链路取值/NULL 断言；新增 TC06 覆盖升级脚本与 MV 投影 |

聚合表与聚合 MV **不改**。

## 3. 关键设计点

### 3.1 为什么必须是 `Nullable`

`JSONEachRow` 下 JSON `null` 落到非 `Nullable` 列会被写成列默认值（`0` / `''`），"未上报"与"显式 0"就再也分不开；这与 MySQL 侧 `DEFAULT NULL`、Kafka JSON 的 `null` 口径也不一致。故两列均为 `Nullable(...)`，与同表 `ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit` 三个"NULL=未求值"列同款。

### 3.2 JSON 布尔 → `Nullable(UInt8)`

payload 里 `ai_price_miss` 是 JSON `true`/`false`（`mod_kafka` 的 Go `bool` 序列化结果）。ClickHouse 的 `input_format_json_read_bools_as_numbers`（默认 `1`）允许 JSON 布尔写数字列，因此 `Nullable(UInt8)` 可正确落 `1`/`0`；`null` 落 NULL。**该行为在集成测试 TC02 中实证锁定**（若某版本默认值变化，会先在这里红）。

### 3.3 跨引擎类型对齐

| 列 | MySQL（ai-gateway-api） | ClickHouse（本仓） | Doris（本仓） | StarRocks（本仓） |
|----|------------------------|--------------------|---------------|-------------------|
| `ai_price_miss` | `TINYINT DEFAULT NULL` | `Nullable(UInt8)` | `BOOLEAN` | `BOOLEAN` |
| `ai_price_warn` | `VARCHAR(32) DEFAULT NULL` | `Nullable(String)` | `VARCHAR(32)` | `VARCHAR(32)` |

按"语义对齐、不按字面类型对齐"（同 `mirror_hit`：Doris/SR `BOOLEAN`、CH `UInt8`）。`ai_price_warn` 长度取 32，与权威 DDL 一致（六个枚举值最长 `subitem_missing` 15 字符）。

### 3.4 打平层不引入表达式

`bfe_ai_log_load_mv` 里其它 JSON 对象列（`ai_apikeytags`、`ai_rate_limit_hits`）需要 `JSONExtract*` 打平，而这两列是**顶层标量**，直接按名投影即可；不引入表达式也避免了 `JSONExtractBool` 对 `NULL` 与 `false` 的区分问题（`JSONExtractBool` 缺失路径返回 `0`，会把 NULL 语义压平）。

### 3.5 ClickHouse 特有约束：Kafka 引擎表不支持任何 ALTER

实测（本机 ClickHouse 26.10.1.1149）：

```
ALTER TABLE bfe_ai_log_kafka ADD COLUMN IF NOT EXISTS b Nullable(UInt8);
→ Code: 48. DB::Exception: Alter of type 'ADD_COLUMN' is not supported by storage Kafka. (NOT_IMPLEMENTED)
ALTER TABLE bfe_ai_log_kafka DROP COLUMN ...  → 同 CODE 48
```

因此存量升级**不能**对 `bfe_ai_log_kafka` 走 `ADD COLUMN`，只能 `DROP TABLE` + 按 `sqls/bfe_ai_log_kafka.sql` 重建：Kafka 引擎表本身不存数据（纯管道），重建时保持 `kafka_group_name` 不变即可复用已提交位点继续消费，不会重复或丢消息。TC06 对该限制做了**负向断言**（写成 ALTER 会立刻红），避免后来者"顺手简化"成 ALTER。

## 4. 上线顺序与回滚

1. **先升级 schema，再升级 log-reader/bfe**：本仓 SQL 先上（新列有默认 NULL，旧 payload 不受影响；`input_format_skip_unknown_fields=1` 保证旧 Kafka 表遇到未知字段不报错，故反序也不会中断消费，只是不落新列）；
2. **存量环境**按顺序执行（详见升级脚本头部注释）：
   1. `DROP TABLE <DB>.bfe_ai_log_load_mv`（旧 MV 的 SELECT 不含新列，不重建则永远不落值；窗口内不消费、位点不推进，**不丢消息**只是延迟）；
   2. `DROP TABLE <DB>.bfe_ai_log_kafka` → 按 `sqls/bfe_ai_log_kafka.sql` 重建（§3.5：不能 ALTER）；
   3. 执行 `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/bfe_ai_request_log_alter.sql`（明细表加列，`IF NOT EXISTS` 可重复执行）；
   4. 按 `sqls/bfe_ai_log_load_mv.sql` 重建消费 MV；
3. 回滚：明细表 `DROP COLUMN` 两列（Kafka 表按旧 DDL 重建、MV 按旧 DDL 重建）；历史行丢失的信号无法恢复，需以 pb 原始日志为准重放。

## 5. 为什么不进聚合表维度

`bfe_ai_metrics_1m` 的 40 个 KEY 维度是报表筛选/分组口径，维度每加一个都会**打散聚合基数**。缺价是"对账"场景（按 Key/模型/时间在明细里筛 `ai_price_miss=1`），不需要在分钟粒度上分组；且 `ai_price_warn` 是文本枚举，进维度后聚合表体积与查询代价都不划算。故本批只加明细列，聚合表 DDL 与聚合 MV 均不动（TC03/TC05 回归即可证明聚合口径未受影响）。

## 6. 测试

| 用例 | 覆盖 |
|------|------|
| TC01（扩展） | 明细表与暂存表两列存在且类型为 `Nullable(UInt8)` / `Nullable(String)` |
| TC02（扩展） | 真实 Kafka → 引擎表 → 消费 MV：`normal_request` 样例携带 `ai_price_miss=true, ai_price_warn=subitem_missing` → 明细读回 `1` / `'subitem_missing'`；`rate_limit` / `auth_reject` 样例不带字段 → 两列为 **NULL**（`isNull()=1`） |
| TC06（新增，无需 Kafka） | 存量升级：DROP MV + 明细表 DROP 两列 → **负向断言** Kafka 引擎表 ALTER 报 `not supported by storage Kafka` → 重建 Kafka 表 → 升级脚本执行两次（幂等）→ 重建 MV 且 `system.tables.as_select` 含两列投影 |

红/绿口径：TC01/TC02 在改动前的 SQL 上必红（缺列 / 消费后为 NULL）；TC06 在无升级脚本时不可执行。

## 7. 风险

- **Kafka 暂存表不能 ALTER（§3.5）**：升级必须 DROP + 重建；重建期间不消费，位点不推进；
- **Nullable 与 `DEFAULT` 混用**：明细表两列只给 `COMMENT`，不给 `DEFAULT`（`Nullable` 的默认值本就是 NULL，显式 `DEFAULT NULL` 在旧版本 CH 会与 `Nullable` 语义冲突）；
- **DROP/重建 MV 窗口**：窗口内不消费（位点不推进），窗口长度取决于重建耗时（秒级）；不要在 Kafka 侧清位点；
- **历史数据**：升级前入库的行两列为 NULL，无法区分"未上报"与"未发生"——这是设计上的既有事实，报表统计缺价率时应以升级时间点切分窗口。

## 8. 实施记录

**改动文件**

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_log_kafka.sql` | 末尾 +2 列（`Nullable(UInt8)` / `Nullable(String)`），表头字段数 80→82 |
| `sqls/bfe_ai_request_log.sql` | 末尾 +2 列（带 `COMMENT`，不带 `DEFAULT`） |
| `sqls/bfe_ai_log_load_mv.sql` | SELECT 末尾 +2 条裸列投影 |
| `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/bfe_ai_request_log_alter.sql` | 新增：明细表单语句 `ADD COLUMN IF NOT EXISTS`，头部注释给出四步升级顺序与 Kafka 表不能 ALTER 的原因 |
| `demo/normal_request.json` | 增加 `ai_price_miss: true` / `ai_price_warn: "subitem_missing"`（另两份样例保持不带字段，用于 NULL 断言） |
| `tests/integration/.../ch01_clickhouse_dock_test.go` | TC01 +列类型断言；TC02 +两列的 `isNull`/取值断言；新增 `TestTC06PriceMissUpgrade` |

**验证结果**（本机 ClickHouse 26.10.1.1149 + Kafka 4.3.1）

```
--- PASS: TestTC01FreshInstall (0.61s)
--- PASS: TestTC02KafkaConsumptionFlatten (45.77s)   ← 真实 Kafka 消费链路
--- PASS: TestTC03DetailToAggregate (3.41s)
--- PASS: TestTC04UpgradeAlter (1.85s)
--- PASS: TestTC05MetricsRebuild (4.83s)
--- PASS: TestTC06PriceMissUpgrade (0.94s)
ok  clickhouse-it/implementation/scenario-CLICKHOUSE01-clickhouse-dock  57.452s
```

**实施期间修正的设计偏差**

1. 原方案打算对 Kafka 暂存表也走 `ADD COLUMN`，实测 `Code: 48 ... Alter of type 'ADD_COLUMN' is not supported by storage Kafka`（DROP 同样不支持）——改为 DROP + 重建，TC06 补负向断言固化该限制（§3.5）。
2. 因上一条，升级脚本收敛为"只改明细表"，Kafka 表与 MV 的重建按主 DDL 文件重新应用（脚本注释与 §4 已写明顺序）。
3. Kafka 侧 `input_format_json_read_bools_as_numbers` 默认 `1`，JSON `true`/`false` 正常落 `Nullable(UInt8)` 的 `1`/`0`，无需额外 setting（TC02 实证）。

**环境备注（不属于改动物）**：本机 WSL 的原 Kafka KRaft 目录 `/home/zhang/kafka/kraft-combined-logs` 里有历史 root 属主的段文件，以 `zhang` 身份启动会 `AccessDeniedException`，且无 sudo 口令不能 chown；本次验证改用独立 `log.dirs`（`/home/zhang/kafka/kraft-it-logs`，配置副本 `/tmp/server-it.properties`）启动 Kafka，原目录与原有 topic 数据未改动。