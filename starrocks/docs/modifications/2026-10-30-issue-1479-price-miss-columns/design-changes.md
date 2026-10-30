# 明细表新增缺价信号列 `ai_price_miss` / `ai_price_warn`（bfe #1479）

- **日期**：2026-10-30
- **上游**：[bfenetworks/bfe#1479](https://github.com/bfenetworks/bfe/issues/1479)（缺价动作三态 + 形态化信号）、`bfe-access-pb v0.3.17`（field 763/766）、`ai-gateway-api 508e552`（MySQL 明细表 99→101 列）、log-reader `2ef0777`（Kafka 默认输出携带两字段）
- **状态**：**已实施**（SQL 资产 + 集成测试）
- **结论**：明细表 `bfe_ai_request_log` 末尾追加 `ai_price_miss BOOLEAN` / `ai_price_warn VARCHAR(32)`，Routine Load 的 `COLUMNS` 补两条裸列映射。聚合 MV、`bfe_ai_metrics_1m` 不改。

---

## 1. 背景

模型缺少某接口定价时，BFE 旧行为是"回退按该模型 chat 价计费"，v0.7.0.1 测试暴露的缺陷是：**该接口的请求不扣费且没有任何可核对痕迹**。bfe #1408 先落 `ai_price_miss` 标记，#1479 补齐三态动作（`fallback_chat` / `bill_zero` / `reject`）与形态化信号 `ai_price_warn`。

报表链路的要求：缺价行必须能在明细里筛出来对账，且**不能把"未发生缺价"读成"发生了但形态未知"**：

| 取值 | 含义 |
|------|------|
| `fallback` | 非 chat 接口按该模型 chat 价计（少收风险） |
| `subitem_missing` | 计价条目缺子项价键，缺失子项按 0（少收） |
| `no_entry` / `no_baseline` | (模型, 接口) 条目缺失 / 且无 chat 价可回退 |
| `no_model_table` | 集群 `ModelTable` 整体缺失 |
| `reject` | 接口未定价且 `PriceMissAction=reject`：请求被拒（`res_status_code=503`，未转发但**行仍写出**） |
| `ai_price_miss` = `1` / `0` | 发生过 / 显式未发生缺价回退 |
| 两列 **NULL** | 未上报（旧版 bfe / 字段缺省）——**≠ 未发生** |

改动前 SR 侧无这两列；Routine Load 的 `COLUMNS` 是显式清单，未列出的 JSON 字段被直接丢弃，报表永远查不到缺价行。

## 2. 改动点

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_request_log.sql` | 明细表末尾追加 `ai_price_miss BOOLEAN COMMENT ...`、`ai_price_warn VARCHAR(32) COMMENT ...` |
| `sqls/bfe_ai_log_load_routine.sql` | `COLUMNS` 末尾追加 `ai_price_miss, ai_price_warn`（裸列映射，无 `get_json_*` 表达式） |
| `tests/integration/`（starrocks-it） | TC01 类型断言补两列；TC02 覆盖真实 Kafka 消费取值与 NULL 语义；TC04 覆盖存量 `ADD COLUMN` |

`bfe_ai_metrics_1m.sql` 与聚合 MV 不改。

## 3. 关键设计点

### 3.1 `ai_price_miss` 必须 `BOOLEAN`（issue #10 教训）

payload 里 `ai_price_miss` 是 JSON `true`/`false`。StarRocks 的 JSON 导入**只在 `BOOLEAN` 目标列**上把 JSON 布尔转 `1`/`0`；`TINYINT` 目标列会**静默写 NULL**（不报错、不计 `NumberFilteredRows`），这正是 `2026-10-22-issue-10-boolean-columns` 修 `ai_stream` / `is_trust_src_ip` 的原因。本批直接按正确类型落地，避免重犯；SR 列默认可空，字段缺省即 NULL。

### 3.2 `ai_price_warn` 用 `VARCHAR(32)`

与 `ai-gateway-api` 权威 DDL 一致（最长枚举 `subitem_missing` 15 字符，留余量）。缺省为 NULL（SR 列默认可空），不使用 `DEFAULT ''`——空串会让"未上报"与"空形态"混淆。

### 3.3 跨引擎类型对齐

| 列 | MySQL（api 仓权威） | ClickHouse | Doris | StarRocks（本仓） |
|----|---------------------|-----------|-------|-------------------|
| `ai_price_miss` | `TINYINT DEFAULT NULL` | `Nullable(UInt8)` | `BOOLEAN` | `BOOLEAN` |
| `ai_price_warn` | `VARCHAR(32) DEFAULT NULL` | `Nullable(String)` | `VARCHAR(32)` | `VARCHAR(32)` |

按"语义对齐、不按字面类型对齐"（同 `mirror_hit`）。

### 3.4 列位置：末尾追加

SR 明细表是 `DUPLICATE KEY(hostid, log_time, ai_apikey_id, ai_requested_model)`，key 列固定在前，普通值列追加在 `ai_intent_questions_version` 之后——与 MySQL 权威 DDL 的追加位置一致，便于跨库核对。

### 3.5 升级脚本的幂等写法（SR 3.5.21 差异）

SR 3.5.21 **不支持** `ADD COLUMN IF NOT EXISTS`（`2026-10-02-starrocks-dock` 已记录，`1064 No viable statement`），因此存量升级不提供独立 SQL 文件，而在 §4 给出步骤：先按 `information_schema.columns` 判断列是否已存在，再执行 `ALTER TABLE ... ADD COLUMN`；重复执行会报 `Duplicate column name`，属预期（每环境一次）。

### 3.6 Routine Load 的 `COLUMNS` 不可在线修改

SR 不支持 `ALTER ROUTINE LOAD ... SET ("columns"=...)`：存量环境必须 `PAUSE` → `STOP` → `DROP` 旧 job → 用新 `sqls/bfe_ai_log_load_routine.sql` 重建（`COLUMNS` 含两列）。明细表是 `DUPLICATE KEY` 模型、**无幂等覆盖**，重建 job 若从 `OFFSET_BEGINNING` 起消费会重放历史并产生重复行——升级时优先沿用既有消费组/位点（脚本参数 `KAFKA_GROUP_ID` 不变即从已提交位点续消费），确需重放时先确认是否接受重复行。

## 4. 上线顺序与回滚

1. **先升级 schema**（存量环境）：

   ```sql
   -- 1) 判断列是否存在（存在则跳过）
   SELECT column_name FROM information_schema.columns
    WHERE table_schema = '<DB>' AND table_name = 'bfe_ai_request_log'
      AND column_name IN ('ai_price_miss','ai_price_warn');
   -- 2) 不存在则加列
   ALTER TABLE bfe_ai_request_log
       ADD COLUMN ai_price_miss BOOLEAN      COMMENT '是否发生缺价回退（NULL=未上报）' AFTER ai_intent_questions_version,
       ADD COLUMN ai_price_warn VARCHAR(32)  COMMENT '缺价形态：fallback/subitem_missing/no_entry/no_baseline/no_model_table/reject（NULL=未上报）' AFTER ai_price_miss;
   ```

   注意：SR 的 Schema Change 是**异步**的，`ADD COLUMN` 返回成功时列尚未对查询可见（需轮询 `information_schema.columns` 直到列出现）。

2. **重建 Routine Load**：`STOP` → `DROP` 旧 job → 用新 `sqls/bfe_ai_log_load_routine.sql` 重建（沿用 `KAFKA_GROUP_ID` 以续用已提交位点）；
3. 新装环境：直接按 `setup.sh` 应用（DDL 已含两列）；
4. 回滚：DROP 两列 + 重建回旧 `COLUMNS` 的 job；历史行丢失的信号需重放 Kafka 原始消息，且重放会产生重复行（`DUPLICATE KEY`），需先确认口径。

**顺序不可颠倒**：先重建 job 而表上无对应列 → job 创建即报列不存在。

## 5. 为什么不进聚合 MV

`bfe_ai_metrics_1m` 的维度是报表分组口径，每加一维打散聚合基数。缺价是"按 Key/模型/时间在明细里筛 `ai_price_miss=1`"的对账场景，不需要分钟粒度分组；`ai_price_warn` 是文本枚举，进维度代价更大。故聚合 MV 与聚合表均不动（TC03/TC05 回归证明聚合口径未受影响）。

## 6. 测试

| 用例 | 覆盖 |
|------|------|
| TC01（扩展） | 明细表两列存在且类型为 `boolean` / `varchar(32)`（`ColumnTypesDesc`） |
| TC02（扩展） | 真实 Kafka → Routine Load：`normal_request` 样例携带 `ai_price_miss=true, ai_price_warn=subitem_missing` → 读回 `1` / `'subitem_missing'`；未携带字段的样例 → 两列 NULL |
| TC04（扩展） | 存量 `ADD COLUMN` 成功（Schema Change 异步，轮询至可见）；重复执行报 `Duplicate column` |

红/绿口径：TC01/TC02 在改动前的 SQL 上必红（缺列 / 消费后仍为 NULL）。

## 7. 风险

- **`BOOLEAN` 在 `information_schema` 中呈现为 `tinyint`**：断言按该口径书写（与 `mirror_hit` 一致）；
- **job 重建窗口不消费**：消息堆在 Kafka，重建后续消费；窗口别跨 topic 保留期；
- **重放产生重复行**：`DUPLICATE KEY` 模型无幂等覆盖，沿用原消费组是最安全的做法；
- **历史数据**：升级前入库的行两列为 NULL，报表统计缺价率需按升级时间点切分窗口。

## 8. 实施记录

**改动文件**

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_request_log.sql` | 末尾 +2 列（`BOOLEAN` / `VARCHAR(32)`，可空，无 `DEFAULT`） |
| `sqls/bfe_ai_log_load_routine.sql` | `COLUMNS` 末尾 +2 条裸列映射 |
| `demo/normal_request.json` | 增加 `ai_price_miss: true` / `ai_price_warn: "subitem_missing"`（另两份样例保持不带字段，用于 NULL 断言） |
| `tests/integration/.../sr01_starrocks_dock_test.go` | TC01 明细表列数 102→104、布尔类型断言加入 `ai_price_miss`、`ai_price_warn` 类型断言；TC02 逐行断言补两列（normal 及其流式变体为 `1`/`subitem_missing`，另两样例 NULL）；新增 `TestTC06PriceMissUpgrade` |

**验证结果**（本机 StarRocks 3.5.21 + Kafka 4.3.1）

```
--- PASS: TestTC01FreshInstall (0.57s)
--- PASS: TestTC02KafkaConsumptionFlatten (27.18s)   ← 真实 Kafka → Routine Load 链路
--- PASS: TestTC03DetailToAggregate (97.08s)
--- PASS: TestTC04UpgradeAlter (3.88s)
--- PASS: TestTC06PriceMissUpgrade (4.19s)
--- PASS: TestTC05MetricsRebuild (14.24s)
ok  starrocks-it/implementation/scenario-STARROCKS01-starrocks-dock  147.188s
```

**实施期间修正的问题**

1. **TC01 列数断言**：明细表 102 → 104（两版 DDL 列定义数同步），否则新列一加即红。
2. **TC02 的 NULL 渲染口径**：断言查询最初写成 `IFNULL(IF(ai_price_miss, '1','0'), '')`，测出 `"0"` 而期望 `""`——SR 的 `IF(NULL, a, b)` 会把 NULL 当 false 返回 `b`，必须先 `IF(col IS NULL, ...)` 显式判空（ClickHouse 侧 `toString(NULL)` 天然返回 NULL，不受影响）。这是**测试表达式**的坑，不是落库口径问题。
3. **JSON 布尔 → `BOOLEAN` 实测通过**：TC02 中 normal 样例携带 `"ai_price_miss": true` 落库为 `1`，未携带字段的两份样例为 NULL——`BOOLEAN` + 可空正是所需语义（若误声明 `TINYINT` 会静默落 NULL，两份样例虽"碰巧"通过但 normal 行会立刻暴露）。

## 9. 参考

- 本仓先例：`docs/modifications/2026-10-02-starrocks-dock/`（建仓与 SQL 资产）、`docs/modifications/2026-10-22-issue-10-boolean-columns/`（JSON 布尔必须落 `BOOLEAN`）
- 上游：`bfe/docs/zh_cn/modifications/2026-10-30-issue-1479-price-miss-control-and-observability/`、`ai-gateway-api/design-docs/modifications/2026-10-30-issue-1479-price-miss-action/`、log-reader `doc/modifications/2026-10-30-issue-1479-price-miss-fields/`