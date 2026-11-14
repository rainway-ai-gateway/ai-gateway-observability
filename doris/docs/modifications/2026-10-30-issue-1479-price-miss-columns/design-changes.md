# 明细表新增缺价信号列 `ai_price_miss` / `ai_price_warn`（bfe #1479）

- **日期**：2026-10-30
- **上游**：[bfenetworks/bfe#1479](https://github.com/bfenetworks/bfe/issues/1479)（缺价动作三态 + 形态化信号）、`bfe-access-pb v0.3.17`（field 763/766）、`ai-gateway-api 508e552`（MySQL 明细表 99→101 列）、log-reader `2ef0777`（Kafka 默认输出携带两字段）
- **状态**：**已实施**（SQL 资产 + 升级脚本 + 集成测试）
- **结论**：明细表 `bfe_ai_request_log` 末尾追加 `ai_price_miss BOOLEAN` / `ai_price_warn VARCHAR(32)`，Routine Load 的 `COLUMNS` 补两条裸列映射；存量环境走新增的 `sqls/upgrade/.../bfe_ai_request_log_alter.sql`。聚合表与 INSERT JOB 不改。

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

改动前 Doris 侧无这两列；Routine Load 的 `COLUMNS` 是显式清单，未列出的 JSON 字段被直接丢弃，因此报表永远查不到缺价行。

## 2. 改动点

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_request_log.sql` | 明细表末尾追加 `ai_price_miss BOOLEAN COMMENT ...`、`ai_price_warn VARCHAR(32) COMMENT ...` |
| `sqls/bfe_ai_log_load_routine.sql` | `COLUMNS` 末尾追加 `ai_price_miss, ai_price_warn`（裸列映射，无需 `json_extract`） |
| `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/bfe_ai_request_log_alter.sql` | 存量环境 `ALTER TABLE ... ADD COLUMN`（含重复执行报错契约说明） |
| `sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql` | 回灌列清单补两列（该脚本 `INSERT INTO _v2 SELECT <显式列>` 必须与当前 101 列 schema 对齐，否则报 `insert into cols should be corresponding to the query output`）；头部补前置条件说明 |
| `tests/integration/`（doris-it） | TC01 类型断言补两列；TC02 覆盖 Routine Load 扩映射被接受；新增 TC07 覆盖升级脚本与明细 NULL 语义 |

`bfe_ai_metrics_1m.sql` / `bfe_ai_metrics_1m_job.sql` 不改。

## 3. 关键设计点

### 3.1 `ai_price_miss` 用 `BOOLEAN`

payload 里 `ai_price_miss` 是 JSON `true`/`false`（`mod_kafka` 的 Go `bool` 序列化）。Doris 的 JSON 导入对 `BOOLEAN` 列把 JSON 布尔转 `1`/`0`（同表 `mirror_hit` / `ai_intent_cache_hit` 已验证），且 Doris 列默认可空，字段缺省即 NULL——正是需要的三态语义。若改用 `TINYINT`，StarRocks 侧会静默落 NULL（见 `2026-10-22-issue-10-boolean-columns`），Doris 虽可吸收但会引入跨引擎口径分歧，故统一 `BOOLEAN`。

### 3.2 `ai_price_warn` 用 `VARCHAR(32)`

与 `ai-gateway-api` 权威 DDL 一致（六个枚举最长 `subitem_missing`，15 字符）。列可空 → 未上报为 NULL，不用 `DEFAULT ''`（空串会让"未上报"与"空形态"混淆）。

### 3.3 跨引擎类型对齐

| 列 | MySQL（api 仓权威） | ClickHouse | Doris（本仓） | StarRocks（本仓） |
|----|---------------------|-----------|---------------|-------------------|
| `ai_price_miss` | `TINYINT DEFAULT NULL` | `Nullable(UInt8)` | `BOOLEAN` | `BOOLEAN` |
| `ai_price_warn` | `VARCHAR(32) DEFAULT NULL` | `Nullable(String)` | `VARCHAR(32)` | `VARCHAR(32)` |

按"语义对齐、不按字面类型对齐"（先例：`mirror_hit` 在 Doris/SR 是 `BOOLEAN`、在 CH 是 `UInt8`）。

### 3.4 列位置：末尾追加

Doris `UNIQUE KEY` 表要求 key 列是列前缀，非 key 列必须追加在末尾——两列都是普通值列，只能追加在 `ai_intent_questions_version` 之后，与 MySQL 权威 DDL 的追加位置一致（列序对 Routine Load 无名映射无影响，但对存量 `ALTER` 与跨库核对有益）。

### 3.5 Routine Load 的 `COLUMNS` 不可在线修改

Doris 不支持 `ALTER ROUTINE LOAD ... SET ("columns"=...)`：存量环境必须 `PAUSE` → `STOP` → `DROP` 旧 job → 用新 `sqls/bfe_ai_log_load_routine.sql` 重建（脚本注释已含 `OFFSET_BEGINNING` 语义说明；重建会按新消费组/位点重放，注意避免重复导入 —— 本链路以 `UNIQUE KEY` 幂等覆盖收敛，重复导入不产生重复行）。升级步骤写入 §4。

## 4. 上线顺序与回滚

1. **先升级 schema**：存量环境执行 `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/bfe_ai_request_log_alter.sql`（每环境一次；重复执行报 `column already exists`）；
2. **重建 Routine Load**：`STOP` 旧 job → `DROP ROUTINE LOAD FOR <job>` → 用新 `sqls/bfe_ai_log_load_routine.sql` 重建（`COLUMNS` 含两列）；
3. 新装环境：直接按 `setup.sh` 顺序应用即可（DDL 已含两列）；
4. 回滚：DROP 两列并重建回旧 `COLUMNS` 的 job；历史行丢失的信号无法在 Doris 内恢复，需重放 Kafka 原始消息。

**顺序不可颠倒**：若先重建 job 而表上没有新列，Routine Load 创建时即报列不存在。

## 5. 为什么不进聚合维度

`bfe_ai_metrics_1m` 的 40 个 KEY 维度是报表分组口径，每加一维都会打散聚合基数。缺价是"按 Key/模型/时间在明细里筛 `ai_price_miss=1`"的对账场景，不需要分钟粒度分组；`ai_price_warn` 是文本枚举，进维度代价更大。故聚合表、INSERT JOB 均不动（TC03/TC05/TC06 回归证明聚合口径未受影响）。

## 6. 测试

| 用例 | 覆盖 |
|------|------|
| TC01（扩展） | 明细表两列存在且类型正确（Doris `information_schema` 把 `BOOLEAN` 报告为 `tinyint`） |
| TC02（扩展） | Routine Load 的 `COLUMNS` 含两列时 job 被 Doris 接受（创建即校验） |
| TC03（扩展） | demo 样例直插链路：`ai_price_miss=1`、`ai_price_warn='subitem_missing'` 落库；缺省样例两列为 NULL |
| TC07（新增） | 存量升级：模拟旧表（DROP 两列）→ 执行升级脚本 → 两列就位且类型正确；二次执行报 `already exists`（每环境一次契约） |

红/绿口径：TC01/TC02/TC03 在改动前必红（缺列 / job 不接受列 / 消费后为 NULL）。

## 7. 风险

- **job 重建窗口不消费**：窗口内消息堆积在 Kafka（位点停在 STOP 处），重建后继续；窗口别跨过 topic 保留期；
- **重复导入**：重建 job 若从 `OFFSET_BEGINNING` 起消费会重放历史，`UNIQUE KEY` 幂等覆盖收敛（`logid` 入键后不会误折叠，见 `2026-10-13-issue-1417-detail-unique-key-logid`）；
- **历史数据**：升级前入库的行两列为 NULL，报表统计缺价率需按升级时间点切分窗口；
- **`BOOLEAN` 与 `TINYINT` 视图差异**：`information_schema` 中以 `tinyint` 呈现，集成测试断言按该口径书写。

## 8. 实施记录

**改动文件**

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_request_log.sql` | 末尾 +2 列（`BOOLEAN` / `VARCHAR(32)`，可空，无 `DEFAULT`） |
| `sqls/bfe_ai_log_load_routine.sql` | `COLUMNS` 末尾 +2 条裸列映射 |
| `sqls/upgrade/2026-10-30-issue-1479-price-miss-columns/bfe_ai_request_log_alter.sql` | 新增：`ALTER TABLE ... ADD COLUMN` 两列（每环境一次），头部注释写明"先改表再重建 job"的顺序 |
| `sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql` | 回灌 `SELECT` 补 `ai_price_miss, ai_price_warn`；头部补"存量表须先有这两列"的前置条件 |
| `demo/normal_request.json` | 增加 `ai_price_miss: true` / `ai_price_warn: "subitem_missing"` |
| `tests/integration/.../doris01_report_cache_mirror_intent_test.go` | 新增 `priceMissColumns` 映射；TC01/TC02 断言扩展到两列；新增 `TestTC07PriceMissUpgrade` |

**验证结果**（本机 Doris 3.0.8，WSL2 单机 FE+BE）

```
--- PASS: TestTC01FreshInstallSchema (3.47s)
--- PASS: TestTC02RoutineLoadAccepted (2.98s)
--- PASS: TestTC03DetailToAggregate (6.39s)
--- PASS: TestTC04UpgradeAlter (3.68s)
--- PASS: TestTC05MetricsRebuildSwap (4.51s)
--- PASS: TestTC07PriceMissUpgrade (5.13s)
--- PASS: TestTC06DetailUniqueKeyRebuild (10.92s)
ok  doris-it/implementation/scenario-DORIS01-report-cache-mirror-intent  37.114s
```

**实施期间修正的设计偏差**

1. **唯一键重建脚本被本次加列打破**：`2026-10-13-…/bfe_ai_request_log_rebuild.sql` 用显式 99 列 `INSERT INTO _v2 SELECT ...`，新表变 101 列后 Doris 直接报 `insert into cols should be corresponding to the query output`（TC06 首跑即红）。已把两列补进回灌清单，并在脚本头部写明升级前置条件（存量表须先执行本次加列）。
2. **升级顺序与 1417 重建的耦合**：两者都要动明细表，正确顺序是"先加列（本批），再跑唯一键重建（回灌按 101 列）"；已在两个脚本头部互相标注。

## 9. 参考

- 本仓先例：`docs/modifications/2026-09-29-report-cache-mirror-intent-doris-alignment/`（同类"明细加列 + Routine Load 扩映射 + 聚合维度"批次，本次只做前两项）
- 唯一键改造：`docs/modifications/2026-10-13-issue-1417-detail-unique-key-logid/`
- 上游：`bfe/docs/zh_cn/modifications/2026-10-30-issue-1479-price-miss-control-and-observability/`、`ai-gateway-api/design-docs/modifications/2026-10-30-issue-1479-price-miss-action/`、log-reader `doc/modifications/2026-10-30-issue-1479-price-miss-fields/`