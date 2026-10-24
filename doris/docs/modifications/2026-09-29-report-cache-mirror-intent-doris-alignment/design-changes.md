# 数据报表二期：Doris 侧对齐缓存/镜像/意图字段 设计变更

- **日期**：2026-09-29
- **变更范围**：Doris 数仓侧（明细表 `bfe_ai_request_log`、聚合表 `bfe_ai_metrics_1m`、Routine Load、INSERT JOB）
- **变更原因**：v0.8 一期按"MySQL 先行"策略交付了缓存/镜像/意图三组访问日志字段的报表能力，ai-gateway-api 的 `storage/dorisreport` 查询层 SQL 已引用 `ai_cache_status` 等新列，而 Doris 侧的表结构、Routine Load 映射、聚合 JOB 均未加列——`[Report].Backend = "doris"` 部署下凡是触碰新列的查询会直接报"列不存在"，三个新维度的下钻请求被能力门控返回 422。本变更消除这一错位，使 Doris 后端与 MySQL 后端能力拉平。
- **设计依据**：《数据报表-Doris 后端设计方案（v0.8）》`迭代系统设计/v0.8/doris-report/数据报表-Doris后端设计方案.md`（本变更实现其 §4 Doris 侧改动；查询层改动由 ai-gateway-api 仓另行落地）

---

## 1. 背景

v0.7 确立了"一套报表 API、两种存储后端、配置切换"架构（`[Report].Backend = "mysql" | "doris"`）。v0.8 一期交付了三组字段（`ai_cache_status` / `mirror_hit` / `ai_intent_answer` 等 10 列）的 MySQL 全量能力；Doris 链路相关环节现状：

| 环节 | 现状 |
|------|------|
| bfe-access-pb proto（v0.3.7/3.8/3.9） | ✅ 已发布，BFE 已回填 |
| log-reader 字段注册（`mod_fields/field_registry.go`） | ✅ 三组字段已注册，mod_kafka JSON 已自动携带新字段 |
| Doris 入库（`sqls/bfe_ai_request_log.sql` + `bfe_ai_log_load_routine.sql`） | ❌ 明细表无 10 列，Routine Load COLUMNS 无映射（新字段到 Kafka 后被丢弃） |
| 聚合 JOB（`sqls/bfe_ai_metrics_1m.sql` / `bfe_ai_metrics_1m_job.sql`） | ❌ 聚合表无 3 维度列，JOB 的 SELECT/GROUP BY 未改 |
| 查询层（ai-gateway-api `storage/dorisreport`） | ⚠️ 一期只落明细支撑部分；三个新维度被门控 422（由 api 仓二期补齐） |

Doris 侧不做本期补齐的话，查询层引用的列在 Doris 中不存在，错位持续存在。

## 2. 目标与非目标

**目标**：

1. 明细表在线 `ALTER` 加 10 列，列名、类型与 MySQL 侧 `db_ddl_report_mysql.sql` 保持一致（写入侧 log-reader 列序基准 + 两仓 DDL 同评审的既定约束）；
2. Routine Load `COLUMNS(...)` 映射追加 10 列，使 mod_kafka JSON 中新字段真正落库；
3. 聚合表 `bfe_ai_metrics_1m`（AGGREGATE KEY 模型）重建并增加 3 个 KEY 维度列，INSERT JOB 的列清单与 `GROUP BY` 同步；
4. `Backend = "doris"` 下五类报表端点全部可用，不再出现列不存在错误。

**非目标**：

- 不改 MySQL 侧任何已交付内容；
- 不改 log-reader、BFE、ai-gateway-web；
- 不重建聚合表历史数据（分钟表只回看近期窗口，新维度从切换时刻起积累，同一期 MySQL 既定口径）；如需历史窗口可选按新口径重算；
- `ai_cache_key` 与 mirror 异步结果字段（844–850）按一期决策不进报表；
- 查询层（dorisreport 新维度 SQL 分支、`Capabilities()` 拉平、report.md 标注更新）属于 ai-gateway-api 仓，不在本变更范围。

## 3. 变更总览

| # | 变更 | 涉及文件 | 方式 |
|---|------|----------|------|
| 1 | 明细表加 13 列（10 列缓存/镜像/意图 + 3 列限流打平） | `sqls/bfe_ai_request_log.sql` | 在线 `ALTER TABLE ... ADD COLUMN`，低成本（每环境执行一次） |
| 2 | Routine Load 扩 13 列映射 | `sqls/bfe_ai_log_load_routine.sql` | 改 `COLUMNS(...)` 后重启/重建 Routine Load |
| 3 | 聚合表加 3 个 KEY 维度（重建） | `sqls/bfe_ai_metrics_1m.sql` | AGGREGATE KEY 模型 KEY 列只能整表重建，`REPLACE WITH TABLE`（swap）原子换名 |
| 4 | INSERT JOB 同步 3 维 + 限流打平列消费 | `sqls/bfe_ai_metrics_1m_job.sql` | INSERT 列清单与 `GROUP BY` 追加 3 列，`COALESCE` 归一空值；限流维度改读打平标量列（见 §4.4） |

## 4. 详细设计

### 4.1 明细表加 13 列（在线 ALTER，每环境执行一次）

`bfe_ai_request_log` 为 UNIQUE KEY 模型，加列走在线 `ALTER`：

**缓存/镜像/意图 10 列**（列名、类型与 MySQL 侧 `db_ddl_report_mysql.sql` 保持一致）：

| 新列 | Doris 类型 | 说明 |
|------|-----------|------|
| `ai_cache_status` | VARCHAR(16) | 缓存状态：hit/miss/skip，空=未启用 |
| `mirror_hit` | BOOLEAN | 镜像是否命中 |
| `mirror_cluster` | VARCHAR(128) | 镜像集群 |
| `ai_intent_question` | VARCHAR(64) | 意图问题 |
| `ai_intent_answer` | VARCHAR(64) | 意图答案（含 unknown） |
| `ai_intent_confidence` | DOUBLE | 意图置信度（NULL=未分类） |
| `ai_intent_source` | VARCHAR(32) | 来源：explicit_header/classifier/cache |
| `ai_intent_latency_us` | BIGINT | 意图决策耗时（微秒，NULL=未分类） |
| `ai_intent_cache_hit` | BOOLEAN | 意图缓存命中（NULL=未分类） |
| `ai_intent_questions_version` | VARCHAR(32) | 意图问题集版本 |

**限流打平 3 列**（Doris 3.0 兼容性修复，见 §4.4）：

| 新列 | Doris 类型 | 说明 |
|------|-----------|------|
| `rate_limit_policy_id` | VARCHAR(128) | 限流策略 ID（取首个命中，打平列） |
| `rate_limit_type` | VARCHAR(32) | 限流类型（取首个命中，打平列） |
| `rate_limit_rule_name` | VARCHAR(128) | 限流规则名（取首条规则，打平列） |

> **执行次数约束**：Doris 3.0 不支持 `ADD COLUMN IF NOT EXISTS`，升级脚本每环境仅执行一次，重复执行报 `Duplicate column`（属预期）。新列从映射上线后到达的消息起积累（Routine Load 对存量 Kafka 消息不追溯，同一期 MySQL 链路既定口径）。

### 4.2 Routine Load 扩 13 列映射

`bfe_ai_log_load_routine.sql` 的 `COLUMNS(...)` 映射追加缓存/镜像/意图 10 列，使 mod_kafka JSON 中的新字段落库而非被丢弃；同时新增 3 个限流打平列（`rate_limit_policy_id` 等），在导入时用 `json_unquote(json_extract(ai_rate_limit_hits, '$[0].rate_limit_policy_id'))` 从 JSON 数组提取首个命中（与 `level1Name` 打平同一模式，见 §4.4）。修改后需对存量 Routine Load 任务重启或重建生效，且必须在 schema（§4.1）就位之后、新列数据大量到达之前完成（schema-first）。

> **时区口径修复（2026-10-01，SC36 集成测试实跑发现）**：`log_time = FROM_UNIXTIME(timestamp)` 按 FE 会话时区解释 epoch，非 UTC 时区（如本机 Asia/Shanghai）下存储偏移，按 UTC 过滤的报表 API / Grafana 窗口查不到数据。修复：`log_time` 改为 `DATE_SUB(FROM_UNIXTIME(timestamp), INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND)`（会话偏移动态扣除，写 UTC 墙钟，与 MySQL 链路 log-reader 写入口径一致），任何 FE 时区下口径一致。INSERT JOB 的分钟窗口同步改用 `UTC_TIMESTAMP()`（§4.4）。

### 4.3 聚合表重建加 3 个 KEY 维度（本变更最大的变更点）

`bfe_ai_metrics_1m` 为 AGGREGATE KEY 模型，KEY 列变更只能整表重建。新聚合表在既有 37 维 + 24 指标基础上增加 3 个 KEY 维度列（Doris KEY 列天然 NOT NULL，空值由 JOB 写入侧 `COALESCE` 归一）：

| 新 KEY 列 | 类型 | 说明 |
|-----------|------|------|
| `ai_cache_status` | VARCHAR(16) | 缓存状态（hit/miss/skip，空=未启用） |
| `mirror_hit` | TINYINT | 镜像命中（0/1） |
| `ai_intent_answer` | VARCHAR(64) | 意图答案（含 unknown，空=未分类） |

**重建步骤（生产选低峰窗口执行）**：

1. 按新 schema 建表 `bfe_ai_metrics_1m_v2`（分区从当前窗口前 7 天起建，由动态分区属性自动向前滚动）；
2. （可选）从新明细表按新口径 `INSERT INTO ..._v2 SELECT ... GROUP BY ts_min, <37+3 维>` 重算近 7 天历史窗口；
3. 暂停 Doris 侧聚合 INSERT JOB（`STOP JOB` / 暂停调度），等待当前周期收尾；
4. `ALTER TABLE bfe_ai_metrics_1m REPLACE WITH TABLE bfe_ai_metrics_1m_v2 PROPERTIES('swap' = 'true')`（原子换名，秒级、查询不断流；Doris 3.0.8 实际语法为 `REPLACE WITH TABLE ... PROPERTIES('swap'='true')`，即设计稿所称 SWAP；不支持时降级为 RENAME 两步换名，秒级无数据可见窗口可接受）；
5. 更新 `bfe_ai_metrics_1m_job.sql` 的 INSERT 目标表与 `GROUP BY`（§4.4），恢复 JOB；
6. 验证报表数据正常后删除旧表。

**历史数据策略**：分钟聚合表只服务近期窗口（明细保留 7 天、聚合图常规看 7 天内），默认不重算历史——三个新维度从切换时刻起积累。若部署方要求历史窗口立即可下钻，可在步骤 2 按新口径从明细表重算近 7 天（明细尚在保留期内，成本可控），但不做强求。

**行数膨胀评估**（沿用一期结论）：3 个维度基数低（cache_status ≤4、mirror_hit 2、intent_answer ≤ 问题选项数 ~10），分钟 × 既有维度组合膨胀估 10–30 倍相关子集；分钟表仅近期窗口且按天分区滚动，可接受。上线后观察表行数与 JOB 耗时，膨胀超预期时降级预案同一期：intent 维度降采样或缩短聚合表保留期。

### 4.4 INSERT JOB 同步

`bfe_ai_metrics_1m_job.sql`：INSERT 的列清单与 `GROUP BY` 追加 3 列，维度空值归一与既有 JOB 的 `COALESCE` 风格一致：

```sql
-- 伪代码，对齐既有 JOB 的 SELECT 形态
SELECT ts_min, ..., COALESCE(ai_cache_status,'') AS ai_cache_status,
       COALESCE(mirror_hit,0) AS mirror_hit,
       COALESCE(ai_intent_answer,'') AS ai_intent_answer,
       SUM(...) ...
FROM bfe_ai_request_log
WHERE log_time >= <窗口起点> AND log_time < <窗口终点>
GROUP BY ts_min, <既有 37 维>, ai_cache_status, mirror_hit, ai_intent_answer;
```

> **Doris 3.0 兼容性修复（集成测试驱动发现）**：既有 JOB 用 `ELEMENT_AT(ai_rate_limit_hits, 1).rate_limit_policy_id` 访问 `ARRAY<STRUCT>` 元素字段，而 Doris 3.0.8 不支持任何 struct 字段解引用形式（`.` / `['field']` / `ELEMENT_AT` 复合 / CAST 均验证不可用），导致 CREATE JOB 在创建时即校验失败。修复：明细表新增 `rate_limit_policy_id`/`rate_limit_type`/`rate_limit_rule_name` 打平标量列，由 Routine Load 在导入时 `json_extract` 提取（§4.2），JOB 改读标量列（`COALESCE(rate_limit_policy_id, '')` 等，GROUP BY 与聚合表列名不变，Grafana 侧无感）。

该 JOB 由数仓侧调度（与 ai-gateway-api 无耦合），`Backend = "doris"` 时 ai-gateway-api 不会启动任何进程内 JOB。

## 5. 涉及文件清单

| 文件 | 修改内容 |
|------|----------|
| `doris/sqls/bfe_ai_request_log.sql` | 明细表 CREATE TABLE 增加 13 列（10 列缓存/镜像/意图 + 3 列限流打平） |
| `doris/sqls/upgrade/2026-09-29-report-cache-mirror-intent/bfe_ai_request_log_alter.sql` | **新增**：存量明细表在线 `ALTER ... ADD COLUMN` 13 列（每环境执行一次；Doris 3.0 无 `IF NOT EXISTS`，重复执行报 Duplicate column 属预期） |
| `doris/sqls/bfe_ai_log_load_routine.sql` | Routine Load `COLUMNS(...)` 映射追加 10 列 + 限流打平 3 列（`json_extract('$[0].xxx')`） |
| `doris/sqls/bfe_ai_metrics_1m.sql` | 聚合表 DDL：新 schema（既有 37 维 + 24 指标 + 3 个新 KEY 维度 = 40 维），供 `_v2` 建表与全新安装 |
| `doris/sqls/bfe_ai_metrics_1m_job.sql` | INSERT JOB：列清单与 `GROUP BY` 追加 3 维；限流维度改读打平标量列（Doris 3.0 不支持 struct 字段解引用，见 §4.4） |
| `doris/demo/normal_request.json` | demo 样例补充 10 个新字段（含缓存命中 + 意图分类 + 镜像命中的完整示例） |
| `doris/docs/design/TABLE_DESIGN.md` | 明细表/聚合表字段文档同步（v1.2） |
| `doris/docs/user/HOWTO.md` | 新增「11. 存量部署升级」章节（扩列 / Routine Load 重建 / 聚合表 SWAP 重建 / 验证），文件结构与更新历史同步（v1.7） |

> 存量部署升级操作步骤见 `doris/docs/user/HOWTO.md` 第 11 节。

## 6. 兼容性与发布顺序

1. **本仓（ai-gateway-observability）先于 ai-gateway-api 上线**：
   a. 明细表 `ALTER` +10 列、`bfe_ai_log_load_routine.sql` 扩映射并重启/重建 Routine Load（schema-first，先于新列数据到达）；
   b. 低峰窗口执行聚合表重建（§4.3），同步更新 INSERT JOB；
2. **ai-gateway-api 二期发布**：dorisreport 新维度分支 + `Capabilities()` 拉平。api 升级必须在 Doris 侧步骤 1 完成之后（升级前新维度请求 422、新列查询报错，升级后全部正常）；
3. **配置切换**：部署方将 `[Report].Backend` 置为 `"doris"` 指向 Doris 数据源；反向切换（doris → mysql）随时可用；
4. **回滚**：Doris 表结构变更（加列、新表）向后兼容旧版本 api——旧 SQL 不引用新列即不受影响，无需随 api 回滚；api 查询层可独立回滚（门控退回 422）。本仓回滚 = 重建回旧 schema 聚合表 + 回改 Routine Load 映射（新列数据不再落库，已落库列对旧链路无影响）。

## 7. 风险与回滚

| 风险 | 说明 | 规避措施 |
|------|------|----------|
| AGGREGATE KEY 重建是有窗操作 | SWAP 换名虽原子，但 JOB 暂停窗口内分钟数据短暂滞后（恢复后下一周期自然补齐，分钟表允许 ≤1 分钟空洞）；Doris 版本不支持 `SWAP WITH` 时降级 RENAME 两步换名，秒级切换窗口内查询报"表不存在" | 务必低峰执行并先在测试环境演练；评估前端报表页瞬时错误展示 |
| 聚合行数膨胀 | 3 个低基数字段估 10–30 倍相关子集膨胀（一期评估） | 上线后监控行数与 JOB 耗时；降级预案：intent 维度降采样或缩短聚合表保留期 |
| Routine Load 不追溯存量消息 | 新明细列从映射上线后积累；新维度聚合同理（分钟表不回溯） | 与一期 MySQL 侧口径一致，部署文档需向用户显式说明 |
| 两仓列名/列序不一致 | Doris DDL、Routine Load 映射与 MySQL `db_ddl_report_mysql.sql`、log-reader 写入列清单必须同评审同 PR 落地（写入侧基准不变） | 防止"查询层引用不存在的列"这类本次要消除的错位再次发生 |
| 双链路并存口径 | 同一集群同时启用 mod_kafka→Doris 与 mod_log_mysql→MySQL 时两套报表天然不一致（log-reader 无持久化位点的既有短板） | 部署文档维持"单集群单落库形态"约束 |

## 8. 相关文档

- 设计依据：《数据报表-Doris 后端设计方案（v0.8）》（`迭代系统设计/v0.8/doris-report/数据报表-Doris后端设计方案.md`；本变更实现其 §4，§5 查询层改动由 ai-gateway-api 仓落地）
- 上游一期：《report 体现缓存/镜像/意图字段设计》（`迭代系统设计/report/report体现缓存镜像意图字段设计.md`，三组字段分期策略的权威来源）
- 表结构与字段语义：[../../design/TABLE_DESIGN.md](../../design/TABLE_DESIGN.md)
- Doris 端搭建指南：[../../user/HOWTO.md](../../user/HOWTO.md)
- 同仓既有变更：[../2026-08-26-update-to-new-pb/design-changes.md](../2026-08-26-update-to-new-pb/design-changes.md)（升级到新 PB 字段）
- api 侧变更记录（另一仓）：`ai-gateway-api/design-docs/modifications/2026-09-27-report-cache-mirror-intent-fields/`（一期 MySQL 全量）、`2026-09-29` 二期 dorisreport 补齐（随 api 仓落地）

---

*文档生成日期：2026-09-29*
