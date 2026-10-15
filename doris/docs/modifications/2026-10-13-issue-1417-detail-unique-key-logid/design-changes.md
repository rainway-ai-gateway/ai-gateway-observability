# Doris 明细表唯一键补请求级判别列 `logid`（issue #1417）设计变更

- **日期**：2026-10-13
- **缺陷**：[bfenetworks/bfe#1417](https://github.com/bfenetworks/bfe/issues/1417)（`bfe_ai_request_log` 秒级覆盖丢行；同轮 SC3201 计费审计发现）
- **上游方案**：`document-ai-gateway/迭代系统设计/v0.8/bfe_ai_request_log秒级覆盖问题/bfe_ai_request_log秒级覆盖问题修复方案.md`
- **已交付的另外三仓**：`bfe@89defa14`（pb `logid` 由恒 0 改为逐请求唯一非 0）、`ai-gateway-api@64f278e`（MySQL 侧 `uk_dedup` 补 `logid` + 收紧 `NOT NULL DEFAULT 0`）、`log-reader@2855edc`（写入侧零代码改动 + 文档/测试对齐）
- **状态**：方案待评审（本文件只做设计；§5 列出的落地物尚未实施）

---

## 1. 背景

### 1.1 缺陷：明细表唯一键缺请求级判别列

`bfe_ai_request_log` 为 **UNIQUE KEY** 模型，主键/唯一键为四列 `(hostid, log_time, ai_apikey_id, ai_requested_model)`（`sqls/bfe_ai_request_log.sql:144`），其中 `log_time` 是 Routine Load 由 pb `timestamp`（Unix **秒**）转换来的秒级 `DATETIME`。Doris UNIQUE KEY 模型对同键行做合并（只保留一行），因此：

> 同一秒内、同一 API Key、同一模型的 n 个并发请求，在明细表里只剩 **1 行**——丢率恰为 (n−1)/n。

生产实测（2026-10-12 13:00–13:59 CST，pb3 解码 2113 行 vs MySQL 明细 1827 行）：丢 286 行 = **13.5%**；同秒 1/2/3-4 并发的丢率 1%/50%/67%。设计上用于区分请求的 `logid` 列（`sqls/bfe_ai_request_log.sql:9`）在该窗口内**恒为 0**（BFE 侧十进制解析十六进制 logid 且吞掉错误——已在 `bfe@89defa14` 修复），判别能力从未兑现。被覆盖行的 tokens/cost 从求和中整体消失，**分钟聚合表同样低估**（`bfe_ai_metrics_1m_job` 按分钟窗口 `SUM`）。

### 1.2 本仓库的位置与两侧一致性

| 层 | 位置 | 与本缺陷的关系 |
|----|------|---------------|
| 明细表 DDL | `sqls/bfe_ai_request_log.sql`（`:144` 唯一键、`:145-158` 分区与动态分区属性） | 唯一键定义所在；**本变更的唯一改动点** |
| Routine Load | `sqls/bfe_ai_log_load_routine.sql`（`COLUMNS(...)` 第 1 项即 `logid`，见 `:5`） | 映射已含 `logid` → **零改动** |
| 聚合表与 JOB | `sqls/bfe_ai_metrics_1m.sql`、`sqls/bfe_ai_metrics_1m_job.sql` | `logid` 不是聚合维度 → **不受影响** |
| 存量升级 | `sqls/upgrade/<date>-<slug>/*.sql` + `docs/user/HOWTO.md` §11 | 本次需新增一个**重建式**升级脚本（键变更不能在线 ALTER） |
| 文档 | `docs/design/TABLE_DESIGN.md`（§1 粒度标注"单次请求"、§2.1 主键四列、§2.2 `logid` 说明） | 需按新键修订 |
| 测试 | `tests/integration/`（scenario-DORIS01，含 TC-04 存量升级 ALTER / TC-05 聚合表重建换名） | 需补"明细表唯一键重建"用例 |

**两侧一致性**：MySQL 侧（轻量形态）自 `ai-gateway-api@64f278e` 起唯一键为五列；本仓及三仓文档此前互称"两侧唯一键完全一致"。Doris 未同改期间该承诺失效——**本变更即恢复一致**。

## 2. 目标与非目标

| # | 目标 | 验证标准 |
|---|------|----------|
| 1 | 明细表唯一键纳入 `logid`（第 5 列，键仍为列前缀） | `DESC bfe_ai_request_log ALL` 显示 5 个 KEY 列、`logid` 为其中之一 |
| 2 | `logid` 收紧为 `NOT NULL`（Doris KEY 列的硬约束） | 键列均 `Null=No`；存量行由升级脚本 `IFNULL(logid,0)` 兜底 |
| 3 | 存量环境有可执行的重建式升级脚本 | `sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/` 落地并在测试环境演练通过 |
| 4 | 文档与新语义一致 | `TABLE_DESIGN.md`、`HOWTO.md` 不再出现"主键四列""粒度=单次请求（秒级覆盖隐含）"等旧表述 |
| 5 | 集成测试覆盖"同秒并发不丢行"与"存量重建" | DORIS01 新增用例（同一秒同 Key 同模型 2 条 → 2 行；重建脚本演练） |

**非目标**：

- 不改 Routine Load 映射（已含 `logid`）、不改聚合表与 INSERT JOB、不改其他列的类型/顺序；
- 不做历史数据修复：存量行 `logid` 本身为 0（BFE 从未写出非 0 值），**重建后它们仍是 0、仍在同秒同 Key 同模型下互相折叠**——这是历史事实，无法回溯（与 MySQL 侧结论一致）；
- 不引入新配置项；不改分区分桶与压缩属性；
- 不采用 DUPLICATE KEY 替代方案（见 §7 备选方向）。

## 3. 变更总览

| # | 变更点 | 文件 | 方式 |
|---|--------|------|------|
| 1 | 明细表唯一键加 `logid`，`logid` 收紧 `NOT NULL` | `sqls/bfe_ai_request_log.sql` | **整表重建**（KEY 列变更不支持在线 schema change） |
| 2 | 存量升级脚本 | `sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql`（新增） | 建新表 + 回灌 + `REPLACE WITH TABLE`（swap） |
| 3 | 升级操作步骤 | `docs/user/HOWTO.md`（文件结构树 + 新增章节） | 文档 |
| 4 | 表设计说明 | `docs/design/TABLE_DESIGN.md` §1 / §2.1 / §2.2 | 文档 |
| 5 | 集成测试用例与设计文档 | `tests/integration/implementation/scenario-DORIS01-.../`、`tests/integration/测试设计文档/scenario-DORIS01-.../` | 新增 TC（需真实 Doris，CI 执行） |
| 6 | Routine Load / 聚合表 / JOB | — | **无需改动** |

## 4. 详细设计

### 4.1 明细表唯一键改造（必须重建，不能在线 ALTER）

**变更前**（`sqls/bfe_ai_request_log.sql:9,144`）

```sql
    logid                   BIGINT          COMMENT 'BFE 请求唯一标识',
...
UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model)
```

**变更后**

```sql
    logid                   BIGINT NOT NULL COMMENT 'BFE 请求唯一标识（逐请求唯一、非 0；seed bfe#1417）',
...
UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model, logid)
```

要点：

1. **键仍为列前缀**：Doris 要求 KEY 列为表定义的前若干列；`logid` 恰为第 5 列（前 4 列即原键），因此**无需调整列序**——这是本次改造能低成本落地的关键；
2. **KEY 列天然 NOT NULL**：Doris 的 KEY 列不允许 NULL，`logid` 需显式收紧为 `NOT NULL`。写入侧（Routine Load 映射）**始终提供** `logid`（pb 必填 `uint64`），缺省不会出现；`NOT NULL` 仅为键约束。
3. **不能在线 ALTER**：Doris 的 schema change 支持加列/改类型/加 rollup，但**不支持修改 KEY 列集合**；同仓先例是聚合表 KEY 维度变更走"整表重建 + `REPLACE WITH TABLE ... PROPERTIES('swap'='true')` 原子换名"（见 `docs/modifications/2026-09-29-report-cache-mirror-intent-doris-alignment/design-changes.md` §4.3），本次沿用同一模式。
4. **分区/分桶/压缩属性不变**：`PARTITION BY RANGE(log_time)` + 动态分区（保留前 7 天 + 后 3 天）+ `DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32` + zstd——新表按同属性建（`sqls/bfe_ai_request_log.sql:145-158`）。

### 4.2 存量升级脚本（重建式，每环境执行一次）

新增 `sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql`（与既有升级脚本同风格：`USE ${DORIS_DATABASE};` 变量化、头部注释引用本变更说明）。步骤：

```sql
-- 1) 按新 schema 建新表（键含 logid；分区/分桶/压缩/动态分区属性与旧表一致）
CREATE TABLE bfe_ai_request_log_v2 ( ... logid BIGINT NOT NULL ... )
UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model, logid)
PARTITION BY RANGE(log_time) ( PARTITION p_init VALUES LESS THAN ('${INIT_PARTITION_DATE}') )
DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32
PROPERTIES ( "replication_num"="1", "dynamic_partition.enable"="true",
             "dynamic_partition.time_unit"="DAY", "dynamic_partition.start"="-7",
             "dynamic_partition.end"="3", "dynamic_partition.prefix"="p",
             "dynamic_partition.buckets"="32", "compression"="zstd" );

-- 2) 回灌近期窗口（明细按天保留，7 天内数据量有界、成本可控）
--    注意 IFNULL：存量行 logid 可能为 NULL（历史写入侧缺陷），新键要求非 NULL；
--    回灌为 0 后与旧四元组语义等价（0 参与键比较），不产生重复行
INSERT INTO bfe_ai_request_log_v2 SELECT ..., IFNULL(logid, 0) AS logid, ... FROM bfe_ai_request_log;

-- 3) 暂停 Routine Load，等待在途消息落定（避免换名瞬间的写入缺口）
PAUSE ROUTINE LOAD FOR bfe_ai_log_load;   -- 目标版本语法：STOP ROUTINE LOAD FOR ...
-- 等待 SHOW ROUTINE LOAD ... State=PAUSED 且 Kafka 消费位点追平

-- 4) 补灌步骤 2 之后到达的行（同上 INSERT SELECT，按 log_time 增量过滤）

-- 5) 原子换名（Doris 3.0.8 语法；不支持时降级为两次 RENAME，秒级窗口可接受）
ALTER TABLE bfe_ai_request_log REPLACE WITH TABLE bfe_ai_request_log_v2 PROPERTIES('swap' = 'true');

-- 6) 恢复消费（换名后同名指向新表，映射无需改动）
RESUME ROUTINE LOAD FOR bfe_ai_log_load;

-- 7) 验证通过后清理（此时 v2 名指向旧表）
-- DROP TABLE bfe_ai_request_log_v2;
```

**执行次数约束与幂等**：本脚本为非幂等的重建流程，每环境执行一次；重复执行的保护手段是"步骤 1 建表名带 `_v2` 后缀 + `DROP TABLE IF EXISTS` 前置清理"，与既有升级脚本（Doris 3.0 不支持 `IF NOT EXISTS`，靠一次性执行）口径一致。

**演练要求**：Doris 版本差异（`REPLACE WITH TABLE ... PROPERTIES('swap')`、`PAUSE/RESUME ROUTINE LOAD` 语法、KEY 列上 `NOT NULL` 的校验强度）需在测试环境先演练——DORIS01 既有 TC-05 已覆盖"聚合表重建换名"路径，本次新增用例沿用同一套 harness。

### 4.3 Routine Load：零改动

`sqls/bfe_ai_log_load_routine.sql` 的 `COLUMNS(...)` 第 1 项即 `logid`（`:5`），映射与列清单无关，**唯一键变更不影响导入**。换名采用 `REPLACE WITH TABLE`（同名交换），因此：

- 存量 load job 不需要重建映射；步骤 3/6 的 `PAUSE`/`RESUME` 仅为消除换名窗口的写入竞争；
- 恢复后建议核对 `SHOW ROUTINE LOAD FOR bfe_ai_log_load` 的 `State=RUNNING`、消费位点继续前进，并用 §4.6 的验证查询确认新行落库。

### 4.4 聚合表与 INSERT JOB：不受影响

`bfe_ai_metrics_1m` 的 AGGREGATE KEY 维度不含 `logid`（`sqls/bfe_ai_metrics_1m.sql`），`bfe_ai_metrics_1m_job` 按分钟窗口从明细 `SUM`——明细不再丢行后，**聚合口径随之修正**（原来的低估消失），但 DDL 与 JOB SQL 均无需改动。

### 4.5 历史数据与新键语义

| 行 | 新键下的行为 | 与变更前比较 |
|----|-------------|-------------|
| 存量行（`logid` 为 0/NULL→回灌为 0） | `0` 参与键比较 → 同秒同 Key 同模型仍只留 1 行 | **逐行等价**（不产生重复行、不改变历史） |
| BFE 修复后写入的新行（`logid` 唯一非 0） | 同秒并发请求各自成行；只有同一请求重放（同 `logid`）才折叠 | **缺陷消除** |
| Kafka 存量消息重放（retention 内） | 按其自带 `logid` 归键：旧消息 `logid=0` → 与存量行等价；新消息 → 各自成行 | 与 MySQL 侧一致 |

### 4.6 文档修订

| 文件 | 位置 | 现状 | 改为 |
|------|------|------|------|
| `docs/design/TABLE_DESIGN.md` | `:13`（§1 总体设计表） | `bfe_ai_request_log \| UNIQUE KEY（明细） \| 单次请求 \| ...` | 粒度列改为"单次请求（键含请求级判别列 `logid`）"，并补一句说明：修复前四列键退化为"秒 + Key + 模型"，同秒并发只留 1 行（bfe#1417） |
| 同上 | `:29`（§2.1 主键） | `(hostid, log_time, ai_apikey_id, ai_requested_model)` | 五列（末位 `logid`，`NOT NULL`，第 5 列即键前缀末尾） |
| 同上 | `:44`（§2.2 `logid` 行） | `BFE 请求唯一标识` | 补口径：BFE 侧十进制值（≤2^63−1）、逐请求唯一非 0、与访问日志字符串 logid 逐值恒等；修复前恒 0 |
| `docs/user/HOWTO.md` | `:41-43`（文件结构树） | 仅列 `2026-09-29-report-cache-mirror-intent/` | 追加本次 `2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql` 说明 |
| 同上 | `:222`（§11 存量部署升级） | 二期（缓存/镜像/意图）三步 | 追加"§11.4 明细表唯一键重建（bfe#1417）"：建 v2 → 回灌 → PAUSE/REPLACE/RESUME → 验证与清理 |

## 5. 涉及文件清单

| 文件 | 变更 | 状态 |
|------|------|------|
| `doris/sqls/bfe_ai_request_log.sql` | 唯一键加 `logid`；`logid` 收紧 `NOT NULL` | 待实施 |
| `doris/sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql` | **新增**（§4.2 重建流程） | 待实施 |
| `doris/docs/user/HOWTO.md` | 文件结构树 + §11.4 新增章节 | 待实施 |
| `doris/docs/design/TABLE_DESIGN.md` | §1 / §2.1 / §2.2 三处修订 | 待实施 |
| `doris/tests/integration/测试设计文档/scenario-DORIS01-报表缓存镜像意图字段/TC-06-明细表唯一键重建.md` | **新增**用例文档（旧四列键缺陷复现 → 重建后五列键/NULL 归 0/新写入各自成行） | ✅ 已实施 |
| `doris/tests/integration/implementation/scenario-DORIS01-.../doris01_report_cache_mirror_intent_test.go` | 新增 `TestTC06DetailUniqueKeyRebuild` | ✅ 已实施并在本机 WSL2 Doris 3.0.8 实测通过 |
| 本文件 | 变更说明 | ✅ |

> 说明：本仓库的集成测试需要真实 Doris 实例；本机环境未提供，故 §5 的测试项按"CI 执行"登记，落地时在具备 Doris 的环境跑通（既有 DORIS01 用例同样如此运行）。

## 6. 兼容性与发布顺序

| 项 | 结论 |
|----|------|
| 与 `bfe` 的关系 | **必须先有 BFE 修复**（`89defa14`，已上线）——否则新行 `logid` 仍为 0，重建后行为与现在完全相同（无收益、也无损） |
| 与 `ai-gateway-api` / MySQL 侧的关系 | **无依赖**（两条链路独立库表）；本变更完成后，两侧唯一键恢复"完全一致" |
| 与 `log-reader` 的关系 | 无依赖（log-reader 写的是 MySQL 链路；Doris 链路走 `mod_kafka` → Routine Load） |
| 发布顺序 | 1) BFE（已完成）→ 2) Doris 升级脚本（本变更）→ 3) 两侧文档口径恢复一致 |
| 变更期间数据可见性 | `REPLACE WITH TABLE` 为原子换名、查询不断流；`PAUSE` 窗口只在"回灌 + 换名"期间，时长按明细 7 天窗口的数据量评估（分钟级） |
| 消费侧影响 | Grafana / 报表查询按列读取，键变更不改变列契约，**无需改造** |

## 7. 风险与回滚

| # | 风险 | 对策 |
|---|------|------|
| 1 | 换名窗口的数据缺口 | `PAUSE` → 回灌 → `REPLACE` → `RESUME` 顺序执行，窗口内 Kafka 消息留存（不丢）；恢复后按 `log_time` 核对无缺口 |
| 2 | Doris 版本语法/约束差异 | `REPLACE WITH TABLE ... PROPERTIES('swap')`、`PAUSE/RESUME ROUTINE LOAD`、KEY 列 `NOT NULL` 校验均在测试环境演练（DORIS01 harness）；`REPLACE` 不可用时降级两次 `RENAME`（秒级窗口） |
| 3 | 存量行 `logid` 为 NULL | 回灌用 `IFNULL(logid, 0)`；并在升级前统计 `SELECT COUNT(*) FROM bfe_ai_request_log WHERE logid IS NULL`（预期为 0，写入侧恒提供） |
| 4 | 历史行仍无判别力 | 已声明为不可回溯（旧消息与旧行 `logid` 均为 0）；对账统一以 pb3 原始日志为准 |
| 5 | 回灌耗时/资源 | 明细按天保留（前 7 天 + 后 3 天），窗口有界；低峰窗口执行，必要时只回灌近 3 天（历史仅服务查询，可接受更短窗口） |
| 6 | 回滚 | **保留旧表**即可：换名后旧表仍在（名为 `bfe_ai_request_log_v2`），再执行一次 `REPLACE WITH TABLE` 即回退；或按四列键重建后回灌 |

**备选方向（已评估，不采纳）**：把 UNIQUE KEY 改为 DUPLICATE KEY（append-only）。表面看可让覆盖丢失消失，但① Doris 改键模型同样必须整表重建，迁移成本与加键相同；② Routine Load 为 at-least-once，且 `bfe_ai_log_load_routine.sql` 显式配了 `OFFSET_BEGINNING`（job 重建即重消费历史），失去去重后每次重放都会复制历史行；③ **本仓聚合 JOB 无 DELETE 窗口**（`sqls/bfe_ai_metrics_1m_job.sql` 仅 `INSERT ... SELECT SUM(...)`），明细重复会直接穿透到聚合、把"丢行"换成"重复行"；④ 与 MySQL 侧（覆盖语义）彻底分叉。详见总方案 §7。

## 8. 相关文档

- 缺陷与总方案：`document-ai-gateway/迭代系统设计/v0.8/bfe_ai_request_log秒级覆盖问题/bfe_ai_request_log秒级覆盖问题修复方案.md`
- 同轮其他仓库变更：
  - `bfe/docs/zh_cn/modifications/2026-10-13-issue-1417-logid-dedup-fix/design-changes.md`
  - `ai-gateway-api/design-docs/modifications/2026-10-13-issue-1417-report-detail-dedup-key-logid/design-changes.md`（MySQL 侧五列键与迁移 runbook）
  - `log-reader/doc/modifications/2026-10-13-issue-1417-dedup-key-logid/design-changes.md`
- 本仓先例：`docs/modifications/2026-09-29-report-cache-mirror-intent-doris-alignment/design-changes.md`（§4.3 聚合表重建 + `REPLACE WITH TABLE` 换名）、`docs/modifications/2026-10-01-routine-load-offset-beginning/design-changes.md`
- 本仓落地参照：`sqls/upgrade/2026-09-29-report-cache-mirror-intent/bfe_ai_request_log_alter.sql`（升级脚本风格）、`docs/user/HOWTO.md` §11（存量部署升级）、`docs/design/TABLE_DESIGN.md` §2（明细表设计）

## 9. 实施与验证记录（2026-10-13，本机 WSL2 Doris 3.0.8 实测）

**实施物（§5 全部完成）**：DDL（`sqls/bfe_ai_request_log.sql`：`logid BIGINT NOT NULL` + 五列唯一键）、升级脚本（`sqls/upgrade/2026-10-13-issue-1417-detail-unique-key-logid/bfe_ai_request_log_rebuild.sql`）、HOWTO（文件结构树 + §11 总览第 4 行 + 新增 **§11.5 明细表唯一键重建**）、TABLE_DESIGN（§1 粒度 / §2.1 主键 / §2.2 `logid`）、DORIS01（`TC-06-明细表唯一键重建.md` + `TestTC06DetailUniqueKeyRebuild`）。

**与本文设计的偏差（以实际实现为准）**：

1. **升级脚本不建 `_v2`**：沿用 §11.3 聚合表重建的做法——`_v2` 由新 DDL 做表名替换生成（HOWTO §11.5 步骤 5.2 的 `sed`；测试内为等价字符串替换），脚本只做"**回灌 + 原子换名**"。这样列清单保持单一来源（`sqls/bfe_ai_request_log.sql`），避免 DDL 副本漂移。脚本头部注释与 HOWTO 均已写明该前置。
2. **回灌不恢复已丢行（实测确认，重要）**：旧表里"同秒同 Key 同模型"的多行在**写入时即已被合并**，回灌只能带走幸存的那 1 行。因此"重建后并发行数 = n"**只对重建后新写入的数据成立**；§4.5 的"存量行逐行等价、历史不可回溯"由此得到实测确认。

**实测证据（真实 Doris 3.0.8，临时库 `it1417_doris`，验证后已 DROP）**：

| # | 验证项 | 结果 |
|---|--------|------|
| 1 | 以 **HEAD（旧）DDL** 建表，写入同秒 + 同 Key + 同模型、不同 `logid` 的 2 行 | `COUNT(*) = 1` ——**Doris 侧同缺陷复现**（与 MySQL 侧同源） |
| 2 | 执行升级脚本（回灌 + `REPLACE WITH TABLE … PROPERTIES('swap'='true')`） | 成功；`logid` 变 `NOT NULL`、`UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model, logid)` |
| 3 | NULL `logid` 行（历史缺陷形态） | 回灌后 `logid = 0`（`IFNULL` 兜底）；全表 `logid IS NULL` 行数 = 0 |
| 4 | 重建后写入同一批（同秒 + 同 Key + 同模型、`logid` 1001/1002） | `COUNT(*) = 2` ——缺陷消除 |
| 5 | 换名后删除旧表 | `DROP TABLE bfe_ai_request_log_v2` 成功，新表名查询正常 |
| 6 | **DORIS01 全场景回归** | **TC-01..TC-06 全 PASS**（`go test ./implementation/scenario-DORIS01-report-cache-mirror-intent/`，43.8s） |

**过程中踩到的坑（脚本已修正，留作提醒）**：`INSERT … SELECT` 的列清单必须与目标表列序**完全一致**。最初按"DDL 行号范围"提取列名，因本轮给 DDL 新增了注释行导致范围偏移、漏掉末列 `ai_intent_questions_version`，Doris 报 `insert into cols should be corresponding to the query output`。修正后以 `information_schema` 的权威列序为准（**102 列**）——**后续明细表加列时，本脚本的列清单需同步更新**。

**未覆盖**：Routine Load 的 `PAUSE`/`RESUME` 与 Kafka 实际消费链路（本机无 Kafka）。存量环境首次执行建议先在测试实例完整走一遍 §11.5（含 5.3/5.5 的消费暂停与恢复）。

**环境提示（非本仓问题）**：本机 Doris 需以 **root** 运行 `start-all.sh`（BE 启动要求 `swapoff`/`vm.max_map_count`，且需先 `unset http_proxy`）；在 Git Bash 中执行须加 `MSYS_NO_PATHCONV=1`，否则 `/mnt/d/...` 会被转成 Windows 路径。