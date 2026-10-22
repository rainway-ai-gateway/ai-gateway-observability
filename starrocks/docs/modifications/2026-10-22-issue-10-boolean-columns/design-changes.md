# 明细表布尔字段 `ai_stream` / `is_trust_src_ip` 落 NULL 修复方案（issue #10）

- **日期**：2026-10-22
- **缺陷**：[rainway-ai-gateway/ai-gateway-observability#10](https://github.com/rainway-ai-gateway/ai-gateway-observability/issues/10)（P2：StarRocks 后端的"流式/非流式"维度整体不可用）
- **状态**：**已实施并回归通过**（2026-10-22，本机 WSL2 StarRocks 3.5.21 + Kafka 4.3.1）——§6 全部落地，实施与验证记录见 §10
- **结论**：本仓把 `ai_stream`（`starrocks/sqls/bfe_ai_request_log.sql:95`）与 `is_trust_src_ip`（`:19`）由 `TINYINT` 改为 `BOOLEAN`，与同表 `mirror_hit`（`:125`）/`ai_intent_cache_hit`（`:132`）保持一致。存量环境不能就地改类型（§4.5 实测 `Can not change TINYINT to BOOLEAN`），走"重建表 + 重建 Routine Load 重放 Kafka"路径（§4.3），旧表里已被写成 NULL 的历史值无法在 SR 内修复，只能靠重放原始消息重新解析

---

## 1. 背景

### 1.1 缺陷与影响

| 列 | 产品 DDL | `mod_kafka` 实际 payload | SR 落库结果 |
|----|----------|--------------------------|-------------|
| `ai_stream` | `TINYINT`（`bfe_ai_request_log.sql:95`） | JSON `true` / `false` | **NULL**（QA 环境重建后 25,598 行全 NULL） |
| `is_trust_src_ip` | `TINYINT`（`:19`） | JSON `true` / `false` | **NULL**（同上） |
| `mirror_hit` | `BOOLEAN`（`:125`） | JSON `true` / `false` | `1` / `0`（正常） |
| `ai_intent_cache_hit` | `BOOLEAN`（`:132`） | JSON `true` / `false` | `1` / `0`（正常） |

StarRocks 的 JSON 导入**只在目标列为 `BOOLEAN` 时**才把 JSON 布尔转成 `1/0`；目标列为 `TINYINT` 时**静默写 NULL**（不报错、不计入 `NumberFilteredRows`，§7 B 实测）。因此：

- `ai_stream` 是 `stream_requests` 指标与 `stream` 过滤条件的唯一来源（api 仓 `storage/starrocksreport/report.go`），全 NULL ⇒ 聚合 `stream_requests` 恒 0、`logs?stream=true` 与 `stream=false` 都查不到任何行（MV 里 `COALESCE(ai_stream, 0)`（`bfe_ai_metrics_1m.sql:27`）把 NULL 归到 0 维度，口径进一步失真）；
- `is_trust_src_ip` 只作明细列（报表 SQL 未直接消费），但明细导出与后续分析口径同样失真；
- 缺陷**在产品原生部署下即可复现**：官方样例 `starrocks/demo/normal_request.json` 就是 `"ai_stream": false` / `"is_trust_src_ip": false`。

### 1.2 根因链

1. 明细表把这两个布尔字段声明为 `TINYINT`（`bfe_ai_request_log.sql:19`、`:95`）；
2. Routine Load 对这两列做**裸列映射**（`bfe_ai_log_load_routine.sql:28`、`:88`），无任何类型转换表达式；
3. SR 的 JSON 导入只对 `BOOLEAN` 目标列做布尔→整数转换，`TINYINT` 目标列收到 JSON 布尔即写 NULL，且**不报错、不计坏行**；
4. 于是两列恒 NULL → `stream_requests` / `stream` 过滤 / 明细导出全部失真；
5. 同表 `mirror_hit`/`ai_intent_cache_hit` 声明为 `BOOLEAN`，在完全相同的加载路径下正常落 `1/0`——**反证**缺陷来自 DDL 类型选择，而非 SR 版本能力（§7 B）。

### 1.3 为什么现有集成测试没抓到

| 用例 | 覆盖方式 | 为何漏掉 |
|------|----------|----------|
| TC01（`sr01_starrocks_dock_test.go:85-92`） | 用 `information_schema` 断言关键列类型 | SR 把 `BOOLEAN` 呈现为 `tinyint(1)`（§7 F），断言里 `mirror_hit: "tinyint"` 之所以"通过"，正是因为两种类型在该视图下**长得一样**——TINYINT/BOOLEAN 混用在 TC01 下不可见 |
| TC02（`:137`，真实 Kafka 消费） | 断言 level 十列、限流三列、NULL 语义 | 断言清单里**没有** `ai_stream` / `is_trust_src_ip`（`grep ai_stream` 在测试目录 0 命中） |
| TC03（`:267`）/ `insertDemoRows`（`:369`） | `INSERT ... SELECT` 直插 | 绕开 JSON 解析路径，直插值天然正确 |

即：真实消费路径有覆盖，但**恰好没断言这两个布尔列**；类型断言又因 `information_schema` 的呈现方式而失效。两层缺口叠加，缺陷得以随 `v0.0.2-preview.1` 发布。

## 2. 目标与非目标

| # | 目标 | 验证标准 |
|---|------|----------|
| 1 | 两列在 SR 明细落 `0/1`，与 Doris/CH/MySQL 同口径 | 官方 demo payload（两字段均为 `false`）落库后两列非空且 `= 0`；`true` 变体落 `= 1` |
| 2 | 合法 JSON 布尔不再被静默丢弃 | 同上；`NumberFilteredRows=0` 且无非空缺失 |
| 3 | `stream_requests` 与 `logs?stream=` 过滤恢复 | 同窗口 SR 的 `SUM(ai_stream=1)` 与 Doris/MySQL 一致（> 0） |
| 4 | 存量环境有可执行、可回滚的升级路径 | §4.3/§4.4 步骤在测试实例演练通过 |
| 5 | 回归守卫落到真实消费路径 | §5 的断言在 starrocks-it 生效且能在旧 DDL 上失败（红→绿） |

**非目标**：

- 不改报表/查询层（api 仓 `storage/starrocksreport`）：MV 维度列类型仍是 `tinyint`（§7 E 实测），`SUM(CASE WHEN ai_stream=1 ...)`、`dimension` 白名单、`stream` 过滤 SQL 均无需改动；
- 不改 doris/、clickhouse/ 资产（两者分别靠 `TINYINT`/`UInt8` 就能吸收 JSON 布尔，见 #10 的跨仓对照表）；
- **不采用"报表侧 `COALESCE(ai_stream,0)` 兜底"**：那只是把 NULL 变成 0，`stream=false` 会吞掉"未上报"，与 Doris/CH 口径继续分叉；
- 不改 `api/depends_api/req_log.md` 的字段类型声明（`bool` 是对的，见 §4.1）；
- 不回灌 7 天窗口以外的历史（明细保留期本为 7 天）。

## 3. 修复方向比较

| # | 方向 | 影响面 | 评价 |
|---|------|--------|------|
| A | **本仓 SR 明细两列 `TINYINT` → `BOOLEAN`** | 本仓 1 份 DDL + 升级步骤 + 测试 | **推荐**：与同表另两列、与上游 `bool` 契约一致；对 JSON `true/false` 与 `0/1` 都成立（§7 A、A2），未来上游改口径也不回归；代价是 SR 侧必须重建表（§4.5） |
| B | 上游 `mod_kafka` 对 4 个布尔字段统一输出 `0/1` | bfe 仓 + 三仓 demo/文档/测试 | 可作为后续**契约收敛**，但不能替代 A：在本仓侧仍属"绕开 SR 类型语义"，且与 `req_log.md` 的 `bool` 声明相左；A 落地后 B 仍安全（`BOOLEAN ← 1/0` 正常，§7 A2），两者不冲突 |
| C | 加 `*_raw varchar` 暂存列 + 视图/生成列派生 | 明细 + MV + 全部读路径 | 不采纳：实测落 `'1'/'0'` 可救，但生成列/视图与 `mirror_hit` 等同名字段冲突（`Mapping column can not ref generated column`），读路径整体改造，成本最高 |
| D | Routine Load `COLUMNS` 内加类型转换表达式（保留 TINYINT） | 1 份 RL 脚本 | **未验证**：COLUMNS 上下文中源值是未定型 JSON，能否 `CAST` 成 TINYINT 取决于 SR 表达式求值细节；且 SR 3.5 对派生列自引用/同名列映射限制较多（见 `2026-10-02-starrocks-dock` §4.3）。作为 A 的备选调研项，不作为主方案 |

## 4. 推荐方案 A：两列改 `BOOLEAN`

### 4.1 依据

1. **SR 只在 `BOOLEAN` 目标列上做 JSON 布尔→`1/0` 的转换**（§7 A），`TINYINT` 一律 NULL——所以"DDL 声明"必须与"payload 是 JSON 布尔"匹配；
2. 同表 `mirror_hit`/`ai_intent_cache_hit` 已是 `BOOLEAN`，**同一加载路径**下正常落 `1`（§7 B），说明该写法在本链路已验证；
3. `BOOLEAN` **同时吸收 JSON `1/0`**（§7 A2），因此将来若上游按方向 B 改成 `0/1`，本仓不需要再改；
4. 与上游契约一致：`api/depends_api/req_log.md:132`（`ai_stream`）、`:172`（`is_trust_src_ip`）都声明为 `bool`，`BOOLEAN` 才是忠实映射，`TINYINT` 属降级声明；
5. **查询层零改动**：BOOLEAN 基列经 MV `COALESCE(ai_stream, 0)` 后，MV 维度列类型仍是 `tinyint`（§7 E 实测），`SUM(CASE WHEN ai_stream=1 ...)`、`ai_stream=0` 过滤、`dimension` 白名单全部照旧。

### 4.2 DDL 变更（`starrocks/sqls/bfe_ai_request_log.sql`）

```sql
-- 变更前
    is_trust_src_ip         TINYINT         COMMENT '是否可信源 IP',          -- :19
    ai_stream               TINYINT         COMMENT '是否流式：0=非流式, 1=流式', -- :95

-- 变更后
    is_trust_src_ip         BOOLEAN         COMMENT '是否可信源 IP（JSON 布尔，落 0/1）',
    ai_stream               BOOLEAN         COMMENT '是否流式：0=非流式, 1=流式（JSON 布尔，落 0/1）',
```

要点：

1. **列序/列名/列数不变**（仍 102 列），排序键、分区、分桶、压缩、动态分区属性均不动；
2. **保留 NULL 语义**：`NULL` = 未上报/未分类，`0` = 非流式。不要加 `NOT NULL DEFAULT 0`——那会把"未上报"折叠成"非流式"，与 Doris/CH 的 NULL 语义分叉（§7 G 实测：`true/false/1/0/NULL` 四种形态在 BOOLEAN 列上各自正确落库）；
3. **`BOOLEAN` 不出现在 `information_schema.columns` 里**（显示 `tinyint(1)`，§7 F）——验证与断言一律看 `DESC`（显示 `boolean`）或落库行为，不要看 `information_schema`；
4. Routine Load（`bfe_ai_log_load_routine.sql:28`、`:88`）**零改动**：裸列映射按列名绑定，类型转换由导入层按目标列类型完成。

### 4.3 存量升级：路径 A（重建 + Kafka 重放，推荐）

旧表里的 `ai_stream`/`is_trust_src_ip` **已经是 NULL，SR 内部无从修复**（信息在解析阶段就被丢弃了），因此"从旧表回灌"没有意义——回灌只会把 NULL 原样搬过去。正确做法是让 Kafka 里的原始消息**重新走一遍导入**：

```sql
-- 0) 记录基线（对账用）
SELECT COUNT(*), MIN(log_time), MAX(log_time), COUNT(ai_stream) FROM bfe_observability.bfe_ai_request_log;

-- 1) 停消费（同名作业在 STOP 后仍可能报 "Name ... already used"，可先改名留档：
--    ALTER ROUTINE LOAD FOR bfe_observability.bfe_ai_log_load RENAME TO bfe_ai_log_load_old;）
STOP ROUTINE LOAD FOR bfe_observability.bfe_ai_log_load;

-- 2) 拆对象（MV 绑定基表，必须一起重建）
DROP MATERIALIZED VIEW bfe_observability.bfe_ai_metrics_1m;
DROP TABLE bfe_observability.bfe_ai_request_log;
```

```bash
# 3) 用修复后的资产重建：建表 → 建 MV → 建 Routine Load（OFFSET_BEGINNING 重放）
bash setup.sh setup.conf
```

```sql
-- 4) 等重放追平后验收（RL State=RUNNING、位点不再推进时）
SELECT COUNT(*) total, COUNT(ai_stream) non_null, COUNT(is_trust_src_ip) trust_non_null,
       SUM(ai_stream = 1) stream1, SUM(ai_stream = 0) stream0
FROM bfe_observability.bfe_ai_request_log;
```

前提与注意：

1. **Kafka retention ≥ 期望的明细历史长度**。RL 脚本是 `OFFSET_BEGINNING`，新作业从 retention 起点重放；retention 若只有 3 天，SR 明细就只回到 3 天前（旧表本来也只有 7 天，接受即可，需在变更单里写明）；
2. **重放的历史消息不会因动态分区窗口丢行**：`p_init` 是 `VALUES LESS THAN (INIT_PARTITION_DATE)` 的兜底分区，早于该日期的 `log_time` 全部落入 `p_init`（§7 G 实测：`2026-08-24` 的行正常入库）。**但** `INIT_PARTITION_DATE` 之后、动态分区窗口（`start=-7`）之外的时间戳会因无对应分区成为错误行（§7 G 实测 2030 年的行被拒：`Insert has filtered data`）；重放前先确认 `INIT_PARTITION_DATE` 早于重放窗口起点，必要时把 `dynamic_partition.start` 临时放宽（`ALTER TABLE ... SET ('dynamic_partition.start'='-10')`；§7 G 实测属性可改，但**不会立即补建**更早的历史分区，需等调度按天创建）；
3. **重放期间 SR 报表缺数据**（分钟级到小时级，取决于消息量；QA 环境量级为每小时数千行）。选低峰执行，并在变更单注明窗口；
4. **MV 重建即全量回填**（既有 TC05 已验证），无需手工重算；
5. **重复风险可控**：表是新建的空表，重放不会产生重复行；此方案的代价是"重放窗口内的历史行按新类型重新生成"，收益是历史值也一起被修好。

### 4.4 备选升级：路径 B（平滑切换，零数据缺口）

适用于"SR 报表不允许停数小时"的环境。本机已实测 `SWAP WITH` 语法可用（§7 D），但 **Routine Load 与表 id 的绑定行为未验证**（本机无 Kafka），实施前必须在有 Kafka 的环境演练一遍：

1. 按新 DDL 建 `bfe_ai_request_log_v2`，并建 `bfe_ai_metrics_1m_v2`（基表不同，MV 必须独立）；
2. 以**临时消费组**（如 `starrocks_bfe_ai_log_backfill`）对 v2 建第二个 Routine Load，`OFFSET_BEGINNING` 重放，边追边用旧表对外服务；
3. 追平（v2 行数 ≈ 旧表 + 增量）后：`STOP` 两个 RL → `ALTER TABLE bfe_ai_request_log SWAP WITH bfe_ai_request_log_v2`（§7 D 实测：两表列名/列序一致、仅列类型不同时可正常交换）→ 删旧 MV、把 v2 的 MV 换名 → 以正式消费组建 RL 指向新表；
4. 风险点：`SWAP WITH` 只交换**表名**，RL 作业持有的是**表 id**——交换后原作业会继续写被改名的旧表，因此必须停旧作业并按新表重建；消费组位点需用 `kafka_partitions`/`kafka_offsets`（或换新消费组 + `OFFSET_BEGINNING` 到空表）精确续接，否则重放会造成明细重复（DUPLICATE KEY 不去重）。

### 4.5 不能就地 `ALTER`（实测）

```sql
ALTER TABLE bfe_ai_request_log MODIFY COLUMN ai_stream BOOLEAN;
-- ERROR 1064: Can not change TINYINT to BOOLEAN
```

SR 3.5.21 不支持 `TINYINT` → `BOOLEAN` 的 schema change（§7 C），因此 §4.3/§4.4 都是"重建式"升级。

### 4.6 跨引擎类型映射规则（同时写入 `TABLE_DESIGN.md`）

| 语义 | Doris | ClickHouse | StarRocks（修复后） |
|------|-------|------------|---------------------|
| JSON 布尔字段 | `TINYINT` | `UInt8` | **`BOOLEAN`** |
| 落库结果 | `0/1` | `0/1` | `0/1` |

规则：**SR 侧"payload 是 JSON 布尔"的列一律声明 `BOOLEAN`，`TINYINT` 只用于真正的整数语义**。三个引擎对 JSON 布尔的隐式转换能力不同（Doris/CH 会转，SR 只在 BOOLEAN 列转），跨仓 DDL 评审时按"语义"而非"字面类型"对齐，避免再次分叉。

## 5. 回归守卫

| # | 守卫 | 落在哪 | 效果 |
|---|------|--------|------|
| 1 | TC02（真实 Kafka 消费）补断言：三份 demo 都是 `"ai_stream": false` / `"is_trust_src_ip": false`，断言两列 `= 0` 且 `IS NOT NULL`；再加一条 `true` 变体（demo 在测试内已有改写先例：`timestamp`/`logid`）断言 `= 1` | `sr01_starrocks_dock_test.go` TC02 | 走真实 Routine Load 解析路径，旧 DDL 下必红、修复后变绿 |
| 2 | TC01 的"关键列类型"断言改用 `DESC`（或 `SHOW CREATE TABLE`）看 `boolean`，不要用 `information_schema` | `:85-92` | 消除"BOOLEAN 与 TINYINT 在视图下同形"的断言盲区（这正是本缺陷漏检的直接原因） |
| 3 | 落库守卫：demo payload 载入后断言 `COUNT(ai_stream) = COUNT(*)` | TC02 末段 | 任何"静默 NULL"回归都会立刻暴露，不依赖具体枚举值 |
| 4 | 发布后对账（SR 应 > 0 且与 Doris/MySQL 同窗口一致） | 变更单 §验收 | 跨后端等价性（SC2201-TC011 A-02、SC2201-TC019 的 stream 维） |

```sql
-- 修复后回归判据
SELECT COUNT(*) total, COUNT(ai_stream) non_null, SUM(ai_stream = 1) stream1
FROM bfe_observability.bfe_ai_request_log
WHERE log_time >= UTC_TIMESTAMP() - INTERVAL 1 HOUR;
```

> 对账注意：SR 会话时区为 CST(+08)，窗口计算必须用 `UTC_TIMESTAMP()`（#10 复现步骤 §4 的实测坑）。

## 6. 实施清单（全部已实施，见 §10）

| # | 文件 | 变更 | 状态 |
|---|------|------|------|
| 1 | `starrocks/sqls/bfe_ai_request_log.sql` | `:19`、`:95` 两列 `TINYINT` → `BOOLEAN`，注释补"JSON 布尔，落 0/1" | ✅ 已实施 |
| 2 | `starrocks/sqls/bfe_ai_log_load_routine.sql` | **零改动**（裸列映射按名绑定） | — |
| 3 | `starrocks/sqls/bfe_ai_metrics_1m.sql` | **零改动**（MV 维度列仍为 `tinyint`，§7 E） | — |
| 4 | `starrocks/docs/design/TABLE_DESIGN.md` | §1.2 新增"布尔字段一律 BOOLEAN"原则；§2.2 增加布尔字段行；§4 变更历史追加一行 | ✅ 已实施 |
| 5 | `starrocks/docs/user/HOWTO.md` | 新增 **§8 存量部署升级**（原"常见问题"顺延为 §9），文件结构树同步 | ✅ 已实施 |
| 6 | `starrocks/tests/integration/implementation/.../sr01_starrocks_dock_test.go` | TC01 类型断言改 `DESC`（新增 `common.ColumnTypesDesc`）；TC02 补布尔两列断言 + 流式变体 + 非空守卫；TC03 补 `ai_stream` 维度断言 | ✅ 已实施 |
| 7 | `starrocks/tests/integration/测试设计文档/.../TC-01`、`TC-02`、`TC-03`、`场景说明` | 断言清单与覆盖点同步 | ✅ 已实施 |
| 8 | 本文件 | 方案说明 + 实施记录 | ✅ |

## 7. 验证记录（2026-10-22，本机 WSL2 StarRocks 3.5.21-85fccf8 实测）

环境：单机 FE(9030/8030) + BE，库 `probe_issue10`/`probe_fix`（验证后已删除）。用 `_stream_load` 走与 Routine Load 相同的 `format=json` 解析路径。

| # | 验证项 | 结果 |
|---|--------|------|
| A | 类型矩阵（JSON `true`/`false`） | `BOOLEAN` → `1/0`；`TINYINT` → **`NULL`/`NULL`**；`VARCHAR` → `'1'/'0'` |
| A2 | JSON `1`/`0` 灌同一批列 | `BOOLEAN` → `1/0`、`TINYINT` → `1/0`（**BOOLEAN 两种 payload 都吃**） |
| B | 产品现状 DDL + 官方 `demo/normal_request.json` | `Status=Success`、`NumberTotalRows=1`、`NumberLoadedRows=1`、**`NumberFilteredRows=0`**；落库 `(mirror_hit=1, ai_stream=NULL, is_trust_src_ip=NULL, ai_intent_cache_hit=1)` ——**缺陷复现，且完全静默** |
| C | `ALTER TABLE ... MODIFY COLUMN ai_stream BOOLEAN` | **ERROR 1064: Can not change TINYINT to BOOLEAN** |
| D | 重建 + `ALTER TABLE a SWAP WITH b` | 成功；交换后 `DESC` 显示 `ai_stream boolean`，向交换后的表灌布尔 payload → `ai_stream=0` 正常落库 |
| E | 修复后全量 DDL（两列 BOOLEAN）建库建表建 MV | 建表/MV 全部成功；`DESC` 明细表四列均为 `boolean`；**MV 维度列 `ai_stream`/`mirror_hit` 仍为 `tinyint`**（查询层契约不变）；官方 demo payload 落库 `(ai_stream=0, is_trust_src_ip=0, mirror_hit=1, ai_intent_cache_hit=1)`；`COUNT(*)=1`、`COUNT(ai_stream)=1`、`stream0=1` |
| F | `information_schema.columns.COLUMN_TYPE` | `BOOLEAN` 列显示为 **`tinyint(1)`**（与 `TINYINT` 无法区分）→ 类型断言必须走 `DESC` |
| G | 补充事实 | ① `true/false/1/0/NULL` 四种形态在 BOOLEAN 列各自正确落库（NULL 保留）；② 早于 `INIT_PARTITION_DATE` 的 `log_time` 落 `p_init` 正常入库，晚于动态分区窗口的（如 2030 年）被拒 `Insert has filtered data`；③ `ALTER TABLE ... SET ('dynamic_partition.start'='-10')` 属性可改并生效，但**不会立即补建**更早的历史分区 |

**未覆盖**：

1. **§4.4 平滑切换的 RL 绑定行为**：`SWAP WITH` 与 `kafka_partitions`/`kafka_offsets` 续接语法未演练（真实消费路径本身已由 §10 的 TC02 覆盖；缺的是"重建后作业如何续接位点"这一运维细节）；
2. 旧表回灌公式在真实 102 列表上的执行（本机用 6 列探针表验证了等价表达式）；
3. 方向 B（上游改 `0/1`）需要 bfe 仓配合，不在本仓范围。

## 8. 风险与回滚

| # | 风险 | 对策 |
|---|------|------|
| 1 | 重放窗口长、期间 SR 报表缺数 | 选低峰执行并在变更单写明；必要时按 §4.4 平滑切换 |
| 2 | Kafka retention 短于期望历史 | 变更前查 `retention.ms`；接受明细历史缩短（旧表数据本就不可修复），或改用 §4.4 保留旧表 |
| 3 | 重放时间戳落在无分区区间 | 重放前核对 `INIT_PARTITION_DATE` 与 retention 窗口；必要时临时放宽 `dynamic_partition.start`（§7 G） |
| 4 | `BOOLEAN` 语义误用（有人把 NULL 当 0） | DDL 注释 + `TABLE_DESIGN.md` §4.6 规则；报表 SQL 保持 `ai_stream = 1 / = 0` 显式比较，不写 `!= 1` |
| 5 | 三引擎 DDL 类型继续分叉 | §4.6 类型映射规则进评审清单；starrocks-it 增加布尔列落库断言 |
| 6 | 回滚 | DDL 变更前旧表结构可由 git 恢复；重建式升级的"回滚"= 用旧 DDL 再重建一次 + 重放（或将 §4.4 的旧表 SWAP 回来），窗口内数据由 Kafka 重放补齐 |

## 9. 相关文档

- 缺陷：`https://github.com/rainway-ai-gateway/ai-gateway-observability/issues/10`
- 上游契约：`api/depends_api/req_log.md`（`:132` `ai_stream`、`:172` `is_trust_src_ip`，均为 `bool`）
- 本仓资产：`starrocks/sqls/bfe_ai_request_log.sql`、`starrocks/sqls/bfe_ai_log_load_routine.sql`、`starrocks/sqls/bfe_ai_metrics_1m.sql`
- 本仓文档：`starrocks/docs/design/TABLE_DESIGN.md`（§2.2 字段说明、§2.3 导入通道）、`starrocks/docs/user/HOWTO.md`
- 本仓先例：`starrocks/docs/modifications/2026-10-02-starrocks-dock/design-changes.md`（§4.3 Routine Load 方言修正、§5 TC01/TC02 覆盖点）、`doris/docs/modifications/2026-10-13-issue-1417-detail-unique-key-logid/design-changes.md`（重建式升级 + 换名的写法参照）
- 跨仓对照：`doris/sqls/bfe_ai_request_log.sql`（`TINYINT`）、`clickhouse/sqls/bfe_ai_request_log.sql:31,102`（`UInt8`）

## 10. 实施与验证记录（2026-10-22）

### 10.1 落地物

| 文件 | 实际改动 |
|------|----------|
| `starrocks/sqls/bfe_ai_request_log.sql` | `is_trust_src_ip`、`ai_stream` 改 `BOOLEAN`，列前补注释说明"JSON 布尔 + TINYINT 会静默落 NULL"；RL 与 MV 资产零改动 |
| `starrocks/tests/integration/common/starrocks.go` | 新增 `ColumnTypesDesc`（`DESC` 口径的列类型映射）——`information_schema` 无法区分 `BOOLEAN` 与 `TINYINT` |
| `.../sr01_starrocks_dock_test.go` | TC01：类型断言改 `DESC`，布尔四列断言 `boolean`；TC02：新增流式变体消息（`ai_stream`/`is_trust_src_ip` 改写为 `true`）、逐行断言扩到 20 列（含布尔两列，NULL 渲染为字面量 `NULL` 使断言失败）、新增两列非空守卫；TC03：新增 `ai_stream` 维度分布断言（`0:3`，锁定"BOOLEAN 基列 → tinyint 维度列"契约） |
| `starrocks/docs/design/TABLE_DESIGN.md` | §1.2 新增"布尔字段一律 `BOOLEAN`"设计原则；§2.2 新增布尔四列行；§4 变更历史追加本次记录 |
| `starrocks/docs/user/HOWTO.md` | 新增 **§8 存量部署升级**（升级前检查 / 操作步骤 / 注意事项），原"常见问题"顺延为 §9；文件结构树补本变更目录 |
| `starrocks/tests/integration/测试设计文档/...` | `TC-01`（类型断言改 `DESC`）、`TC-02`（4 条消息 + 20 列断言 + 非空守卫）、`TC-03`（`ai_stream` 维度）、`场景说明`（覆盖点）同步 |

### 10.2 回归结果（STARROCKS01 全场景，本机 WSL2）

`go test ./implementation/scenario-STARROCKS01-starrocks-dock/ -count=1 -v`（StarRocks 3.5.21-85fccf8 + Kafka 4.3.1 KRaft 单节点，真实 Kafka 生产 + Routine Load 消费）：

| 用例 | 结果 | 耗时 | 说明 |
|------|------|------|------|
| TC01 全新安装 schema | **PASS** | 1.3s | 布尔四列 `DESC` 均为 `boolean`；102/65 列、对象类型、重复应用报错文案不变 |
| TC02 消费打平（真实 Kafka） | **PASS** | 27.9s | 4 条消息（3 demo `false` + 1 变体 `true`）落库：`ai_stream`/`is_trust_src_ip` = `0/0/0/1`，非空守卫 0 缺失 |
| TC03 明细→聚合口径 | **PASS** | 147.0s | 24 指标 + 40 维关键子集不变；新增 `ai_stream` 维度 `0:3`（NULL→`COALESCE` 归 0） |
| TC04 存量升级 ALTER | **PASS** | 12.4s | `ADD COLUMN` / `ADD COLUMN IF NOT EXISTS` 负向断言 / MV 不携带新列均不变 |
| TC05 聚合 MV 重建 | **PASS** | 27.9s | 重建后自动全量回填、口径不变 |

### 10.3 红→绿证据

把 DDL 两列临时改回 `TINYINT`（其余保持不变）后重跑，两处守卫都按预期失败，说明断言确实能拦住本缺陷：

```
# TC01（类型守卫）
明细表列 ai_stream 类型 = "tinyint"，期望 "boolean"（DESC 口径）
明细表列 is_trust_src_ip 类型 = "tinyint"，期望 "boolean"（DESC 口径）

# TC02（落库守卫，真实 Kafka 链路）
got:  [... "1792676777" "2026-10-22 13:46:17" "0" "0.95" "NULL" "NULL"]
want: [... "1792676777" "2026-10-22 13:46:17" "0" "0.95" "1" "1"]
布尔列存在 NULL 行：ai_stream 缺失 4 行、is_trust_src_ip 缺失 4 行（均应为 0）
```

改回 `BOOLEAN` 后全场景 5/5 PASS。

### 10.4 环境注记（非资产问题）

- 本机 Kafka 的 KRaft 日志目录 `/home/zhang/kafka/kraft-combined-logs` 内有历史 root 属主文件，`zhang` 用户启动时会因 `Permission denied` 立即退出；本次验证改用独立 `log.dirs` 与 `advertised.listeners=PLAINTEXT://127.0.0.1:9092` 的副本配置启动（`/home/zhang/kafka/server-it.properties`）。既有环境修复方式是 `sudo chown -R zhang:zhang /home/zhang/kafka/kraft-combined-logs`。
- 测试 topic 只建不删（本机 9p 盘删除 topic 会使 broker 宕机，见 `common/kafka.go` 注释）：本次验证（含一次"红"实验与一次全场景回归）共留下 3 个 `bfe_ai_log_it_*` topic，命名唯一、体积极小；测试库由用例自身 `DROP DATABASE ... FORCE` 清理，无残留。