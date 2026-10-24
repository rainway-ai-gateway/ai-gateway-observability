# StarRocks 对接（报表存储新引擎）设计变更

- **日期**：2026-10-02
- **变更范围**：ai-gateway-observability 新增 `starrocks/` 资产目录，本次变更全部交付：目录骨架、本变更记录、四份 SQL 资产（§3 #2–#5）、部署/清理脚本与配置（#6）、demo 样例（#7）、HOWTO 与 TABLE_DESIGN 文档（#8）、starrocks-it 集成测试（#9，`tests/integration/`，TC01–TC05 在真实 StarRocks 3.5.21 环境全绿，§5）；数仓侧冒烟验证已在真实 StarRocks 3.5.21（WSL2 单机 FE+BE）通过（§5）。api 查询层 `storage/starrocksreport` 属后续变更，另行落地
- **变更原因**：v0.8 数据统计在 Doris 之外新增 ClickHouse / StarRocks 两个可选报表存储引擎（客户现场数仓存量以二者为主，Doris 单选项构成标案准入障碍）。StarRocks 具备 Routine Load 但无 Doris 式 `CREATE JOB`，分钟聚合改用异步物化视图重新落地，与 Doris 侧资产平行
- **设计依据**：《数据报表-ClickHouse 与 StarRocks 对接设计方案（v0.8）》（本变更实现其 §4 StarRocks 侧；ClickHouse 侧已实现于 `clickhouse/` 目录，见 [2026-10-01-clickhouse-dock](../../clickhouse/docs/modifications/2026-10-01-clickhouse-dock/design-changes.md)）

---

## 1. 背景

既有 Doris 链路（本仓 `doris/`）：Kafka（topic `bfe_ai_log`）→ Routine Load（COLUMNS 内完成 JSON 打平 + UTC 墙钟换算）→ 明细表 `bfe_ai_request_log`（UNIQUE KEY，动态分区 7 天）→ `CREATE JOB` 每分钟聚合 → `bfe_ai_metrics_1m`（AGGREGATE KEY，40 维 + 24 指标）。

StarRocks 3.5.21 的对应机制：

| Doris 机制 | StarRocks 等价物 |
|------------|------------------|
| `UNIQUE KEY` 明细表 | `DUPLICATE KEY` 明细表（排序键四列一致） |
| `CREATE JOB ... ON SCHEDULE EVERY 1 MINUTE`（分钟聚合 INSERT SELECT） | 异步物化视图 `CREATE MATERIALIZED VIEW ... REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)`，MV 名直接取 `bfe_ai_metrics_1m` |
| Routine Load COLUMNS 映射 | Routine Load COLUMNS 映射（语法高度同构，原样平移） |
| 动态分区 `start=-7`（保留期） | 明细表动态分区 `start=-7` + MV `partition_ttl=7 DAY` |

字段契约不变：`api/depends_api/req_log.md`（Kafka JSON 字段）继续是唯一权威，明细表与 Doris 逐列同名同型（102 列），聚合口径 40 维 + 24 指标逐列对齐。

## 2. 目标与非目标

**目标**：

1. 新增 `starrocks/` 目录，结构平移 `doris/` 既定模式（`sqls/` + `demo/` + `docs/{user,design,modifications}/` + setup/cleanup 脚本）；
2. 四份 SQL 资产按 §4 落地：建库、明细表（DUPLICATE KEY）、分钟聚合异步 MV、Routine Load；
3. 导入与聚合全链路为库内声明式机制，**不引入常驻消费服务**，保持本仓"声明式资产"定位；
4. 口径与 Doris 逐条对齐：UTC 墙钟、apikeytags 十列打平、限流三列打平、聚合 40 维 + 24 指标、错误/限流/认证拒绝计数语义、7 天保留期。

**非目标**：

- 不改 `doris/`、`clickhouse/`、`grafana/` 任何既有资产（Grafana 维持 Doris 形态，本期不交付 StarRocks 数据源与 dashboard）；
- 不改 log-reader / BFE / ai-gateway-api（查询层 `storage/starrocksreport` 属 api 仓，另行落地）；
- 不做历史数据迁移：新消费组（`starrocks_bfe_ai_log`）从分区起始加载既有 Kafka 消息，更早历史不回灌。

## 3. 变更总览

| # | 变更 | 涉及文件 | 方式 |
|---|------|----------|------|
| 1 | 目录骨架 | `starrocks/{sqls,demo,docs/user,docs/design,docs/modifications}/` | 本次交付 |
| 2 | 建库 | `sqls/bfe_observability.sql` | `CREATE DATABASE`，sed 变量替换库名（同 doris 版） |
| 3 | 明细表 | `sqls/bfe_ai_request_log.sql` | `DUPLICATE KEY`，动态分区按天，zstd 压缩；列契约与 Doris 同名同列（102 列）；**5 个嵌套列按 SR 能力以 VARCHAR 承载 JSON 文本**（§4.3） |
| 4 | 分钟聚合异步 MV | `sqls/bfe_ai_metrics_1m.sql` | MV 名即 `bfe_ai_metrics_1m`；`REFRESH ASYNC EVERY 1 MINUTE`；分区列 `ts_day` + `partition_ttl=7 DAY`；rate_limit_hits 经 parse_json 还原数组取长度（§4.3） |
| 5 | Routine Load | `sqls/bfe_ai_log_load_routine.sql` | doris 版同构 + SR 方言改写：level/限流打平用 `get_json_string`（§4.3）；消费组 `starrocks_bfe_ai_log` 独立；`OFFSET_BEGINNING` |
| 6 | 部署/清理脚本与配置 | `setup.sh` / `cleanup.sh` / `setup.conf` / `setup_test.conf` | mysql 客户端直连 FE 9030（sed 变量替换，同 doris 版模式）；**必带 `--skip-comments`**（§4.5） |
| 7 | 样例消息 | `demo/` | 拷贝 `doris/demo/` 三个样例（normal / rate_limit / auth_reject），目录自包含 |
| 8 | 文档 | `docs/user/HOWTO.md`、`docs/design/TABLE_DESIGN.md` | 随 SQL 资产落地时编写 |
| 9 | 集成测试 | `tests/integration/`（Go module `starrocks-it`） | 仿 `clickhouse/tests/integration/`：common harness（MySQL 协议直连、SQL 资产变量替换、MV/RL 状态辅助）+ kafka-go 生产辅助 + 场景 STARROCKS01 五个用例（TC01–TC05）+ 测试设计文档 6 份 |

## 4. 详细设计

### 4.1 明细表：`DUPLICATE KEY` 对齐 Doris `UNIQUE KEY`

设计稿 §4.2 已定稿该决策，不再重开：BFE 访问日志 append-only、`logid` 天然唯一，Doris UNIQUE KEY 的去重语义无实际作用；`DUPLICATE KEY` 是 StarRocks 写入最快的模型，排序键/前缀索引四列与 Doris 一致 `(hostid, log_time, ai_apikey_id, ai_requested_model)`，查询行为等价。表契约只约束列，不约束引擎模型。

其余属性与 Doris 版对齐：`PARTITION BY RANGE(log_time)` 动态分区按天（`start=-7` / `end=3` / `buckets=32`）、`DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32`、`replication_num=1`、`compression=zstd`。

### 4.2 STRUCT 语法差异（实测记录，已随 §4.3 决策闭环）

Doris 的 STRUCT 字段声明为冒号风格 `STRUCT<k: VARCHAR(10)>`，StarRocks 为空格风格 `STRUCT<k VARCHAR(10)>`，冒号写法在 SR 3.5.21 直接报语法错误。该知识仍对升级脚本（若未来引入 STRUCT 列）有效；但如 §4.3 所述，嵌套列最终按 VARCHAR(JSON 文本) 落地，当前 DDL 不再出现 STRUCT 定义。

### 4.3 Routine Load：同构平移 + 真实消费链路验证出的三项修正

与 doris/sqls/bfe_ai_log_load_routine.sql 逐列对照，UTC 墙钟算术式（`DATE_SUB(FROM_UNIXTIME(timestamp), INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND)`）与 `max_error_number=1000` 原样平移（已在 FE 会话时区 Asia/Shanghai 下实测得 UTC 墙钟）。但 **starrocks-it TC02 的真实消费链路证明：CREATE ROUTINE LOAD 只校验语法，列表达式在 BE 任务创建/运行时才分析**——初版"同构平移"在冒烟阶段未被发现的三个兼容性问题，全部由真实消费暴露并修正：

1. **`json_extract(varchar, varchar)` 不存在**：SR 3.5 的 `json_extract` 无该签名（任务创建报 "No matching function"）。level 十列打平改写为 **`get_json_string(ai_apikeytags, '$.levelN.x')`**——直接返回去引号字符串，无需再 `json_unquote`；
2. **COLUMNS 表达式上下文中复杂列以 VARCHAR（JSON 文本）承载**：`ai_rate_limit_hits[1].xxx` 子脚本写法报 "cannot subscript VARCHAR"。限流三列打平改写为 **`get_json_string(ai_rate_limit_hits, '$[0].xxx')`**（JSON 路径取首个命中）；
3. **VARCHAR 不能直转 ARRAY<STRUCT>，且派生列禁止自引用**：任务提交报 "Not support cast VARCHAR(-1) to ARRAY<STRUCT{...}>"，`col = CAST(parse_json(col) ...)` 又报 "Referenced column ... can't be found in column list"（备选方案别名派生列报 "Mapping column is not in table"）。**决策：明细表 5 个嵌套列（`req_headers`/`res_headers`/`ai_route_rule_hits`/`ai_cluster_key_names`/`ai_rate_limit_hits`）以 VARCHAR 承载 JSON 文本**——列名契约不变，Routine Load 裸映射即可落库；结构化访问由查询侧 `CAST(parse_json(col) AS ARRAY<STRUCT<...>>)` 还原（MV 的 rate_limit_hits 即此用法）；标量数组（两个配额计划列）的 VARCHAR 直转原生支持，保持 ARRAY 类型。设计稿 §4.3"复杂 ARRAY<STRUCT> 列直映射、预期无需打平替代"的假设在 3.5.21 不成立，以本节实测结论为准。

消费组 `starrocks_bfe_ai_log`（测试 `starrocks_bfe_ai_log_test`）与 Doris 链路 `doris_bfe_ai_log`、ClickHouse 链路 `clickhouse_bfe_ai_log` 三者独立，位点互不影响。

### 4.4 分钟聚合：异步 MV 的三分修正（实测驱动）

设计稿 §4.4 的 MV 骨架在 3.5.21 实测踩了三个坑，逐一修正后落地：

1. **`array_size` 不存在**：SR 无 `array_size` 函数，等价函数为 `array_length`（或 `cardinality`）；限流命中计数改写为 `array_length(ai_rate_limit_hits) > 0`（限流命中计数口径不变）。
2. **PARTITION BY 溯源校验**：设计稿原写 `PARTITION BY date_trunc('day', ts_min)`，SR 拒绝——"partition function date_trunc must related with column"。MV 的分区表达式必须引用 SELECT 输出列且能溯源基表分区列，`ts_min = date_trunc('minute', log_time)` 的表达式嵌套别名无法通过校验；写 `PARTITION BY date_trunc('day', log_time)` 也被拒绝（"log_time is not found in query statement"，基表列必须在 SELECT 输出中）。
3. **`partition_ttl` 依赖分区 MV**：去掉 PARTITION BY 后 MV 成为非分区表，`partition_ttl` 报 "only supported by partitioned materialized-view"，同时自动分区推导未生效。

**最终形态**：SELECT 首列显式暴露按天粒度的 `date_trunc('day', log_time) AS ts_day` 并纳入 GROUP BY（比 `ts_min` 粗，不改变聚合行数），`PARTITION BY ts_day` + `partition_ttl=7 DAY`。代价：MV 比 Doris 聚合表多一列（api 查询用显式列名不受影响，仅 `SELECT *` 多一列）；收益：7 天保留期链路完整，且分区裁剪按天生效。设计稿 §9.2 预留的"版本确认项"由此闭环。

刷新语义维持设计稿口径：基表增量刷新（分区粒度按天）+ `date_trunc('minute')` 分钟桶，无滑窗谓词；端到端延迟 1~2 分钟。

### 4.5 mysql 客户端 `--skip-comments`（执行通道修正，与 SQL 资产无关）

StarRocks FE 解析器**拒绝仅含注释的语句**（`Unexpected input '<EOF>'`，1064），而 MySQL 8.x 客户端默认把 SQL 文件中的注释行作为独立语句发送。本仓 SQL 资产遵循 doris 版惯例带注释头，故 `setup.sh` / `cleanup.sh` 构建的 mysql 命令必须带 **`--skip-comments`**（客户端侧剥离注释后发送，SQL 文件中的注释仅作资产文档）。doris 版脚本无此问题（Doris FE 容忍注释-only 语句），属 StarRocks 链路独有。

### 4.6 保留期

| 对象 | 机制 | 实测 |
|------|------|------|
| 明细表 | 动态分区 `start=-7`（过期分区自动删除） | 3.5.21 通过（表属性 `dynamic_partition` 生效） |
| 聚合 MV | `partition_ttl=7 DAY` | 3.5.21 通过（`SHOW MATERIALIZED VIEWS` 回显 `"partition_ttl" = "7 DAY"`）；老版本不支持时由 cleanup 脚本或定时任务兜底 |

## 5. 验证情况（真实环境冒烟，2026-10-02）

环境：StarRocks 3.5.21（WSL2 单机 FE+BE，MySQL 协议 127.0.0.1:9030，root 无密码），测试库 `bfe_observability_test`、测试 Topic `bfe_ai_log_test`：

| 项 | 结果 |
|----|------|
| `setup.sh setup_test.conf` 全量执行 | ✅ 建库 → 明细表 → MV → Routine Load 四步全部成功 |
| `cleanup.sh -y setup_test.conf` 全量清理 | ✅ 删 Routine Load → 删 MV → 删明细表全部成功（幂等重跑无残留报错） |
| setup 二次全量执行（清理后重建） | ✅ 全部成功 |
| 明细表结构 | ✅ 102 列与 doris 版列定义数一致；5 个嵌套列按 §4.3 决策以 VARCHAR 承载 JSON 文本（列名契约不变），其余同型 |
| 物化视图状态 | ✅ `is_active=true`，`last_refresh_state=SKIPPED`（无数据时正常），列清单 = ts_day + 40 维 + 24 指标 |
| Routine Load 配置 | ⚠️ 冒烟仅验证任务创建被接受（CREATE 不校验列表达式）；**表达式兼容性由 starrocks-it TC02 真实消费链路验证**——初版平移在任务创建/运行时暴露三处不兼容，已按 §4.3 修正并全绿 |

starrocks-it（`tests/integration/`，2026-10-02 落地）在真实环境完成五个用例的端到端断言：

| 用例 | 结果 | 覆盖点 |
|------|------|--------|
| TC01 全新安装 schema | ✅ | 四份 SQL 按序建 3 对象；明细表 102 列 / MV 65 列（ts_day+40 维+24 指标）；关键列类型固化（**BOOLEAN 在 information_schema 以 tinyint 呈现**）；重复应用行为固化（建库幂等；表/MV 报 already exists、RL 报 "Name ... already used in db"） |
| TC02 消费打平（真实 Kafka） | ✅ | demo 三样例（timestamp 重写当前时刻、logid 重写到 Int64 域、单行压缩）经 Routine Load COLUMNS 映射落库：level 十列、限流三列、log_time UTC 墙钟（与 FE 会话时区无关）、NULL 语义；RL PAUSED 自愈；明细恰好 3 行。**本用例同时抓出并验证了 §4.3 的三项 RL 方言修正**（初版 json_extract 平移在任务创建/运行时连续报错） |
| TC03 明细到聚合口径 | ✅ | 直插三行（ai_rate_limit_hits 写 JSON 文本字面量——SR 侧该列为 VARCHAR）→ 异步 MV 周期刷新：24 指标关键子集（request=3、error=2、auth_reject=1、input/output/total=95/173/207、rate_limit_hits=1）+ 40 维关键子集；**跨刷新周期（70s）重查不重复累计** |
| TC04 存量升级 ALTER | ✅ | ADD COLUMN 成功（Schema Change 异步生效，轮询至可见）；**负向断言：SR 3.5.21 不支持 ADD COLUMN IF NOT EXISTS**（1064 "No viable statement for input 'ADD COLUMN IF'"）；MV 不自动携带新列；ALTER 后 MV 刷新不受影响 |
| TC05 聚合 MV 重建 | ✅ | DROP MV → 仓库 DDL 重建 → **异步 MV 创建即自动全量回填**（无需 CH 侧手工 INSERT..SELECT 重算）→ 口径与重建前逐项一致 |

starrocks-it 驱动出的补充方言知识（已固化在 `tests/integration/测试设计文档/`）：

1. **超界 BIGINT 被拒绝**：INSERT 字面量报 "Number out of range"；Routine Load 计错误行跳过——与 Doris 静默截断、CH UInt64 原值保留为三引擎差异点。测试消息将 demo 的 UInt64 域 logid 重写到 Int64 域（上游真实 logid 在 Int64 范围内，非资产缺陷）。
2. **异步 MV 增量刷新分批可见**：轮询等待必须以"聚合行数到位"（SUM(request_count)=N）为信号，不能以"出现聚合行"为信号。
3. **`ADD COLUMN IF NOT EXISTS` 不支持**：升级脚本幂等写法与 Doris 3.0 / CH 不同（须 information_schema 判断或容忍报错）。
4. **JSON 函数面**（§4.3 已吸收进资产）：`get_json_string(varchar, path)` 为 varchar JSON 文本取值函数；`UNIX_TIMESTAMP(datetime)` 按会话时区解释 UTC 墙钟列（断言差 8h），epoch 断言须用 `TIMESTAMPDIFF` 墙钟算术。
5. **查询侧嵌套还原**：`CAST(parse_json(varchar_col) AS ARRAY<STRUCT<...>>)` 可在 MV/查询中把 VARCHAR(JSON 文本) 列还原为结构化数组（TC03 的 rate_limit_hits 指标链路验证）。

## 6. 影响面与回滚

- **纯新增目录**：`starrocks/` 与既有 `doris/`、`clickhouse/`、`grafana/`、`api/` 无交叉，回滚 = 删除 `starrocks/` 目录 + `cleanup.sh` 清理已部署对象。
- **对生产 Doris / ClickHouse 链路**：零影响（独立消费组、独立数据库对象；三引擎并存仅用于灰度比对期，生产仍按既定约束单落库）。
- **后续依赖**：api 仓 `storage/starrocksreport`（dorisreport 同源克隆 + 方言差异点修正，设计稿 §6.2）与 `starrocks/tests/integration/`（starrocks-it）。
