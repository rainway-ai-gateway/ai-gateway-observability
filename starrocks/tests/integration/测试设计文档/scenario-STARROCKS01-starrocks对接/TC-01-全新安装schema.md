# TC-01 全新安装 schema

## 用例编号与名称

TC-01 全新安装 schema

## 所属场景

STARROCKS01 StarRocks 对接（报表存储新引擎）

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 StarRocks 对接）
- `StarRocks`：本机 WSL2 单机 3.5.21（FE+BE）

## 测试目的

验证全新安装路径：按 `setup.sh` 顺序应用四份 SQL 资产（建库 → 明细表 → 分钟聚合 MV → Routine Load）后，三类对象全部正确创建，列契约与 Doris 侧一致；并把重复应用行为固化为断言（建库幂等；表/MV/RL 非幂等，与 doris 版资产语义一致）。对应设计稿 §4.2–§4.4 与修改说明 §5。

## 运行模式

直连模式：go-sql-driver/mysql 连接本机 StarRocks FE（`127.0.0.1:9030`），在独立测试库中真实执行仓库 SQL 资产（仅 `${VAR}` 变量替换）。StarRocks 不可达时自动 `SKIP`。

## 前置条件

1. StarRocks 已启动（`wsl bash ~/starrocks/bin/start-all.sh`），root 无密码可连。
2. `starrocks/sqls/` 四份 SQL 资产与 `starrocks/demo/` 三个样例存在。

## 测试步骤

1. 生成独立测试库名（`bfe_observability_it_tc01..._<纳秒>`），`DROP DATABASE IF EXISTS ... FORCE` 清残留后执行 `bfe_observability.sql` 建库；
2. 按 setup.sh 顺序执行 `bfe_ai_request_log.sql`、`bfe_ai_metrics_1m.sql`、`bfe_ai_log_load_routine.sql`；
3. 断言对象存在且类型正确：`information_schema.tables` 中 `bfe_ai_request_log`=BASE TABLE、`bfe_ai_metrics_1m`=VIEW（异步物化视图形态）；`SHOW ALL ROUTINE LOAD` 中本库存在 `bfe_ai_log_load`；`SHOW MATERIALIZED VIEWS` 中 `bfe_ai_metrics_1m` 存在且 `is_active=true`；
4. 断言列数：明细表 104 列（与 doris 版 DDL 列定义数一致，逐列同名同型）；MV 65 列（分区列 ts_day + 40 维 + 24 指标）；
5. 断言关键列类型（**用 `DESC` 而非 `information_schema`**：后者把 `BOOLEAN` 呈现为 `tinyint(1)`，与真正的 `TINYINT` 无法区分，布尔列类型错误在该视图下不可见——issue #10 的漏检路径）：`logid`=bigint、`log_time`=datetime、`mirror_hit`=`ai_intent_cache_hit`=`ai_stream`=`is_trust_src_ip`=**boolean**、`ai_intent_confidence`=double、`level1Name`=varchar(128)、`ai_input_tokens`=bigint；
6. 重复应用行为：建库 SQL 重复执行成功（`IF NOT EXISTS` 幂等）；`bfe_ai_request_log.sql` / `bfe_ai_metrics_1m.sql` / `bfe_ai_log_load_routine.sql` 重复执行报错，且错误信息分别含 "already exists"（表/MV）与 "already used in db"（Routine Load，SR 特有报错文案）；
7. `t.Cleanup` 中 `DROP DATABASE ... FORCE` 清理（`STARROCKS_IT_KEEP_DB=1` 可保留现场）。

## 预期结果

全部断言通过；重复应用的三类报错信息与本用例固化的文案一致（防误改 SQL 引入 `IF NOT EXISTS` 造成"重复 setup 静默跳过"的运维错觉——starrocks 资产沿 doris 语义，重建须先 `cleanup.sh`）。

## 备注

- MV 在 `information_schema.tables` 以 `VIEW` 类型出现，明细表为 `BASE TABLE`；`SHOW MATERIALIZED VIEWS` 的 `text` 列可回看完整 MV 定义（含 `partition_ttl=7 DAY`）。
- Routine Load 创建时 Kafka 不可达不影响本用例（任务创建即存在，状态 PAUSED 属环境条件，由 TC02 的自愈逻辑覆盖）。
