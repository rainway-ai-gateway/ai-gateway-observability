# TC-05 聚合 MV 重建

## 用例编号与名称

TC-05 聚合 MV 重建

## 所属场景

STARROCKS01 StarRocks 对接（报表存储新引擎）

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 StarRocks 对接）
- `StarRocks`：本机 WSL2 单机 3.5.21（FE+BE）

## 测试目的

验证聚合口径的重建路径：DROP 异步 MV → 按仓库 DDL 重建（REPLACE 语义，SR 无 Doris `REPLACE WITH TABLE` 原子换名）→ **SR 异步 MV 创建后自动从基表全量回填**（初始刷新，无需 CH 侧手工 `INSERT..SELECT` 重算）→ 口径与重建前逐项一致。对应设计稿 §4.4 / §5 / §6 验证计划 2。

## 运行模式

直插模式：独立测试库中写入样例数据、等待 MV 聚合、执行 DROP+CREATE、等待自动回填、重跑口径断言。StarRocks 不可达时自动 `SKIP`。

## 前置条件

1. StarRocks 已启动，root 无密码可连。
2. `starrocks/sqls/` 资产存在。

## 测试步骤

1. 独立测试库建仓 + 明细表 + 异步 MV；
2. 写入三行样例等价数据（同 TC03），轮询至 MV 该桶 `SUM(request_count)=3`，跑 `assertAggregation` 记录重建前口径；
3. 重建：`DROP MATERIALIZED VIEW bfe_ai_metrics_1m`（cleanup.sh 逆序：MV 先于明细表删除），断言 `SHOW MATERIALIZED VIEWS` 中不再可见；
4. 按仓库 DDL 重新执行 `bfe_ai_metrics_1m.sql`，断言 MV 存在且 `is_active=true`；
5. **自动回填**：轮询至 MV 该桶 `SUM(request_count)=3`（新建异步 MV 的初始刷新会从基表全量计算历史分区，即 SR 侧的"重算"，无需手工 INSERT..SELECT）；
6. 重跑 `assertAggregation`：整窗 9 指标与 40 维关键子集分布与重建前逐项一致；
7. `t.Cleanup` 删除测试库。

## 预期结果

MV 重建后自动回填，全部口径断言与重建前一致（若回填未发生或口径漂移，轮询超时/断言失败）。

## 备注

- 与 clickhouse-it TC05 的关键差异：CH 重建后聚合表为空，须按设计稿重算语句手工 `INSERT INTO ... SELECT` 回填；SR 异步 MV 创建即触发初始全量刷新，**重建+回填一步完成**——运维上更简单，但回填期间（1-2 个刷新周期）聚合查询会缺数，生产重建仍需低峰执行。
- 与 Doris 侧"重建聚合表 + JOB 重跑"心智一致：都是非原子 DROP+CREATE，期间聚合口径短暂不可用。
