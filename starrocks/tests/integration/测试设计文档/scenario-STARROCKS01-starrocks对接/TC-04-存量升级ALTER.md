# TC-04 存量升级 ALTER

## 用例编号与名称

TC-04 存量升级 ALTER

## 所属场景

STARROCKS01 StarRocks 对接（报表存储新引擎）

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 StarRocks 对接）
- `StarRocks`：本机 WSL2 单机 3.5.21（FE+BE）

## 测试目的

验证存量升级路径的方言行为：明细表在线加列成功（Schema Change 异步生效）；**负向断言**——SR 3.5.21 不支持 `ADD COLUMN IF NOT EXISTS`（与 Doris 3.0 / ClickHouse 差异点固化，升级脚本的幂等手段须改为 information_schema 判断或容忍报错）；行为记录——MV 不自动携带明细新列（对齐 Doris AGGREGATE KEY 重建心智）；明细 ALTER 后既有 MV 刷新不受影响。对应设计稿 §5 演进约束。

## 运行模式

直连模式：独立测试库中真实执行 DDL/DML 与查询断言。StarRocks 不可达时自动 `SKIP`。

## 前置条件

1. StarRocks 已启动，root 无密码可连。
2. `starrocks/sqls/` 资产存在。

## 测试步骤

1. 独立测试库建仓 + 明细表 + 异步 MV；
2. `ALTER TABLE bfe_ai_request_log ADD COLUMN it_probe_col VARCHAR(64) DEFAULT ''`——SR Schema Change 为异步生效，**轮询** `information_schema.columns` 至列可见（deadline 60s）；
3. 负向断言：再次执行 `ADD COLUMN IF NOT EXISTS it_probe_col2 ...` 应报错（1064 "No viable statement for input 'ADD COLUMN IF'"），错误信息含 "ADD COLUMN IF"；
4. 行为记录：MV `bfe_ai_metrics_1m` 的列清单中不应出现 `it_probe_col`（MV 输出列在创建时固化，不随基表加列自动扩展）；
5. 写入一行含新列取值的数据（logid=2001, it_probe_col='probe'），轮询至 MV 该桶 `SUM(request_count)=1` 后断言聚合正常（MV SELECT 不引用新列，刷新不受影响）；
6. `t.Cleanup` 删除测试库。

## 预期结果

加列成功且轮询可见；`IF NOT EXISTS` 变体报错（负向断言通过）；MV 无新列；ALTER 后 MV 刷新与聚合正常。

## 备注

- 与 clickhouse-it TC04 的差异：CH 支持 `ADD COLUMN IF NOT EXISTS`（正向断言），SR 3.5.21 不支持（负向断言）——三引擎升级脚本的幂等写法不同，升级 SQL 资产须按引擎分目录（沿用 `sqls/upgrade/<日期>-<变更>/` 惯例）。
- Doris/SR 共性：MV/聚合表不自动携带明细新列，新维度进聚合口径须重建（TC05）。
