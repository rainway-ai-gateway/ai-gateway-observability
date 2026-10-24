# TC-04 存量升级 ALTER

## 用例编号与名称

TC-04 存量升级 ALTER

## 所属场景

CLICKHOUSE01 ClickHouse 对接（报表存储新引擎）

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 ClickHouse 对接）
- `ClickHouse`：本机 WSL2 单文件 26.10.1.1149

## 测试目的

验证存量部署的升级 ALTER 行为（PB/log-reader 新增字段后按 doris-it 同款「升级 ALTER」模式补列）：明细表 `ADD COLUMN` 成功；ClickHouse 支持 `ADD COLUMN IF NOT EXISTS`，升级脚本可幂等重跑（与 Doris 3.0 不支持该语法形成差异，固化为断言）；并记录演进约束行为——**聚合表/聚合 MV 不自动携带明细新列**（对齐 Doris AGGREGATE KEY 重建心智：新维度要进聚合口径须按 TC05 流程重建聚合表与聚合 MV）。对应设计稿 §5 演进约束。

## 运行模式

单组件模式：直连真实 ClickHouse，在独立测试库中模拟明细表加列并观察聚合侧行为。

## 前置条件

1. ClickHouse 已启动，default 无密码可连。

## 测试数据构造

- 独立测试库，按 setup 顺序建明细表/聚合表/聚合 MV（不建 Kafka 表与消费 MV）。

## 执行步骤

1. 创建独立测试库并建三类对象。
2. `ALTER TABLE bfe_ai_request_log ADD COLUMN it_probe_col String DEFAULT ''` → 断言 `system.columns` 中 `it_probe_col` 存在且类型为 `String`。
3. `ALTER TABLE bfe_ai_request_log ADD COLUMN IF NOT EXISTS it_probe_col String DEFAULT ''` → 断言不报错（幂等升级脚本可重跑；Doris 3.0 第二次执行报 Duplicate column，此处为引擎差异固化）。
4. 行为记录断言：`system.columns` 中聚合表 `bfe_ai_metrics_1m` 不存在 `it_probe_col`（MV 目标表列即 MV 输出列——聚合 MV 不自动携带明细新列）。
5. 兼容性断言：明细 ALTER 后既有聚合 MV 不受影响——插入一行（含 `it_probe_col='probe'`），等待 1s 后按 `GROUP BY ts_min + sum(request_count)` 兜底查询，断言该行正常聚合计数（request_count=1）。

## 预期结果

- 步骤 2：`it_probe_col` 就位且类型为 `String`。
- 步骤 3：重复 `ADD COLUMN IF NOT EXISTS` 无错误。
- 步骤 4：聚合表无 `it_probe_col`（记录演进约束：明细加列后聚合 MV 需重建才能纳入新维度）。
- 步骤 5：明细 ALTER 后聚合链路未破坏，新行正常计入聚合结果。

## 清理

`t.Cleanup`：`DROP DATABASE <测试库>`；`CLICKHOUSE_IT_KEEP_DB=1` 时保留现场。
