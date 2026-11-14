# 异步物化视图的 TTFT/TPOT 收敛为流式口径（ai-gateway-api #315）

- **日期**：2026-11-08
- 上游：[rainway-ai-gateway/ai-gateway-api#315](https://github.com/rainway-ai-gateway/ai-gateway-api/issues/315)（SC2201-TC005：overview 的 TTFT/TPOT 均值把非流式行的首包延迟计入分子）
- **状态**：已实施（SQL 资产 + TABLE_DESIGN 口径 + 集成断言；部署步骤见 §4）
- **结论**：`sqls/bfe_ai_metrics_1m.sql`（异步 MV，MV 名即 `bfe_ai_metrics_1m`）的 `ttft_us_sum` / `tpot_us_sum` 由全行求和改为**仅 `ai_stream=1` 行**条件聚合。维度、分区（`ts_day` + `partition_ttl=7 DAY`）、其余指标不变。

## 1. 背景

BFE 对非流式响应同样实测首包/每 token 耗时（`mod_body_process` 对任意响应首包置 `TFirstToken` 并计算 `TTFT`/`TPOT`），明细非流式行的 `ai_ttft_us` **可非 0**（明细列可空，MV 内 `COALESCE(ai_stream,0)` 归一）。旧 MV 用 `SUM(COALESCE(ai_ttft_us,0))` 全行求和，聚合列混入非流式贡献；api 除以流式请求数后混合流量下均值被放大（实测 2.5~2.9 倍），违背 `report.md §2.2`「聚合于 stream 请求」。

api 仓已在查询侧以 `CASE WHEN ai_stream=1` 收敛（四后端 storager + MySQL JOB）。本仓三份聚合定义与之一致化；由于聚合按 `ai_stream` 维度分组（行内无混合），两侧过滤恒等，任意顺序发布、不重复扣减，api 正确性不依赖本仓改动（查询侧已兜底存量污染行）。

## 2. 改动点

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_metrics_1m.sql` | `SUM(CASE WHEN COALESCE(ai_stream,0)=1 THEN COALESCE(ai_ttft_us/ai_tpot_us,0) ELSE 0 END)`，附口径注释 |
| `docs/design/TABLE_DESIGN.md` | §3.2 口径差异表新增 TTFT/TPOT 行 |
| `tests/integration/.../sr01_starrocks_dock_test.go` | `aggTotalsWant` 增加 `ttft_us_sum`/`tpot_us_sum` = `"0"`：三行 demo 均为非流式且 normal 行携带 `ai_ttft_us=6287`，锁死新口径（修复前该断言为 6287） |

## 3. 口径与边界

- 明细 `ai_ttft_us`/`ai_tpot_us` 语义**不变**（非流式行如实记录首包耗时）。
- 明细 `ai_stream` 可空，`COALESCE(ai_stream,0)` 把 NULL 归为非流式，与既有归一风格一致。
- 纯非流式窗口聚合行两列为 0；纯流式窗口与修复前一致。

## 4. 部署与存量数据

- **生效**：`DROP MATERIALIZED VIEW bfe_ai_metrics_1m` 后重新执行 `sqls/bfe_ai_metrics_1m.sql`。
- **存量**：异步 MV 刷新从基表重算，重建后 `partition_ttl=7 DAY` 内的历史分区**应自动回到流式口径**（其它后端不具备此性质）。QA 验证方式：重建后抽样某个历史分钟桶，`ttft_us_sum` 应等于该桶流式明细行求和；若假设不成立，按 Doris/CH 同款方式手动重放（先删后插，AGGREGATE 语义下重插前必须删除，否则叠加翻倍）。
- **回滚**：重走同一流程建回旧 MV 定义即可。

## 5. 测试

集成测试 `aggTotals`/`aggTotalsWant` 新增两列断言（非流式 demo 行携带 ttft，期望 0），`go vet` 通过；真实实例执行随常规 CI/QA 环境。
