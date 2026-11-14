# 聚合物化视图的 TTFT/TPOT 收敛为流式口径（ai-gateway-api #315）

- **日期**：2026-11-08
- 上游：[rainway-ai-gateway/ai-gateway-api#315](https://github.com/rainway-ai-gateway/ai-gateway-api/issues/315)（SC2201-TC005：overview 的 TTFT/TPOT 均值把非流式行的首包延迟计入分子）
- **状态**：已实施（SQL 资产 + TABLE_DESIGN 口径 + 查询示例 + 集成断言；部署步骤见 §4）
- **结论**：`sqls/bfe_ai_metrics_1m_mv.sql` 的 `ttft_us_sum` / `tpot_us_sum` 由 `sum(...)` 改为 `sumIf(..., ai_stream=1)`，**仅流式行贡献**。目标表结构、排序键、其余指标不变。

## 1. 背景

BFE 对非流式响应同样实测首包/每 token 耗时（`mod_body_process` 对任意响应首包置 `TFirstToken` 并计算 `TTFT`/`TPOT`），明细非流式行的 `ai_ttft_us` **可非 0**。旧 MV 用 `sum(ai_ttft_us)` 全行求和，聚合列混入非流式贡献；api 除以流式请求数后混合流量下均值被放大（实测 2.5~2.9 倍），违背 `report.md §2.2`「聚合于 stream 请求」。

api 仓已在查询侧以 `CASE WHEN ai_stream=1` 收敛（四后端 storager + MySQL JOB）。本仓三份聚合定义与之一致化；由于聚合表按 `ai_stream` 分组（行内无混合），两侧过滤恒等，任意顺序发布、不重复扣减，api 正确性不依赖本仓改动（查询侧已兜底存量污染行）。

## 2. 改动点

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_metrics_1m_mv.sql` | `sumIf(ai_ttft_us, ai_stream=1)` / `sumIf(ai_tpot_us, ai_stream=1)`，文件头口径清单同步（"其余指标"由 17 个改为 15 个） |
| `docs/design/TABLE_DESIGN.md` | 指标字段表与口径映射表注明两列仅流式行；§5 直连查询示例分母改为 `sumIf(request_count, ai_stream = 1)` |
| `tests/integration/.../ch01_clickhouse_dock_test.go` | `aggTotalsWant` 增加 `ttft_us_sum`/`tpot_us_sum` = `"0"`：三行 demo 均为非流式且 normal 行携带 `ai_ttft_us=6287`，锁死新口径（修复前该断言为 6287） |

## 3. 口径与边界

- 明细 `ai_ttft_us`/`ai_tpot_us` 语义**不变**（非流式行如实记录首包耗时）。
- `ai_stream` 列为 `UInt8 DEFAULT 0`，无 NULL；`sumIf` 条件不满足的行自然贡献 0。
- 纯非流式窗口聚合行两列为 0；纯流式窗口与修复前一致。

## 4. 部署与存量数据

- **生效**：`DROP MATERIALIZED VIEW bfe_ai_metrics_1m_mv` 后重新执行 `sqls/bfe_ai_metrics_1m_mv.sql`。MV 不挂 POPULATE，只对新建明细生效。
- **存量**：目标表 `bfe_ai_metrics_1m`（SummingMergeTree）中的历史行仍是全行口径。**api 查询侧已按流式过滤，接口结果对历史窗口立即正确**；直连消费方如需修正历史：`ALTER TABLE … DELETE WHERE ts_min >= ? AND ts_min < ?`（异步 mutation，等 `system.mutations` 完成）后按窗口 `INSERT INTO … SELECT … GROUP BY <全 40 维>` 重放；或直接等 7 天 TTL 滚出。
- **回滚**：重走同一流程建回旧 MV 定义即可。

## 5. 测试

集成测试 `aggTotals`/`aggTotalsWant` 新增两列断言（非流式 demo 行携带 ttft，期望 0），`go vet` 通过；真实实例执行随常规 CI/QA 环境。
