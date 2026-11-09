# 分钟聚合 JOB 的 TTFT/TPOT 收敛为流式口径（ai-gateway-api #315）

- **日期**：2026-11-08
- 上游：[rainway-ai-gateway/ai-gateway-api#315](https://github.com/rainway-ai-gateway/ai-gateway-api/issues/315)（SC2201-TC005：overview 的 TTFT/TPOT 均值把非流式行的首包延迟计入分子）
- **状态**：已实施（SQL 资产 + TABLE_DESIGN 口径 + 查询示例；部署步骤见 §4）
- **结论**：`sqls/bfe_ai_metrics_1m_job.sql` 的 `ttft_us_sum` / `tpot_us_sum` 由全行求和改为**仅 `ai_stream=1` 行**条件聚合。表结构、维度、其余 22 个指标不变。

## 1. 背景

BFE 对非流式响应同样实测首包/每 token 耗时（`mod_body_process` 对任意响应首包置 `TFirstToken` 并计算 `TTFT`/`TPOT`，`mod_access_pb3` 无条件写入 `ai_ttft_us`/`ai_tpot_us`），因此明细表非流式行的 `ai_ttft_us` **可非 0**。旧 JOB 用 `SUM(COALESCE(ai_ttft_us,0))` 全行求和，导致 `bfe_ai_metrics_1m.ttft_us_sum` 混入非流式贡献；api 查询侧除以流式请求数后，混合流量下流式 TTFT/TPOT 均值被系统性放大（实测 2.5~2.9 倍）。`report.md §2.2` 的口径是「聚合于 stream 请求」。

api 仓已在查询侧以 `CASE WHEN ai_stream=1` 收敛（四后端 storager + MySQL 进程内 JOB）。本仓三份聚合定义与之一致化，使聚合列自解释为「流式累计」，直连表 / BI / Grafana 消费方不再踩坑。由于聚合表按 `ai_stream` 分组（行内无流/非流混合），两侧过滤结果恒等，任意顺序发布、不会重复扣减；api 正确性不依赖本仓改动（查询侧已兜底存量污染行）。

## 2. 改动点

| 文件 | 改动 |
|------|------|
| `sqls/bfe_ai_metrics_1m_job.sql` | `ttft_us_sum`/`tpot_us_sum` 改为 `SUM(CASE WHEN COALESCE(ai_stream,0)=1 THEN COALESCE(ai_ttft_us/ai_tpot_us,0) ELSE 0 END)`，附口径注释 |
| `docs/design/TABLE_DESIGN.md` | §3.3 字段表与口径映射表注明两列仅流式行贡献；§5 直连查询示例的分母改为 `SUM(CASE WHEN ai_stream=1 THEN request_count ELSE 0 END)` |

## 3. 口径与边界

- 明细 `ai_ttft_us`/`ai_tpot_us` 语义**不变**：非流式行继续如实记录首包耗时（有价值的原始观测），文档不再承诺「非流式为 0」。
- `ai_stream` 为 NULL 的明细行按非流式处理（`COALESCE(ai_stream,0)`），与既有归一风格一致。
- 纯非流式窗口的聚合行两列为 0；纯流式窗口与修复前一致。

## 4. 部署与存量数据

- **生效**：`DROP JOB WHERE JobName = 'bfe_ai_metrics_1m_job'` 后重新执行 `sqls/bfe_ai_metrics_1m_job.sql`（Doris 不支持就地修改 JOB 的 DO 体；先例见 `docs/user/HOWTO.md` §5）。
- **存量**：JOB 只影响重建后的分钟窗口；历史行的 `ttft_us_sum` 仍是全行口径。**api 查询侧已按流式过滤，接口结果对历史窗口立即正确**；直连表消费方如需修正历史，按天 `DELETE` 窗口后重放同一条 `INSERT … SELECT`（AGGREGATE KEY 表重插前必须先删，否则按 key 叠加翻倍）。
- **回滚**：重走同一流程建回旧 JOB 定义即可。

## 5. 测试

- Doris 集成测试（`tests/integration` TC-01）断言「INSERT JOB 可被 Doris 创建」（CREATE JOB 时校验 DO 语句合法性），条件聚合表达式为 Doris 标准 SQL，与文件内既有 `SUM(CASE WHEN …)`（error_count 等）同模式。
- 端到端流式口径断言在 api 仓集成测试（`test/integration/tests/report/query/`）覆盖。
