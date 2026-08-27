# 变更记录：升级到新 PB（bfe-access-pb）字段

- **日期**：2026-08-26
- **变更范围**：Grafana 展示层（Dashboard `bfe-ai-gateway-observability.json`、`setup.sh`、数据源模板）
- **变更原因**：Doris 存储层升级到新版 protobuf（`bfe-access-pb` 的 `BfeLog` 结构）后字段改名/新增，Grafana Dashboard 的变量与面板 SQL 同步对齐，并补充新字段对应的面板。

> 字段语义与 Doris 表结构对齐，以 [Doris 表设计说明](../../doris/docs/design/TABLE_DESIGN.md) 为准。

## 1. 变更总览

| 类别 | 内容 |
|------|------|
| 字段改名 | 面板 SQL 中 `ai_mapped_model` → `ai_target_model`、`ai_apikey` → `ai_apikey_id` |
| 新增模板变量 | `provider`（上游提供商） |
| 新增面板 | 6 个（按提供商/协议/模式请求量 + 标签 Level1~Level5 下钻中的 Level2~Level5） |
| 更新面板 | Token 组成（新增音频 Token + 图片数）、成本趋势、模型层重试次数、认证拒绝等 |
| 参数化 | 数据源名/库名改为 `${DATASOURCE_NAME}` / `${DORIS_DATABASE}` 占位符 |

## 2. 字段改名（面板 SQL 对齐）

| 旧字段 | 新字段 | 涉及面板 |
|--------|--------|----------|
| `ai_mapped_model` | `ai_target_model` | 全局概览、延迟分布、组件重试、维度下钻、明细回溯 |
| `ai_apikey` | `ai_apikey_id` | 全局概览、延迟分布、维度下钻、明细回溯 |
| `IN ($model)` / `IN ($apikey)` | `('$model' = '*' OR ...)` | 所有带模板变量的面板，兼容 All 与具体值 |

## 3. 新增模板变量

| 变量名 | 标签 | 类型 | 数据源 SQL |
|--------|------|------|-----------|
| `provider` | 提供商 | query（单选，含 All） | `SELECT DISTINCT ai_provider FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR AND ai_provider != '' ORDER BY ai_provider` |

> 原有变量 `model`、`apikey` 的 SQL 分别改为从 `ai_target_model`、`ai_apikey_id` 取值。

## 4. 新增面板

### 4.1 按提供商请求量（维度下钻）

- 类型：piechart
- 数据源字段：`ai_provider`

### 4.2 按协议请求量（维度下钻）

- 类型：piechart
- 数据源字段：`ai_protocol`

### 4.3 按模式请求量（维度下钻）

- 类型：piechart
- 数据源字段：`ai_mode`

### 4.4 按标签 Level2~Level5 请求量（维度下钻）

- 类型：barchart（水平）× 4
- 数据源字段：`level2Name`/`level2` ~ `level5Name`/`level5`（与 Level1 组成 5 级标签下钻）

## 5. 更新面板（补充新 PB 指标）

### 5.1 Token 组成（Token 与成本，堆叠）

新增三个系列：

| 系列 | 数据源字段 | 说明 |
|------|-----------|------|
| `ai_audio_input_tokens` | `ai_audio_input_tokens` | 音频输入 Token 累计 |
| `ai_audio_output_tokens` | `ai_audio_output_tokens` | 音频输出 Token 累计 |
| `ai_image_count` | `ai_image_count` | 图片数量累计 |

### 5.2 成本趋势（Token 与成本）

- 数据源字段：`ai_cost_value_sum`、`ai_cost_currency`（按币种分系列）

### 5.3 模型层重试次数（组件与重试）

- 数据源字段：`ai_retry_count_sum`

### 5.4 其它对齐

- 延迟分布、明细回溯等面板的过滤字段同步改为新字段名（`ai_target_model`、`ai_apikey_id`）。

## 6. 参数化改动

| 项 | 旧值 | 新值 |
|----|------|------|
| 数据源 label/name | `Doris` / `Doris` | `${DATASOURCE_NAME}`（由 `setup.sh` 注入） |
| 数据源 dataset | `bfe_observability` | `${DORIS_DATABASE}` |

## 7. 相关文档

- Dashboard 面板布局与 SQL：[../design/DASHBOARD_DESIGN.md](../design/DASHBOARD_DESIGN.md)
- Grafana 配置指南：[../user/HOWTO.md](../user/HOWTO.md)
- Doris 表设计（字段语义）：[../../doris/docs/design/TABLE_DESIGN.md](../../doris/docs/design/TABLE_DESIGN.md)
