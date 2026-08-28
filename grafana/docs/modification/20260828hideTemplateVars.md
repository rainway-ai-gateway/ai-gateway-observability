# 变更记录：隐藏 Dashboard 顶部过滤变量（模型/API Key/提供商/Host/URI）

- **日期**：2026-08-28
- **变更范围**：Grafana 展示层（Dashboard `bfe-ai-gateway-observability.json` 的 `templating.list`）
- **变更原因**：顶部 5 个过滤变量（模型、API Key、提供商、Host、URI）在选定具体值后大量面板出现空数据，且过滤条件在各面板间应用不一致。经排查决定先隐藏下拉、保留默认 `*`（全部）语义，避免误导使用者。

## 1. 问题描述

访问 Dashboard（`http://<grafana>/d/bfe-ai-gateway-observability/...`）时，顶部有 5 个过滤选项：模型、API Key、提供商、Host、URI。当选定某个具体值后，下方大量面板变空。

## 2. 根因分析

### 2.1 数据时间范围与默认窗口脱节（主因）

两张表的数据仅覆盖一小段时间窗口，而 Dashboard 默认时间范围为 `now-6h`：

| 表 | 数据时间范围 | 说明 |
|----|--------------|------|
| `bfe_ai_metrics_1m` | 数据集中在较短窗口内 | 分钟级聚合表 |
| `bfe_ai_request_log` | 数据集中在较短窗口内 | 明细日志表 |

当默认 `now-6h` 窗口内没有数据时，无论是否选择过滤值，面板都是空的。

同时，各过滤变量的取数 SQL 使用固定的近 1 小时查询（`NOW() - INTERVAL 1 HOUR`），与 Dashboard 的时间范围（`now-6h`）完全脱节。数据陈旧时，下拉选项本身也可能是空的。

### 2.2 过滤条件在各面板间应用不一致（设计缺陷）

`model / apikey / provider` 只在 `bfe_ai_metrics_1m` 的指标面板上生效；延迟类面板（P50/P90/P99、TTFT、TPOT、组件耗时）查 `bfe_ai_request_log`，SQL 中只用了 `model` + `apikey`，没有 `provider`；`host` 只作用在「按 Host 请求量」一个面板；`uri` 只作用在「日志明细」一个表。

因此选定「提供商」后，指标面板被过滤、延迟面板却不过滤；选定「Host」后几乎所有面板都不受影响。表现就是“面板为空 / 没反应”。

### 2.3 维度值极度稀疏

数据中 `provider` / `apikey` 的值大多是单条或极少条（例如 `api-key-601/602` 在指标表中各只有 1 行）。选到这些值后，过滤结果只剩 1 个数据点，时间序列图看起来就是空的。

## 3. 处理措施

将 `templating.list` 中 5 个变量的 `hide` 从 `0` 改为 `2`（完全隐藏），变量定义与默认值 `*` 保留，所有面板 SQL 不动，仍按「全部」展示。

| 变量名 | 标签 | 类型 | hide 变化 |
|--------|------|------|-----------|
| `model` | 模型 | query | `0` → `2` |
| `apikey` | API Key | query | `0` → `2` |
| `provider` | 提供商 | query | `0` → `2` |
| `host` | Host | query | `0` → `2` |
| `uri` | URI | textbox | `0` → `2` |

> `hide` 取值含义：`0` = 显示；`1` = 仅显示标签、隐藏下拉；`2` = 完全隐藏。本变更使用 `2`，顶部不再出现这些过滤控件。

## 4. 生效方式

需重新导入 Dashboard JSON 后生效，二选一：

1. 在 Grafana 中重新导入 `grafana/dashboards/bfe-ai-gateway-observability.json`；
2. 重新运行 `grafana/setup.sh`。

## 5. 后续建议

- 保证数据持续上报，避免数据窗口与默认时间范围脱节导致面板整体为空。
- 如需彻底移除过滤能力，除删除 `templating.list` 中对应变量外，还须同步删除各面板 `rawSql` 中的 `('$xxx' = '*' OR ...)` 条件，二者必须一起处理，否则 `$xxx` 会变成未定义变量导致查询报错。
- 如需保留并正确使用过滤，建议将变量改为多选（`multi: true`）并使用 `IN` 子句，同时把 `provider` / `host` 等过滤补到延迟类面板，使过滤真正一致生效。

## 6. 相关文档

- Dashboard 面板布局与 SQL：[../design/DASHBOARD_DESIGN.md](../design/DASHBOARD_DESIGN.md)
- Grafana 配置指南：[../user/HOWTO.md](../user/HOWTO.md)
- Doris 表设计（字段语义）：[../../doris/docs/design/TABLE_DESIGN.md](../../doris/docs/design/TABLE_DESIGN.md)