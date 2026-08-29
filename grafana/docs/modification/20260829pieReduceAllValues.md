# 变更记录：饼图按行取值修复（按协议 / 按模式 / 按状态码 三个面板）

- **日期**：2026-08-29
- **变更范围**：Grafana 展示层（Dashboard `bfe-ai-gateway-observability.json` 三个饼图面板）
- **变更原因**：饼图面板默认使用「按列归并（Calculate）」模式，会把唯一数值列 `count` 折叠成单一切片，图例显示为 `count / 100%`，而不是按维度取值分片（如 `openai`、`chat`、状态码 `200/401/500…`）。

## 1. 问题现象

| 面板 | 面板 id | 期望显示 | 实际错误显示 |
|------|---------|----------|--------------|
| 按协议请求量 | 32 | `openai` | `count / 26 / 100%` |
| 按模式请求量 | 33 | `chat` | `count / 26 / 100%` |
| 按状态码分布 | 23 | `200 / 401 / 500 …` | `count / 100%` |

根因：三个面板的 `options` 均缺少 `reduceOptions`，默认 `reduceOptions.values = false`（Calculate 模式），Grafana 对唯一的数值列 `count` 做归并，切片名称取其**列名** `count`，而不是用维度列的值命名。

## 2. 解决方案

### 2.1 切换到「按行取值（All values）」模式

为三个饼图面板的 `options` 增加：

```json
"reduceOptions": { "values": true, "fields": "", "calcs": [] }
```

`values = true` 即「All values」模式，按维度列（`ai_protocol` / `ai_mode` / `status_code`）的取值分片，切片名称取维度值。

### 2.2 状态码面板额外 CAST（字符串化）

`res_status_code` 为 SMALLINT 数值类型，无法直接作为切片标签，需转成字符串：

```sql
SELECT CAST(res_status_code AS CHAR) AS status_code, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
GROUP BY res_status_code ORDER BY count DESC
```

> 协议、模式两列为 VARCHAR，无需 CAST。

## 3. 面板变更明细

### 3.1 按协议请求量（id=32）

- 类型：piechart
- 新增：`options.reduceOptions = { "values": true, "fields": "", "calcs": [] }`
- SQL（不变）：
```sql
SELECT ai_protocol, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_protocol != ''
GROUP BY ai_protocol ORDER BY count DESC
```

### 3.2 按模式请求量（id=33）

- 类型：piechart
- 新增：`options.reduceOptions = { "values": true, "fields": "", "calcs": [] }`
- SQL（不变）：
```sql
SELECT ai_mode, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_mode != ''
GROUP BY ai_mode ORDER BY count DESC
```

### 3.3 按状态码分布（id=23）

- 类型：piechart
- 新增：`options.reduceOptions = { "values": true, "fields": "", "calcs": [] }`
- SQL（CAST 状态码为字符串）：
```sql
SELECT CAST(res_status_code AS CHAR) AS status_code, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
GROUP BY res_status_code ORDER BY count DESC
```

## 4. 生效方式

重新导入 Dashboard JSON 后生效，二选一：

1. 在 Grafana 中重新导入 `grafana/dashboards/bfe-ai-gateway-observability.json`；
2. 重新运行 `grafana/setup.sh`。

## 5. 相关文档

- Dashboard 面板布局与 SQL（3.15 / 3.16 / 3.20 节已同步更新）：[../design/DASHBOARD_DESIGN.md](../design/DASHBOARD_DESIGN.md)
- Grafana 配置指南：[../user/HOWTO.md](../user/HOWTO.md)