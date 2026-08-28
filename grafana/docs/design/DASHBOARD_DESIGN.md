# BFE AI Gateway 可观测 — Grafana Dashboard 设计文档

本文档描述 `bfe-ai-gateway-observability` 看板的设计，作为 `dashboards/bfe-ai-gateway-observability.json` 的实现依据。字段语义以 [Doris 表设计说明](../../doris/docs/design/TABLE_DESIGN.md) 为准。

---

## 1. 数据源与变量

### 1.1 数据源

| 项 | 值 |
|----|----|
| 名称 | Doris |
| 类型 | MySQL |
| UID | `doris`（由 `setup.sh` 的 `DATASOURCE_UID` 注入，面板里 `${DS_DORIS}` 会被替换为该 UID） |

### 1.2 模板变量

| 变量名 | 标签 | 类型 | 数据源 SQL | 说明 |
|--------|------|------|-----------|------|
| `model` | 模型 | query（单选，含 All） | `SELECT DISTINCT ai_target_model FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR ORDER BY ai_target_model` | 按路由模型过滤 |
| `apikey` | API Key | query（单选，含 All） | `SELECT DISTINCT ai_apikey_id FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR AND ai_apikey_id != '' ORDER BY ai_apikey_id` | 按 API Key 过滤 |
| `provider` | 提供商 | query（单选，含 All） | `SELECT DISTINCT ai_provider FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR AND ai_provider != '' ORDER BY ai_provider` | 按上游提供商过滤 |
| `host` | Host | query（单选，含 All） | `SELECT DISTINCT header_host FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR AND header_host != '' ORDER BY header_host` | 按请求 Host 过滤 |
| `uri` | URI | textbox | — | 明细回溯中按 URI 模糊匹配 |

> 变量 `allValue` 设为 `*`，SQL 中用 `('$model' = '*' OR ai_target_model = '$model')` 模式兼容「All」与「具体值」。

---

## 2. 面板布局总览

| 行 | 面板 | 类型 | 数据表 | 时间字段 |
|----|------|------|--------|----------|
| 全局概览 | QPS | timeseries | metrics | `ts_min` |
| 全局概览 | 错误率 | timeseries | metrics | `ts_min` |
| 全局概览 | Token 吞吐 (TPM) | timeseries | metrics | `ts_min` |
| 全局概览 | 限流命中 | timeseries | metrics | `ts_min` |
| 全局概览 | 认证拒绝 | timeseries | metrics | `ts_min` |
| 延迟分布 | P50/P90/P99 全链路延迟 | timeseries | detail | `log_time` |
| 延迟分布 | TTFT 分位延迟 | timeseries | detail | `log_time` |
| 延迟分布 | TPOT 分位延迟 | timeseries | detail | `log_time` |
| Token 与成本 | Token 组成（含音频/图片） | timeseries(堆叠) | metrics | `ts_min` |
| Token 与成本 | 成本趋势 | timeseries | metrics | `ts_min` |
| 组件与重试 | 各组件平均耗时 | barchart(堆叠) | detail | `log_time` |
| 组件与重试 | 模型层重试次数 | timeseries | metrics | `ts_min` |
| 维度下钻 | 按模型请求量 | table | metrics | `ts_min` |
| 维度下钻 | 按提供商请求量 | barchart（水平） | metrics | `ts_min` |
| 维度下钻 | 按协议请求量 | piechart | metrics | `ts_min` |
| 维度下钻 | 按模式请求量 | piechart | metrics | `ts_min` |
| 维度下钻 | 按 API Key Token 消耗 | barchart | metrics | `ts_min` |
| 维度下钻 | 按标签 Level1 请求量 | barchart | metrics | `ts_min` |
| 维度下钻 | 按标签 Level2 请求量 | barchart | metrics | `ts_min` |
| 维度下钻 | 按标签 Level3 请求量 | barchart | metrics | `ts_min` |
| 维度下钻 | 按标签 Level4 请求量 | barchart | metrics | `ts_min` |
| 维度下钻 | 按标签 Level5 请求量 | barchart | metrics | `ts_min` |
| 维度下钻 | 按 Host 请求量 | barchart | metrics | `ts_min` |
| 维度下钻 | 按状态码分布 | piechart | metrics | `ts_min` |
| 明细回溯 | 日志明细 | table | detail | `log_time` |
| 明细回溯 | 限流命中展开 | table | detail | `log_time` |
| 明细回溯 | 认证拒绝明细 | table | detail | `log_time` |

---

## 3. 面板详细设计

### 3.1 QPS（全局概览）

- 类型：timeseries，单位 `reqps`
- SQL：
```sql
SELECT ts_min AS time, SUM(request_count) / 60 AS qps
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.2 错误率（全局概览）

- 类型：timeseries，单位 `percent`（阈值 5% 变红）
- SQL：
```sql
SELECT ts_min AS time,
       IFNULL(SUM(error_count) / NULLIF(SUM(request_count), 0), 0) AS error_rate
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.3 Token 吞吐 TPM（全局概览）

- 类型：timeseries
- SQL：
```sql
SELECT ts_min AS time, SUM(total_tokens) AS tokens_per_minute
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.4 限流命中（全局概览）

- 类型：timeseries
- SQL：
```sql
SELECT ts_min AS time, SUM(rate_limit_hits) AS rate_limit_hits
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.5 认证拒绝（全局概览）

- 类型：timeseries
- SQL：
```sql
SELECT ts_min AS time, SUM(auth_reject_count) AS auth_reject_count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.6 P50/P90/P99 全链路延迟（延迟分布）

- 类型：timeseries，单位 `ms`（p50 绿 / p90 橙 / p99 红）
- 数据表：detail，用 `PERCENTILE_APPROX`
- SQL：
```sql
SELECT $__timeGroup(log_time, '1m') AS time,
       PERCENTILE_APPROX(all_time, 0.50) AS p50,
       PERCENTILE_APPROX(all_time, 0.90) AS p90,
       PERCENTILE_APPROX(all_time, 0.99) AS p99
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND all_time IS NOT NULL
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
GROUP BY time ORDER BY time
```

### 3.7 TTFT 分位延迟（延迟分布，仅流式）

- 类型：timeseries，单位 `ms`
- SQL（`ai_ttft_us` 为微秒，除以 1000 转 ms；仅 `ai_stream = 1`）：
```sql
SELECT $__timeGroup(log_time, '1m') AS time,
       PERCENTILE_APPROX(ai_ttft_us, 0.50) / 1000 AS ttft_p50_ms,
       PERCENTILE_APPROX(ai_ttft_us, 0.90) / 1000 AS ttft_p90_ms,
       PERCENTILE_APPROX(ai_ttft_us, 0.99) / 1000 AS ttft_p99_ms
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ai_stream = 1 AND ai_ttft_us IS NOT NULL
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
GROUP BY time ORDER BY time
```

### 3.8 TPOT 分位延迟（延迟分布，仅流式）

- 类型：timeseries，单位 `ms`
- SQL 同 3.7，字段换 `ai_tpot_us`

### 3.9 Token 组成（Token 与成本，堆叠）

- 类型：timeseries，堆叠
- SQL（含音频 Token 与图片数）：
```sql
SELECT ts_min AS time,
       SUM(input_tokens)        AS input_tokens,
       SUM(output_tokens)       AS output_tokens,
       SUM(cache_read_tokens)   AS cache_read_tokens,
       SUM(cache_write_tokens)  AS cache_write_tokens,
       SUM(ai_audio_input_tokens)  AS ai_audio_input_tokens,
       SUM(ai_audio_output_tokens) AS ai_audio_output_tokens,
       SUM(ai_image_count)         AS ai_image_count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.10 成本趋势（Token 与成本）

- 类型：timeseries；按币种分系列
- SQL（`ai_cost_value_sum` 是固定点整数，除以 10000 仅为示例，实际精度由币种决定）：
```sql
SELECT ts_min AS time, ai_cost_currency, SUM(ai_cost_value_sum) AS cost
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ai_cost_currency != ''
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min, ai_cost_currency ORDER BY ts_min
```

### 3.11 各组件平均耗时（组件与重试，堆叠）

- 类型：barchart，堆叠，单位 `ms`
- SQL（detail 表 `AVG`）：
```sql
SELECT $__timeGroup(log_time, '30m') AS time,
       AVG(read_client_time)    AS avg_read_client,
       AVG(proxy_delay_time)    AS avg_proxy_delay,
       AVG(connect_backend_time) AS avg_connect,
       AVG(backend_serve_time)  AS avg_backend,
       AVG(cluster_serve_time)  AS avg_cluster_serve,
       AVG(write_client_time)   AS avg_write_client
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
GROUP BY time ORDER BY time
```

### 3.12 模型层重试次数（组件与重试）

- 类型：timeseries
- SQL：
```sql
SELECT ts_min AS time, SUM(ai_retry_count_sum) AS retries
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$provider' = '*' OR ai_provider = '$provider')
GROUP BY ts_min ORDER BY ts_min
```

### 3.13 按模型请求量（维度下钻）

- 类型：table
- SQL：
```sql
SELECT ai_requested_model, ai_target_model,
       SUM(request_count) AS total_requests,
       SUM(total_tokens)  AS total_tokens,
       SUM(error_count)   AS error_count,
       IFNULL(SUM(error_count) / NULLIF(SUM(request_count), 0), 0) AS error_rate
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
GROUP BY ai_requested_model, ai_target_model
ORDER BY total_requests DESC LIMIT 10
```

### 3.14 按提供商请求量（维度下钻）

- 类型：barchart（水平），显示数值标签（`showValue: always`）
- 说明：提供商请求量极端倾斜（单一提供商占比可达 99%+），改用水平条形图以便逐条看清每个提供商；并将 Top 10 之外的提供商合并为「其他」。
- SQL：
```sql
SELECT name, SUM(cnt) AS count
FROM (
  SELECT CASE WHEN rn <= 10 THEN ai_provider ELSE '其他' END AS name, cnt
  FROM (
    SELECT ai_provider, cnt,
           ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
    FROM (
      SELECT ai_provider, SUM(request_count) AS cnt
      FROM bfe_ai_metrics_1m
      WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_provider != ''
      GROUP BY ai_provider
    ) a
  ) b
) c
GROUP BY name
ORDER BY CASE WHEN name = '其他' THEN 1 ELSE 0 END, count DESC
```

### 3.15 按协议请求量（维度下钻）

- 类型：piechart
- SQL：
```sql
SELECT ai_protocol, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_protocol != ''
GROUP BY ai_protocol ORDER BY count DESC
```

### 3.16 按模式请求量（维度下钻）

- 类型：piechart
- SQL：
```sql
SELECT ai_mode, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_mode != ''
GROUP BY ai_mode ORDER BY count DESC
```

### 3.17 按 API Key Token 消耗（维度下钻）

- 类型：barchart（水平）
- SQL：
```sql
SELECT ai_apikey_id, SUM(total_tokens) AS total_tokens, SUM(request_count) AS requests
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_apikey_id != ''
GROUP BY ai_apikey_id ORDER BY total_tokens DESC LIMIT 20
```

### 3.18 按标签 Level1~Level5 请求量（维度下钻）

- 类型：barchart（水平）× 5
- 说明：按 API Key 标签的 5 个层级分别下钻（`levelNName` 为标签名、`levelN` 为标签值），SQL 结构相同，仅替换 `levelN` 字段。
- SQL（以 Level1 为例，Level2~Level5 同理替换字段名）：
```sql
SELECT CONCAT(level1Name, '=', level1) AS level1, SUM(request_count) AS requests
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND level1 != ''
GROUP BY level1Name, level1 ORDER BY requests DESC LIMIT 20
```

### 3.19 按 Host 请求量（维度下钻）

- 类型：barchart（水平）
- SQL：
```sql
SELECT header_host AS host, SUM(request_count) AS requests
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND header_host != '' AND ('$host' = '*' OR header_host = '$host')
GROUP BY header_host ORDER BY requests DESC LIMIT 10
```

### 3.20 按状态码分布（维度下钻）

- 类型：piechart
- SQL：
```sql
SELECT res_status_code, SUM(request_count) AS count
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
GROUP BY res_status_code ORDER BY count DESC
```

### 3.21 日志明细（明细回溯）

- 类型：table
- SQL（detail 表，倒序取最近 100 条）：
```sql
SELECT log_time, logid, ai_apikey_id, ai_requested_model, ai_target_model,
       res_status_code, err_code, all_time, ai_total_tokens
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
  AND ('$model' = '*' OR ai_target_model = '$model')
  AND (origin_uri LIKE '%$uri%')
ORDER BY log_time DESC LIMIT 100
```

### 3.22 限流命中展开（明细回溯）

- 类型：table
- SQL（`UNNEST` 展开 `ai_rate_limit_hits`）：
```sql
SELECT log_time, logid, ai_apikey_id, ai_target_model, origin_uri,
       hit.rate_limit_policy_id, hit.rate_limit_type, hit.rule_names
FROM bfe_ai_request_log
CROSS JOIN UNNEST(ai_rate_limit_hits) AS hit
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ARRAY_SIZE(ai_rate_limit_hits) > 0
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
ORDER BY log_time DESC LIMIT 100
```

### 3.23 认证拒绝明细（明细回溯）

- 类型：table
- SQL：
```sql
SELECT log_time, logid, ai_apikey_id, ai_auth_reject_reason, ai_auth_reject_quota_plans
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ai_auth_reject_reason IS NOT NULL AND ai_auth_reject_reason != ''
  AND ('$apikey' = '*' OR ai_apikey_id = '$apikey')
ORDER BY log_time DESC LIMIT 100
```

---

## 4. 通用配置

- `refresh: 30s`
- 默认时间范围 `now-6h` ~ `now`
- `timezone: browser`

## 5. 文档更新历史

| 日期 | 版本 | 变更说明 |
|------|------|----------|
| 2026-08-25 | v1.2 | 新增 `ai_protocol`、`ai_mode` 两个维度面板（按协议/按模式请求量），`Token 组成` 面板新增音频 Token（`ai_audio_input_tokens`/`ai_audio_output_tokens`）与图片数（`ai_image_count`）系列 |
| 2026-08-25 | v1.1 | 维度下钻新增「按标签 Level2/Level3/Level4/Level5 请求量」四个面板，与 Level1 组成 5 级标签下钻 |
| 2026-08-25 | v1.0 | 初始版本：基于 TABLE_DESIGN.md（v1.0）生成，字段对齐 `ai_target_model`/`ai_apikey_id`/`input_tokens`，新增成本、缓存、provider、level、重试等面板 |
