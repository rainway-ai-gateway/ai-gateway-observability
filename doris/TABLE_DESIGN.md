# BFE AI Gateway 可观测性 — Doris 表设计说明

本文档描述 `bfe_observability` 数据库中两张核心表的结构与语义。

> 数据库名可通过 `DORIS_DATABASE` 参数化（生产 `bfe_observability`，测试 `bfe_observability_test`）。下文中涉及数据库名的查询示例统一用 `bfe_observability`。

---

## 1. 总体设计

| 表 | 模型 | 粒度 | 用途 |
|----|------|------|------|
| `bfe_ai_request_log` | UNIQUE KEY（明细） | 单次请求 | 原始请求日志，用于明细检索、问题排查 |
| `bfe_ai_metrics_1m` | AGGREGATE KEY（聚合） | 1 分钟 | 分钟级聚合，用于看板、趋势、告警 |

- 明细表由 Routine Load（Kafka → Doris）实时写入。
- 聚合表由 INSERT JOB `bfe_ai_metrics_1m_job` 每分钟执行一次，把上一分钟明细聚合后写入。
- **Grafana 看板推荐优先使用 `bfe_ai_metrics_1m`**（数据量小、查询快）；需要单条明细时再用 `bfe_ai_request_log`。

---

## 2. 明细表 `bfe_ai_request_log`

### 2.1 表属性

| 属性 | 值 |
|------|----|
| 模型 | UNIQUE KEY |
| 主键 | `(hostid, log_time, ai_apikey_id, ai_requested_model)` |
| 分区 | `PARTITION BY RANGE(log_time)`，动态分区按天（保留前 7 天 + 后 3 天） |
| 分桶 | `DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32` |
| 压缩 | zstd |

### 2.2 字段说明

**基础 / 主键**

| 字段 | 类型 | 说明 | Grafana 提示 |
|------|------|------|--------------|
| `hostid` | VARCHAR(256) | 主机标识，格式 `hostname_netns` | 维度 |
| `log_time` | DATETIME | 日志产生时间 | **时间字段**，明细表用此字段做时间过滤 |
| `ai_apikey_id` | VARCHAR(256) | API Key ID（虚拟 Key，非原始 Key） | 维度 |
| `ai_requested_model` | VARCHAR(128) | 请求模型名（客户端请求的模型） | 维度 |
| `logid` | BIGINT | BFE 请求唯一标识 | — |
| `product` | VARCHAR(64) | 产品标识 | 维度 |
| `log_tag` | VARCHAR(64) | `req_<product>`（正常）/ `req_err_<product>`（错误） | — |

**客户端连接**

| 字段 | 类型 | 说明 | 单位/取值 |
|------|------|------|-----------|
| `client_ip` | VARCHAR(64) | 客户端 IP | — |
| `client_network` | VARCHAR(16) | 客户端网络类型 | `Ipv4` / `Ipv6` |
| `is_trust_src_ip` | TINYINT | 是否可信源 IP | 0 / 1 |
| `req_num` | INT | 会话内请求序号 | 从 1 开始 |
| `session_id` | BIGINT | 会话 ID | — |
| `bfe_ip` | VARCHAR(64) | BFE 服务器 IP | 基础设施维度 |
| `sock_src_ip` | VARCHAR(64) | Socket 源 IP | — |
| `vip` | VARCHAR(64) | 目的 VIP | — |
| `vip6` | VARCHAR(128) | 目的 VIP6 | — |

**错误信息**

| 字段 | 类型 | 说明 | 取值 |
|------|------|------|------|
| `err_code` | VARCHAR(64) | 错误码 | 空串=正常；常见 `AI_RATE_LIMIT`、`AI_AUTH_REJECT` |
| `err_msg` | VARCHAR(512) | 错误详情 | — |

**请求头**

| 字段 | 类型 | 说明 |
|------|------|------|
| `proto` | VARCHAR(16) | HTTP 协议版本（`HTTP/1.1` 等） |
| `header_host` | VARCHAR(256) | 请求 Host |
| `origin_uri` | VARCHAR(2048) | 原始请求 URI |
| `final_uri` | VARCHAR(2048) | 最终路由 URI |
| `method` | VARCHAR(16) | HTTP 方法（`GET`/`POST`/...） |
| `content_type` | VARCHAR(128) | 请求 Content-Type |
| `x_forward_for` | VARCHAR(1024) | X-Forwarded-For |
| `accept_language` | VARCHAR(256) | Accept-Language |
| `authorization` | VARCHAR(1024) | Authorization 头 |
| `transfer_encoding` | VARCHAR(64) | Transfer-Encoding |
| `referrer` | VARCHAR(2048) | Referer 头 |
| `user_agent` | VARCHAR(1024) | User-Agent 头 |
| `delegation` | VARCHAR(256) | 委托域名 |
| `uid` | VARCHAR(256) | UID 头 |
| `cookie` | VARCHAR(4096) | Cookie 头 |
| `req_headers` | ARRAY\<STRUCT\<`key`, `value`\>\> | 请求头列表（key/value 对） |
| `req_header_len` | INT | 请求头长度 | 字节 |
| `req_body_len` | INT | 请求体长度 | 字节 |

**路由 / 响应**

| 字段 | 类型 | 说明 | 取值 |
|------|------|------|------|
| `cluster` | VARCHAR(256) | 目标集群 | 维度 |
| `sub_cluster` | VARCHAR(256) | 目标子集群 | 维度 |
| `backend_info` | VARCHAR(256) | 后端 `IP:Port` | — |
| `backend_retry` | TINYINT | 后端重试次数 | 次数 |
| `res_status_code` | SMALLINT | 响应状态码 | HTTP 状态码 |
| `res_header_len` | INT | 响应头长度 | 字节 |
| `res_body_len` | INT | 响应体长度 | 字节 |
| `res_content_type` | VARCHAR(128) | 响应 Content-Type | — |
| `res_location` | VARCHAR(2048) | 响应 Location（3xx） | — |
| `res_transfer_encoding` | VARCHAR(64) | 响应 Transfer-Encoding | — |
| `res_headers` | ARRAY\<STRUCT\<`key`, `value`\>\> | 响应头列表 | — |

**耗时（单位：毫秒 ms）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `all_time` | INT | 请求总耗时 |
| `read_client_time` | INT | 读客户端耗时 |
| `cluster_serve_time` | INT | 集群层耗时 |
| `backend_serve_time` | INT | 后端耗时 |
| `write_client_time` | INT | 写客户端耗时 |
| `connect_backend_time` | INT | 连接后端耗时 |
| `proxy_delay_time` | INT | 代理延迟 |
| `session_offset_time` | INT | 会话内时间偏移 |

**AI — API Key 标签（按层级打平，5 级）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `level1Name` / `level1` | VARCHAR(128) | Level1 标签名 / 值 |
| `level2Name` / `level2` | VARCHAR(128) | Level2 标签名 / 值 |
| `level3Name` / `level3` | VARCHAR(128) | Level3 标签名 / 值 |
| `level4Name` / `level4` | VARCHAR(128) | Level4 标签名 / 值 |
| `level5Name` / `level5` | VARCHAR(128) | Level5 标签名 / 值 |

> 来源是 API Key 的 `ai_apikeytags`（对象，`level1~level5`），Routine Load 时打平成 10 个固定列。`level*Name` 是标签名（如 `dep`、`team`），`level*` 是标签值（如 `rd`、`bfe`）。未设置的层级为 NULL/空。

**AI — 可观测指标**

| 字段 | 类型 | 说明 | 单位/取值 |
|------|------|------|-----------|
| `ai_target_model` | VARCHAR(128) | 实际路由模型名 | 维度 |
| `ai_stream` | TINYINT | 是否流式 | 0=非流式，1=流式 |
| `ai_input_tokens` | BIGINT | 输入 Token 数 | 个 |
| `ai_output_tokens` | BIGINT | 输出 Token 数 | 个 |
| `ai_total_tokens` | BIGINT | 总 Token 数 | 个 |
| `ai_cache_read_tokens` | BIGINT | 缓存读取 Token 数 | 个 |
| `ai_cache_write_tokens` | BIGINT | 缓存写入 Token 数 | 个 |
| `ai_audio_input_tokens` | BIGINT | 音频输入 Token 数 | 个 |
| `ai_audio_output_tokens` | BIGINT | 音频输出 Token 数 | 个 |
| `ai_image_count` | BIGINT | 图片数量 | 个 |
| `ai_ttft_us` | BIGINT | 首 Token 延迟 TTFT | **微秒 µs** |
| `ai_tpot_us` | BIGINT | 每 Token 延迟 TPOT | **微秒 µs** |
| `ai_provider` | VARCHAR(64) | 上游模型提供商（如 `openai`、`deepseek`） | 维度 |
| `ai_protocol` | VARCHAR(64) | AI 协议（上游 API 协议类型） | 维度 |
| `ai_mode` | VARCHAR(64) | AI 模式（如 `chat`/`audio`/`image`） | 维度 |
| `ai_retry_count` | INT | 模型调用层重试次数 | 次 |
| `ai_cost_value` | BIGINT | 成本固定点整数值 | 精度取决于 `ai_cost_currency` |
| `ai_cost_currency` | VARCHAR(16) | 成本币种 | `RMB` / `USD` |

**AI — 复杂类型（ARRAY）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ai_route_rule_hits` | ARRAY\<STRUCT\<`rule_owner`,`rule_owner_type`,`rule_name`\>\> | AI 路由规则命中记录 |
| `ai_cluster_key_names` | ARRAY\<STRUCT\<`cluster_name`,`key_name`\>\> | 尝试过的集群与 Key 组合 |
| `ai_rate_limit_hits` | ARRAY\<STRUCT\<`rate_limit_policy_id`,`rate_limit_type`,`rule_names`\>\> | 限流命中列表 |
| `ai_auth_reject_reason` | VARCHAR(256) | 认证拒绝原因 |
| `ai_auth_reject_quota_plans` | ARRAY\<VARCHAR(128)\> | 被拒绝的配额计划 |
| `ai_auth_hit_quota_plans` | ARRAY\<VARCHAR(128)\> | 成功请求时命中的配额计划 |

---

## 3. 聚合表 `bfe_ai_metrics_1m`

### 3.1 表属性

| 属性 | 值 |
|------|----|
| 模型 | AGGREGATE KEY |
| 聚合维度 | 见 3.2（37 个维度列） |
| 聚合指标 | SUM 类型，见 3.3（24 个指标列） |
| 分区 | `PARTITION BY RANGE(ts_min)`，动态分区按天 |
| 分桶 | `DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 16` |

### 3.2 维度字段（AGGREGATE KEY）

| 字段 | 类型 | 说明 |
|------|------|------|
| `ts_min` | DATETIME | **分钟时间桶**（时间字段，聚合表用此字段做时间过滤） |
| `hostid` | VARCHAR(256) | 主机标识 |
| `ai_apikey_id` | VARCHAR(128) | API Key ID |
| `ai_requested_model` | VARCHAR(128) | 请求模型 |
| `ai_target_model` | VARCHAR(128) | 路由模型 |
| `ai_stream` | TINYINT | 流式标识（0/1） |
| `product` | VARCHAR(64) | 产品线 |
| `cluster` | VARCHAR(64) | 集群 |
| `sub_cluster` | VARCHAR(64) | 子集群 |
| `backend_info` | VARCHAR(256) | 后端节点 |
| `method` | VARCHAR(16) | HTTP 方法 |
| `res_status_code` | SMALLINT | 响应状态码 |
| `err_code` | VARCHAR(64) | 错误码（空=正常） |
| `header_host` | VARCHAR(256) | 请求 Host |
| `ai_provider` | VARCHAR(64) | 上游模型提供商 |
| `ai_protocol` | VARCHAR(64) | AI 协议 |
| `ai_mode` | VARCHAR(64) | AI 模式 |
| `ai_cost_currency` | VARCHAR(16) | 成本币种 |
| `level1Name` ~ `level5Name` | VARCHAR(128) | 标签层级名 ×5 |
| `level1` ~ `level5` | VARCHAR(128) | 标签层级值 ×5 |
| `rate_limit_policy_id` | VARCHAR(128) | 限流策略 ID（取首个命中） |
| `rate_limit_type` | VARCHAR(32) | 限流类型（`tpm`/`rpm`/`concurrency`） |
| `rate_limit_rule_name` | VARCHAR(128) | 限流规则名（取首条规则） |
| `ai_auth_reject_reason` | VARCHAR(256) | 认证拒绝原因 |
| `ai_auth_reject_quota_plans_slot1~slot5` | VARCHAR(128) | 被拒绝配额计划槽位 ×5 |

### 3.3 聚合指标字段（SUM）

| 字段 | 类型 | 说明 | 单位 |
|------|------|------|------|
| `request_count` | BIGINT SUM | 请求数 | 次 |
| `error_count` | BIGINT SUM | 错误数（`err_code` 非空） | 次 |
| `auth_reject_count` | BIGINT SUM | 认证拒绝数 | 次 |
| `input_tokens` | BIGINT SUM | 输入 Token 累计 | 个 |
| `output_tokens` | BIGINT SUM | 输出 Token 累计 | 个 |
| `total_tokens` | BIGINT SUM | 总 Token 累计 | 个 |
| `cache_read_tokens` | BIGINT SUM | 缓存读取 Token 累计 | 个 |
| `cache_write_tokens` | BIGINT SUM | 缓存写入 Token 累计 | 个 |
| `ttft_us_sum` | BIGINT SUM | TTFT 累计 | 微秒 |
| `tpot_us_sum` | BIGINT SUM | TPOT 累计 | 微秒 |
| `req_header_bytes` | BIGINT SUM | 请求头字节累计 | 字节 |
| `req_body_bytes` | BIGINT SUM | 请求体字节累计 | 字节 |
| `res_header_bytes` | BIGINT SUM | 响应头字节累计 | 字节 |
| `res_body_bytes` | BIGINT SUM | 响应体字节累计 | 字节 |
| `rate_limit_hits` | BIGINT SUM | 限流命中次数 | 次 |
| `backend_retries` | BIGINT SUM | 后端重试总次数 | 次 |
| `all_time_sum` | BIGINT SUM | 总耗时累计 | 毫秒 |
| `cluster_serve_sum` | BIGINT SUM | 集群层耗时累计 | 毫秒 |
| `backend_serve_sum` | BIGINT SUM | 后端耗时累计 | 毫秒 |
| `ai_retry_count_sum` | BIGINT SUM | 模型层重试总次数 | 次 |
| `ai_cost_value_sum` | BIGINT SUM | 成本累计（固定点整数） | — |
| `ai_audio_input_tokens` | BIGINT SUM | 音频输入 Token 累计 | 个 |
| `ai_audio_output_tokens` | BIGINT SUM | 音频输出 Token 累计 | 个 |
| `ai_image_count` | BIGINT SUM | 图片数量累计 | 个 |

---

## 4. 明细表 → 聚合表 映射

INSERT JOB 每分钟执行，逻辑如下（关键映射）：

| 聚合表字段 | 来源（明细表） |
|-----------|---------------|
| `ts_min` | `DATE_TRUNC(log_time, 'minute')` |
| 维度列 | 对应明细表同名字段，`COALESCE(x, '')` 或 `COALESCE(x, 0)`（空值归一化） |
| `level*Name/level*` | 直接来自明细表打平列 |
| `rate_limit_policy_id/type/rule_name` | `ai_rate_limit_hits[1]` 的首个元素（`ELEMENT_AT(...,1)`） |
| `ai_auth_reject_quota_plans_slot1~5` | `ai_auth_reject_quota_plans` 的前 5 个元素 |
| `ai_protocol` / `ai_mode` | 明细表同名字段，`COALESCE(x, '')` |
| `ai_audio_input_tokens` / `ai_audio_output_tokens` / `ai_image_count` | `SUM(COALESCE(明细字段, 0))` |
| `request_count` | `COUNT(1)` |
| `error_count` | `SUM(err_code 非空 ? 1 : 0)` |
| `auth_reject_count` | `SUM(ai_auth_reject_reason 非空 ? 1 : 0)` |
| 其余 `*_sum` / `*_tokens` | `SUM(COALESCE(明细字段, 0))` |

> 注意：`rate_limit_hits` 是「命中限流的请求数」（`ARRAY_SIZE(ai_rate_limit_hits) > 0`），而非命中的规则条数。

---

## 5. Grafana 查询指南

### 5.1 选表与时间字段

| 场景 | 用表 | 时间字段 |
|------|------|----------|
| 看板 / 趋势 / 告警 | `bfe_ai_metrics_1m` | `ts_min` |
| 单条明细排查 | `bfe_ai_request_log` | `log_time` |

时间过滤宏（Grafana MySQL 数据源）：
```sql
WHERE $__timeFilter(ts_min)            -- 聚合表
WHERE $__timeFilter(log_time)          -- 明细表
```

### 5.2 常用聚合示例

聚合表是 AGGREGATE KEY（指标为 SUM 类型），跨时间/维度聚合时**对指标列求和即可**；求平均值用「sum / request_count」。

```sql
-- 1) QPS（每分钟请求数）
SELECT ts_min AS time, SUM(request_count) AS qps
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ts_min ORDER BY ts_min;

-- 2) 请求量按模型
SELECT ai_requested_model, SUM(request_count) AS cnt
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ai_requested_model ORDER BY cnt DESC;

-- 3) 错误率（%）
SELECT ts_min AS time,
       SUM(error_count) / SUM(request_count) * 100 AS error_rate
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ts_min ORDER BY ts_min;

-- 4) 平均 TTFT（微秒）/ TPOT（微秒）/ 总耗时（毫秒）
SELECT ts_min AS time,
       SUM(ttft_us_sum)  / NULLIF(SUM(request_count),0) AS avg_ttft_us,
       SUM(tpot_us_sum)  / NULLIF(SUM(request_count),0) AS avg_tpot_us,
       SUM(all_time_sum) / NULLIF(SUM(request_count),0) AS avg_latency_ms
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ts_min ORDER BY ts_min;

-- 5) Token 消耗 / 成本
SELECT ts_min AS time,
       SUM(input_tokens)  AS input_tokens,
       SUM(output_tokens) AS output_tokens,
       SUM(ai_cost_value_sum) AS cost
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ts_min ORDER BY ts_min;

-- 6) 限流命中
SELECT ts_min AS time, SUM(rate_limit_hits) AS rate_limited
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ts_min ORDER BY ts_min;

-- 7) 认证拒绝
SELECT ts_min AS time, SUM(auth_reject_count) AS auth_rejected
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ts_min ORDER BY ts_min;
```

### 5.3 标签层级（level）使用

标签层级是多级维度，可按需 `GROUP BY` 某一级：

```sql
-- 按 Level1 标签值（如部门 dep）统计请求量
SELECT level1Name, level1, SUM(request_count) AS cnt
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY level1Name, level1 ORDER BY cnt DESC;
```

### 5.4 流式 / 非流式

```sql
SELECT ai_stream, SUM(request_count) AS cnt
FROM bfe_ai_metrics_1m
WHERE $__timeFilter(ts_min)
GROUP BY ai_stream;   -- 0=非流式, 1=流式
```

---

## 6. 空值与特殊值约定

| 类型 | 空值/默认 |
|------|-----------|
| 维度字符串（`err_code`、`ai_auth_reject_reason` 等） | 空串 `''` 表示「无 / 未命中」，聚合表已用 `COALESCE(..., '')` 归一化 |
| 数值字段（tokens、耗时、长度） | `0` 或 `NULL` |
| `ai_stream` / `is_trust_src_ip` | TINYINT，0 / 1 |
| 复杂类型（ARRAY） | 明细表为 `NULL` 或 `[]`；聚合表已打平成标量 |

**判断错误**：`err_code != '' AND err_code IS NOT NULL`
**判断认证拒绝**：`ai_auth_reject_reason != '' AND ai_auth_reject_reason IS NOT NULL`
**判断限流**：聚合表用 `rate_limit_hits > 0`，明细表用 `ARRAY_SIZE(ai_rate_limit_hits) > 0`

---

## 7. 字段语义速查（单位 / 枚举）

| 类别 | 字段 | 单位 / 取值 |
|------|------|-------------|
| 耗时（明细） | `all_time`、`read_client_time`、`cluster_serve_time`、`backend_serve_time`、`write_client_time`、`connect_backend_time`、`proxy_delay_time`、`session_offset_time` | 毫秒 ms |
| 耗时（聚合） | `all_time_sum`、`cluster_serve_sum`、`backend_serve_sum` | 毫秒 ms |
| 首字/每字延迟 | `ai_ttft_us`、`ttft_us_sum`、`ai_tpot_us`、`tpot_us_sum` | 微秒 µs |
| 长度 | `req_header_len`、`req_body_len`、`res_header_len`、`res_body_len` 及聚合 `*_bytes` | 字节 |
| Token | `ai_input_tokens`、`ai_output_tokens`、`ai_total_tokens`、`ai_cache_read_tokens`、`ai_cache_write_tokens`、`ai_audio_input_tokens`、`ai_audio_output_tokens` 及聚合 | 个 |
| 计数 | `request_count`、`error_count`、`auth_reject_count`、`rate_limit_hits`、`backend_retries`、`ai_retry_count_sum` | 次 |
| 图片 | `ai_image_count` 及聚合 | 个 |
| 流式 | `ai_stream` | 0=非流式，1=流式 |
| 网络类型 | `client_network` | `Ipv4` / `Ipv6` |
| 限流类型 | `rate_limit_type` | `tpm` / `rpm` / `concurrency` |
| 币种 | `ai_cost_currency` | `RMB` / `USD` |

---

## 8. 文档更新历史

| 日期 | 版本 | 变更说明 |
|------|------|----------|
| 2026-08-25 | v1.0 | 初始版本：最新 `bfe_ai_request_log`、`bfe_ai_metrics_1m` 表结构 |
| 2026-08-25 | v1.1 | 新增 `ai_protocol`、`ai_mode`（聚合表维度列）与 `ai_audio_input_tokens`、`ai_audio_output_tokens`、`ai_image_count`（明细表 + 聚合表指标列） |
