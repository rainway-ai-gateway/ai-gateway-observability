# BFE AI Gateway 可观测性 — ClickHouse 表设计说明

本文档描述 `bfe_observability` 数据库中两张核心表及导入/聚合链路上各对象（Kafka 引擎表、消费 MV、聚合 MV）的结构与语义。事实来源为 `clickhouse/sqls/` 下六份 SQL 资产，设计依据见修改说明 [2026-10-01-clickhouse-dock](../modifications/2026-10-01-clickhouse-dock/design-changes.md) 与《数据报表-ClickHouse 与 StarRocks 对接设计方案（v0.8）》。

> 数据库名可通过 `CLICKHOUSE_DATABASE` 参数化（生产 `bfe_observability`，测试 `bfe_observability_test`）。下文中涉及数据库名的查询示例统一用 `bfe_observability`。

---

## 1. 总体设计

| 对象 | 类型 | 粒度 | 用途 |
|------|------|------|------|
| `bfe_ai_request_log` | MergeTree（明细表） | 单次请求 | 原始请求日志，用于明细检索、问题排查 |
| `bfe_ai_log_kafka` | Kafka 引擎暂存表 | Kafka 消息 | 消息暂存，不落盘；schema 与 Kafka JSON 同构 |
| `bfe_ai_log_load_mv` | 消费物化视图 | — | 从 Kafka 表读消息，完成 JSON 打平与 UTC 换算后写入明细表 |
| `bfe_ai_metrics_1m` | SummingMergeTree（聚合表） | 1 分钟 | 分钟级聚合，用于看板、趋势、告警 |
| `bfe_ai_metrics_1m_mv` | 聚合物化视图 | — | 明细表插入时同步触发分钟聚合，写入聚合表 |

- 明细表由 **Kafka 引擎表 + 消费 MV** 实时写入（等价 Doris Routine Load）。
- 聚合表由 **聚合 MV** 随明细插入同步触发写入（等价 Doris 每分钟 INSERT JOB，延迟从分钟级降到秒级）。
- 看板/趋势/告警推荐优先使用 `bfe_ai_metrics_1m`（数据量小、查询快）；需要单条明细时再用 `bfe_ai_request_log`。

### 1.1 与 Doris 机制对照

| Doris 机制 | ClickHouse 等价物 | 说明 |
|------------|-------------------|------|
| UNIQUE KEY（明细表主键四列） | MergeTree `ORDER BY (hostid, log_time, ai_apikey_id, ai_requested_model)` | 对齐的是排序键列序，**不约束唯一性**；日志 append-only 无更新语义，不设 ReplacingMergeTree |
| AGGREGATE KEY + SUM 指标 | SummingMergeTree（显式 24 指标求和列） | 相同完整排序键的行仅在后台 merge 时合并求和，查询侧必须 `GROUP BY + sum()` 兜底（见 §3.4） |
| Routine Load 的 COLUMNS 映射 | 消费 MV 的 SELECT 投影 | JSON 打平（level×10、rate_limit×3）与 UTC 墙钟换算在 MV 内完成 |
| `CREATE JOB` 每分钟 INSERT SELECT | 聚合 MV `TO bfe_ai_metrics_1m` | `toStartOfMinute` 天然对齐分钟桶，无需 JOB 的时间窗谓词 |
| 动态分区 `start=-7` | `TTL ... + INTERVAL 7 DAY` | 两表均为 7 天保留 |

### 1.2 设计原则（与 Doris 逐条对齐的口径）

1. 两表与 Doris 同名同列：明细表在 Doris 版基础上增加 level×10、rate_limit×3 共 13 个打平列（Doris 侧同样存在，由 Routine Load 打平）。
2. 聚合表 40 维 + 24 指标，逐列同名同口径。
3. UTC 墙钟：`toDateTime(timestamp, 'UTC')`，列类型 `DateTime('UTC')`，无时区歧义。
4. 7 天保留：两表均 `TTL ... + INTERVAL 7 DAY`，按天分区。

---

## 2. 明细表 `bfe_ai_request_log`

### 2.1 表属性

| 属性 | 值 |
|------|----|
| 引擎 | `MergeTree`（不设 ReplacingMergeTree：append-only 无更新语义） |
| 排序键 | `ORDER BY (hostid, log_time, ai_apikey_id, ai_requested_model)` —— 对齐 Doris UNIQUE KEY 四列 |
| 分区 | `PARTITION BY toDate(log_time)`，按天 |
| TTL | `TTL log_time + INTERVAL 7 DAY`（对齐 Doris 动态分区 `start=-7`） |

> **排序键 ≠ 唯一键**：MergeTree 排序键只做索引与数据局部性，不拒绝重复行。Kafka 消费为 at-least-once，重投会产生重复明细（见 §3.4 与 HOWTO §9.3）。

### 2.2 字段说明

**基础 / 排序键**

| 字段 | 类型 | 说明 |
|------|------|------|
| `hostid` | String | 主机标识，格式 `hostname_netns` |
| `log_time` | DateTime('UTC') | 日志产生时间（**UTC 墙钟**，由消费 MV 以 `toDateTime(timestamp, 'UTC')` 写入；**时间字段**，明细表用此字段做时间过滤） |
| `ai_apikey_id` | String | API Key ID（虚拟 Key，非原始 Key） |
| `ai_requested_model` | String | 请求模型名（客户端请求的模型） |
| `logid` | **UInt64** | BFE 请求唯一标识（类型偏差说明见 §2.3） |
| `product` | String | 产品标识 |
| `log_tag` | String | `req_<product>`（正常）/ `req_err_<product>`（错误） |

**客户端连接**

| 字段 | 类型 | 说明 |
|------|------|------|
| `client_ip` | String | 客户端 IP |
| `client_network` | String | 客户端网络类型，`Ipv4` / `Ipv6` |
| `is_trust_src_ip` | UInt8 | 是否可信源 IP（0/1） |
| `req_num` | Int32 | 会话内请求序号，从 1 开始 |
| `session_id` | Int64 | 会话 ID |
| `bfe_ip` | String | BFE 服务器 IP |
| `sock_src_ip` | String | Socket 源 IP |
| `vip` | String | 目的 VIP |
| `vip6` | String | 目的 VIP6 |

**错误信息**

| 字段 | 类型 | 说明 |
|------|------|------|
| `err_code` | String | 错误码，空串=正常；常见 `AI_RATE_LIMIT`、`AI_AUTH_REJECT` |
| `err_msg` | Nullable(String) | 错误详情（Nullable，见 §2.3） |

**请求头**

| 字段 | 类型 | 说明 |
|------|------|------|
| `proto` | String | HTTP 协议版本（`HTTP/1.1` 等） |
| `header_host` | String | 请求 Host |
| `origin_uri` | String | 原始请求 URI |
| `final_uri` | String | 最终路由 URI |
| `method` | String | HTTP 方法（`GET`/`POST`/...） |
| `content_type` | String | 请求 Content-Type |
| `x_forward_for` | String | X-Forwarded-For |
| `accept_language` | String | Accept-Language |
| `authorization` | String | Authorization 头 |
| `transfer_encoding` | String | Transfer-Encoding |
| `referrer` | String | Referer 头 |
| `user_agent` | String | User-Agent 头 |
| `delegation` | String | 委托域名 |
| `uid` | String | UID 头 |
| `cookie` | String | Cookie 头 |
| `req_headers` | Nested(`key` String, `value` String) | 请求头列表（key/value 对，等价 Doris `ARRAY<STRUCT>`） |
| `req_header_len` | Int32 | 请求头长度（字节） |
| `req_body_len` | Int32 | 请求体长度（字节） |

**路由 / 响应**

| 字段 | 类型 | 说明 |
|------|------|------|
| `cluster` | String | 目标集群 |
| `sub_cluster` | String | 目标子集群 |
| `backend_info` | String | 后端 `IP:Port` |
| `backend_retry` | Int8 | 后端重试次数 |
| `res_status_code` | Int16 | 响应状态码 |
| `res_header_len` | Int32 | 响应头长度（字节） |
| `res_body_len` | Int32 | 响应体长度（字节） |
| `res_content_type` | String | 响应 Content-Type |
| `res_location` | String | 响应 Location（3xx） |
| `res_transfer_encoding` | String | 响应 Transfer-Encoding |
| `res_headers` | Nested(`key` String, `value` String) | 响应头列表 |

**耗时（单位：毫秒 ms）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `all_time` | Int32 | 请求总耗时 |
| `read_client_time` | Int32 | 读客户端耗时 |
| `cluster_serve_time` | Int32 | 集群层耗时 |
| `backend_serve_time` | Int32 | 后端耗时 |
| `write_client_time` | Int32 | 写客户端耗时 |
| `connect_backend_time` | Int32 | 连接后端耗时 |
| `proxy_delay_time` | Int32 | 代理延迟 |
| `session_offset_time` | Int32 | 会话内时间偏移 |

**AI — API Key 标签（按层级打平，5 级；导入层打平列）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `level1Name` / `level1` | String | Level1 标签名 / 值 |
| `level2Name` / `level2` | String | Level2 标签名 / 值 |
| `level3Name` / `level3` | String | Level3 标签名 / 值 |
| `level4Name` / `level4` | String | Level4 标签名 / 值 |
| `level5Name` / `level5` | String | Level5 标签名 / 值 |

> 来源是 API Key 的 `ai_apikeytags`（JSON 对象，`level1~level5`），由消费 MV 以 `JSONExtractString` 打平成 10 个固定列（Doris 侧由 Routine Load `json_extract` 打平，同一模式）。`level*Name` 是标签名（如 `dep`、`team`），`level*` 是标签值（如 `rd`、`bfe`）。未设置的层级为空对象/缺失路径，打平结果为 `''`。

**AI — 可观测指标**

| 字段 | 类型 | 说明 | 单位/取值 |
|------|------|------|-----------|
| `ai_target_model` | String | 实际路由模型名 | — |
| `ai_stream` | UInt8 | 是否流式 | 0=非流式，1=流式 |
| `ai_input_tokens` | Int64 | 输入 Token 数 | 个 |
| `ai_output_tokens` | Int64 | 输出 Token 数 | 个 |
| `ai_total_tokens` | Int64 | 总 Token 数 | 个 |
| `ai_cache_read_tokens` | Int64 | 缓存读取 Token 数 | 个 |
| `ai_cache_write_tokens` | Int64 | 缓存写入 Token 数 | 个 |
| `ai_audio_input_tokens` | Int64 | 音频输入 Token 数 | 个 |
| `ai_audio_output_tokens` | Int64 | 音频输出 Token 数 | 个 |
| `ai_image_count` | Int64 | 图片数量 | 个 |
| `ai_ttft_us` | Int64 | 首 Token 延迟 TTFT | **微秒 µs** |
| `ai_tpot_us` | Int64 | 每 Token 延迟 TPOT | **微秒 µs** |
| `ai_provider` | String | 上游模型提供商（如 `openai`、`deepseek`） | — |
| `ai_protocol` | String | AI 协议（上游 API 协议类型） | — |
| `ai_mode` | String | AI 模式（如 `chat`/`audio`/`image`） | — |
| `ai_retry_count` | Int32 | 模型调用层重试次数 | 次 |
| `ai_cost_value` | Int64 | 成本固定点整数值 | 精度取决于 `ai_cost_currency` |
| `ai_cost_currency` | String | 成本币种 | `RMB` / `USD` |

**AI — 复杂类型（Array / Tuple）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ai_route_rule_hits` | Array(Tuple(`rule_owner` String, `rule_owner_type` String, `rule_name` String)) | AI 路由规则命中记录 |
| `ai_cluster_key_names` | Array(Tuple(`cluster_name` String, `key_name` String)) | 尝试过的集群与 Key 组合 |
| `ai_rate_limit_hits` | Array(Tuple(`rate_limit_policy_id` String, `rate_limit_type` String, `rule_names` Array(String))) | 限流命中列表（唯一三层嵌套） |
| `rate_limit_policy_id` | String | 限流策略 ID（取首个命中，**打平列**） |
| `rate_limit_type` | String | 限流类型（取首个命中，**打平列**） |
| `rate_limit_rule_name` | String | 限流规则名（取首条规则，**打平列**） |
| `ai_auth_reject_reason` | String | 认证拒绝原因 |
| `ai_auth_reject_quota_plans` | Array(String) | 被拒绝的配额计划 |
| `ai_auth_hit_quota_plans` | Array(String) | 成功请求时命中的配额计划 |

> 限流打平列：Doris 3.0 不支持 `ARRAY<STRUCT>` 元素字段解引用，由 Routine Load 在导入时 `json_extract` 打平；ClickHouse 具备 Tuple 具名字段访问能力，打平在消费 MV 内以 `ai_rate_limit_hits[1].rate_limit_policy_id` 完成（带 length 守卫，见 §2.3 对照表）。INSERT/查询层直接使用打平列。

**AI — 缓存/镜像/意图（v0.8 报表二期）**

| 字段 | 类型 | 说明 | 单位/取值 |
|------|------|------|-----------|
| `ai_cache_status` | String | 缓存状态 | `hit`/`miss`/`skip`，空=未启用 |
| `mirror_hit` | UInt8 | 镜像是否命中 | 0/1 |
| `mirror_cluster` | String | 镜像集群 | — |
| `ai_intent_question` | String | 意图问题 | — |
| `ai_intent_answer` | String | 意图答案 | 含 `unknown`，空=未分类 |
| `ai_intent_confidence` | Nullable(Float64) | 意图置信度 | NULL=未分类 |
| `ai_intent_source` | String | 意图来源 | `explicit_header`/`classifier`/`cache` |
| `ai_intent_latency_us` | Nullable(Int64) | 意图决策耗时 | 微秒 µs，NULL=未分类 |
| `ai_intent_cache_hit` | Nullable(UInt8) | 意图缓存命中 | NULL=未分类，0/1 |
| `ai_intent_questions_version` | String | 意图问题集版本 | — |

### 2.3 ClickHouse 特有设计

**（a）`logid` 为 UInt64：与 Doris 的取值偏差**

样例/线上 `logid`（如 `10602749765076101032`、`12345678901234567890`）超出 Int64 上限（`9223372036854775807`）。Doris 以 BIGINT 承载、非严格模式下超界值被**截断为边界值**存储；ClickHouse 直接以 `UInt64` 承载原值。**两库 `logid` 取值口径存在偏差**，按 `logid` 明细检索或对账时需注意（Doris 侧大 logid 只能取到 Int64 最大值）。

**（b）Nullable 四列与 `DEFAULT ''`/`0` 归一口径**

`Nullable` 仅限 Doris 语义允许 NULL 的列：`err_msg`、`ai_intent_confidence`、`ai_intent_latency_us`、`ai_intent_cache_hit`（NULL 表示"未发生/未分类"）。其余字符串列 `DEFAULT ''`、数值列 `DEFAULT 0`，对应 Doris 侧 `COALESCE(x,'')` / `COALESCE(x,0)` 的归一口径，保证 `col != ''` 等既有谓词行为一致。Kafka JSON 缺失字段（LogReader `omitempty` 零值不输出）经 JSONEachRow 落为默认值/NULL，语义不变。

**（c）打平列在消费 MV 内完成（Doris ↔ ClickHouse 表达式对照）**

| 口径 | Doris Routine Load（COLUMNS） | ClickHouse 消费 MV（`bfe_ai_log_load_mv`） |
|------|------------------------------|--------------------------------------------|
| UTC 墙钟 | `DATE_SUB(FROM_UNIXTIME(timestamp), INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND)` | `toDateTime(timestamp, 'UTC')`（列固定 UTC，无时区歧义） |
| level 打平（×10） | `json_unquote(json_extract(ai_apikeytags, '$.level1.tagname'))` | `JSONExtractString(ai_apikeytags, 'level1', 'tagname')`（空对象/缺失路径返回 `''`，Doris 侧为 NULL，谓词行为一致） |
| 限流打平（×3） | `json_unquote(json_extract(ai_rate_limit_hits, '$[0].rate_limit_policy_id'))` 等 | `if(length(ai_rate_limit_hits) >= 1, ai_rate_limit_hits[1].rate_limit_policy_id, '')` 等（Tuple 具名字段访问 + length 守卫，空数组返回 `''`） |
| 源字段不落表 | `ai_apikeytags` 仅参与表达式求值 | 同：MV SELECT 不投影 `ai_apikeytags` / `timestamp` |

**（d）导入层对象**

- **`bfe_ai_log_kafka`（Kafka 引擎暂存表）**：schema 与 Kafka JSON 消息同构（同名同型）；`ai_apikeytags` 源 JSON 对象以 `JSON` 类型承载，仅在 MV 内打平。引擎设置：`kafka_format = 'JSONEachRow'`、`input_format_skip_unknown_fields = 1`（PB/log-reader 新增字段先于 DDL 到达时跳过而非报错，消费不中断）、`kafka_num_consumers = 3`、独立消费组 `clickhouse_bfe_ai_log`（与 Doris/StarRocks 位点互不影响）。Kafka 引擎表只做消息暂存，不落盘存储。
- **`bfe_ai_log_load_mv`（消费 MV）**：`TO bfe_ai_request_log`，SELECT 完成上述打平与 UTC 换算后写入明细表，随 Kafka 表消费自动触发。

---

## 3. 聚合表 `bfe_ai_metrics_1m`

### 3.1 表属性

| 属性 | 值 |
|------|----|
| 引擎 | `SummingMergeTree((request_count, error_count, auth_reject_count, input_tokens, output_tokens, total_tokens, ttft_us_sum, tpot_us_sum, req_header_bytes, req_body_bytes, res_header_bytes, res_body_bytes, rate_limit_hits, backend_retries, all_time_sum, cluster_serve_sum, backend_serve_sum, ai_retry_count_sum, ai_cost_value_sum, cache_read_tokens, cache_write_tokens, ai_audio_input_tokens, ai_audio_output_tokens, ai_image_count))` |
| 排序键 | `ORDER BY (ts_min, hostid, ai_apikey_id, ai_requested_model, ai_target_model, ai_stream, product, cluster, sub_cluster, backend_info, method, res_status_code, err_code, header_host, ai_provider, ai_protocol, ai_mode, ai_cost_currency, level1Name, level1, level2Name, level2, level3Name, level3, level4Name, level4, level5Name, level5, rate_limit_policy_id, rate_limit_type, rate_limit_rule_name, ai_auth_reject_reason, ai_auth_reject_quota_plans_slot1, ai_auth_reject_quota_plans_slot2, ai_auth_reject_quota_plans_slot3, ai_auth_reject_quota_plans_slot4, ai_auth_reject_quota_plans_slot5, ai_cache_status, mirror_hit, ai_intent_answer)`（全 40 个维度列，理由见下） |
| 分区 | `PARTITION BY toDate(ts_min)`，按天 |
| TTL | `TTL ts_min + INTERVAL 7 DAY` |

**排序键必须为全 40 个维度列**：SummingMergeTree 在后台 merge 时会把"排序键完全相同"的行合并求和，**非排序键维度列只保留其中一行的值**——若排序键只取前缀子集，维度组合仅在未排序维度上有差异的行会被折叠成一行，维度粒度丢失、分布统计被污染（本地冒烟实测：normal 与 rate_limit 样例共享前六列排序键，merge 后 `ai_cache_status` 分布由 1:2 变成 2:1）。因此 40 维全部进入排序键，确保只有真正同维组合的行才被合并。**显式求和列**共 24 个，与全维排序键配合双保险，保证数值维度列（`res_status_code` Int16、`mirror_hit` UInt8 等）绝不参与求和。

### 3.2 维度字段（40，NOT NULL，`Default ''`/`0` 归一）

| 字段 | 类型 | 说明 |
|------|------|------|
| `ts_min` | DateTime('UTC') | **分钟时间桶**（`toStartOfMinute(log_time)`；**时间字段**，聚合表用此字段做时间过滤） |
| `hostid` | String | 主机标识 |
| `ai_apikey_id` | String | API Key ID |
| `ai_requested_model` | String | 请求模型 |
| `ai_target_model` | String | 路由模型 |
| `ai_stream` | UInt8 | 流式标识（0/1） |
| `product` | String | 产品线 |
| `cluster` | String | 集群 |
| `sub_cluster` | String | 子集群 |
| `backend_info` | String | 后端节点 |
| `method` | String | HTTP 方法 |
| `res_status_code` | Int16 | 响应状态码 |
| `err_code` | String | 错误码（空=正常） |
| `header_host` | String | 请求 Host |
| `ai_provider` | String | 上游模型提供商 |
| `ai_protocol` | String | AI 协议 |
| `ai_mode` | String | AI 模式 |
| `ai_cost_currency` | String | 成本币种 |
| `level1Name` ~ `level5Name` | String | 标签层级名 ×5 |
| `level1` ~ `level5` | String | 标签层级值 ×5 |
| `rate_limit_policy_id` | String | 限流策略 ID（取首个命中） |
| `rate_limit_type` | String | 限流类型（`tpm`/`rpm`/`concurrency`） |
| `rate_limit_rule_name` | String | 限流规则名（取首条规则） |
| `ai_auth_reject_reason` | String | 认证拒绝原因 |
| `ai_auth_reject_quota_plans_slot1` ~ `slot5` | String | 被拒绝配额计划槽位 ×5 |
| `ai_cache_status` | String | 缓存状态（`hit`/`miss`/`skip`，空=未启用） |
| `mirror_hit` | UInt8 | 镜像命中（0/1） |
| `ai_intent_answer` | String | 意图答案（含 `unknown`，空=未分类） |

> 聚合表维度列**全部 NOT NULL**，空值归一由明细表 `DEFAULT ''`/`0` 天然保证（等价 Doris JOB 的 `COALESCE(x,'')` / `COALESCE(x,0)`），MV 直取同名列即可。

### 3.3 聚合指标字段（24，Int64，SummingMergeTree 显式求和列）

| 字段 | 说明 | 单位 |
|------|------|------|
| `request_count` | 请求数 | 次 |
| `error_count` | 错误数（`err_code != ''`） | 次 |
| `auth_reject_count` | 认证拒绝数（`ai_auth_reject_reason != ''`） | 次 |
| `input_tokens` | 输入 Token 累计 | 个 |
| `output_tokens` | 输出 Token 累计 | 个 |
| `total_tokens` | 总 Token 累计 | 个 |
| `ttft_us_sum` | TTFT 累计 | 微秒 |
| `tpot_us_sum` | TPOT 累计 | 微秒 |
| `req_header_bytes` | 请求头字节累计 | 字节 |
| `req_body_bytes` | 请求体字节累计 | 字节 |
| `res_header_bytes` | 响应头字节累计 | 字节 |
| `res_body_bytes` | 响应体字节累计 | 字节 |
| `rate_limit_hits` | 命中限流的**请求数**（`length(ai_rate_limit_hits) > 0`） | 次 |
| `backend_retries` | 后端重试总次数 | 次 |
| `all_time_sum` | 总耗时累计 | 毫秒 |
| `cluster_serve_sum` | 集群层耗时累计 | 毫秒 |
| `backend_serve_sum` | 后端耗时累计 | 毫秒 |
| `ai_retry_count_sum` | 模型层重试总次数 | 次 |
| `ai_cost_value_sum` | 成本累计（固定点整数） | — |
| `cache_read_tokens` | 缓存读取 Token 累计 | 个 |
| `cache_write_tokens` | 缓存写入 Token 累计 | 个 |
| `ai_audio_input_tokens` | 音频输入 Token 累计 | 个 |
| `ai_audio_output_tokens` | 音频输出 Token 累计 | 个 |
| `ai_image_count` | 图片数量累计 | 个 |

### 3.4 聚合 MV 与查询纪律

**`bfe_ai_metrics_1m_mv`**：`TO bfe_ai_metrics_1m`，明细表插入时同步触发（秒级延迟，优于 Doris JOB 的分钟级）；分钟桶由 `toStartOfMinute(log_time)` 天然对齐 UTC 整分钟，**无需 Doris JOB 的时间窗谓词**（`WHERE log_time >= 上一分钟起点 AND < 本分钟起点`）。默认不挂 `POPULATE`，从 MV 创建时刻起积累（同 Doris 口径）；历史窗口可按 §4 口径从明细表重算。

**SummingMergeTree 查询纪律**（与 Doris AGGREGATE KEY 直读不同，必须遵守）：

- 相同完整排序键的行**仅在被后台 merge 时才合并求和**；未合并的 part 中仍存在多行。
- 查询侧必须 `GROUP BY <维度> + sum(<指标>)` 兜底（api 查询层按维度聚合天然兼容）。
- **禁止 `SELECT *` 直读聚合表**——会拿到未合并的中间行，指标被放大。

**at-least-once 口径**：Kafka 引擎 + MV 为 at-least-once，位点提交前失败重投的重复行会被 SummingMergeTree 重复累计（Doris Routine Load 事务性更强）。日志统计场景惯例接受小概率重复；发现重复时可按 §4 口径从明细表重算近 7 天聚合兜底：

```sql
-- 历史窗口重算示例：清空后从明细回灌（先停写或容忍双份区间）
INSERT INTO bfe_ai_metrics_1m
SELECT toStartOfMinute(log_time) AS ts_min, /* ... 40 维 ... */ count() AS request_count /* ... 24 指标 ... */
FROM bfe_ai_request_log
WHERE log_time >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY /* ... 全 40 维 ... */;
```

（SELECT 投影与 `bfe_ai_metrics_1m_mv.sql` 完全同构，此处不展开。）

---

## 4. 明细表 → 聚合表 映射（口径对照）

聚合 MV 随明细插入触发，关键映射与 Doris INSERT JOB 逐一对齐：

| 聚合表字段 | ClickHouse 表达式（聚合 MV） | Doris INSERT JOB 对照 |
|-----------|------------------------------|----------------------|
| `ts_min` | `toStartOfMinute(log_time)` | `DATE_TRUNC(log_time, 'minute')` |
| 其余维度列（39 列，含 5 个配额槽位） | 明细表同名字段直取（明细 `DEFAULT ''`/`0` 归一，等价 COALESCE） | 同名字段，`COALESCE(x, '')` / `COALESCE(x, 0)` |
| `level*Name` / `level*` | 明细表打平列（消费 MV 已打平） | 明细表打平列（Routine Load 已打平） |
| `rate_limit_policy_id` / `type` / `rule_name` | 明细表打平列 | 明细表打平列（Routine Load `json_extract` 导入时打平） |
| `ai_auth_reject_quota_plans_slot1~5` | `ai_auth_reject_quota_plans[N]`（越界取类型默认值 `''`） | `ai_auth_reject_quota_plans` 的前 5 个元素（缺失补 `''`） |
| `request_count` | `count()` | `COUNT(1)` |
| `error_count` | `sum(toInt64(err_code != ''))` | `SUM(err_code 非空 ? 1 : 0)` |
| `auth_reject_count` | `sum(toInt64(ai_auth_reject_reason != ''))` | `SUM(ai_auth_reject_reason 非空 ? 1 : 0)` |
| `rate_limit_hits` | `sum(toInt64(length(ai_rate_limit_hits) > 0))` | `SUM(ARRAY_SIZE(ai_rate_limit_hits) > 0)`（命中限流的请求数，非规则条数） |
| 其余 SUM 指标（tokens、耗时、字节、重试、成本、音视频、图片等） | `sum(<明细列>)`（明细列非空，等价 Doris COALESCE 归一） | `SUM(COALESCE(明细字段, 0))` |

---

## 5. 查询范式（时区纪律 + 常用查询）

### 5.1 选表与时间字段

| 场景 | 用表 | 时间字段 |
|------|------|----------|
| 看板 / 趋势 / 告警 | `bfe_ai_metrics_1m` | `ts_min` |
| 单条明细排查 | `bfe_ai_request_log` | `log_time` |

**时区纪律**：两表时间列均为 `DateTime('UTC')`，存储即 UTC 墙钟，**查询与渲染一律显式按 UTC 解释**（`toDateTime('...', 'UTC')`、`toString(ts_min)`），不要依赖会话时区；与外部系统（Doris/MySQL 链路、epoch 报表参数）比较时**走 epoch**（`toUnixTimestamp(log_time)`），避免时区换算歧义。

### 5.2 常用聚合示例

聚合表是 SummingMergeTree，跨时间/维度聚合时**必须对指标列 `sum()` 兜底、按维度 `GROUP BY`**；求平均值用「sum / sum(request_count)」。

```sql
-- 1) QPS（每分钟请求数）
SELECT ts_min, sum(request_count) AS qps
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ts_min ORDER BY ts_min;

-- 2) 请求量按模型
SELECT ai_requested_model, sum(request_count) AS cnt
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ai_requested_model ORDER BY cnt DESC;

-- 3) 错误率（%）
SELECT ts_min,
       sum(error_count) / sum(request_count) * 100 AS error_rate
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ts_min ORDER BY ts_min;

-- 4) 平均 TTFT（微秒）/ TPOT（微秒）/ 总耗时（毫秒）
SELECT ts_min,
       sum(ttft_us_sum)  / sum(request_count) AS avg_ttft_us,
       sum(tpot_us_sum)  / sum(request_count) AS avg_tpot_us,
       sum(all_time_sum) / sum(request_count) AS avg_latency_ms
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ts_min ORDER BY ts_min;

-- 5) Token 消耗 / 成本
SELECT ts_min,
       sum(input_tokens)      AS input_tokens,
       sum(output_tokens)     AS output_tokens,
       sum(ai_cost_value_sum) AS cost
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ts_min ORDER BY ts_min;

-- 6) 限流命中
SELECT ts_min, sum(rate_limit_hits) AS rate_limited
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ts_min ORDER BY ts_min;

-- 7) 认证拒绝
SELECT ts_min, sum(auth_reject_count) AS auth_rejected
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ts_min ORDER BY ts_min;
```

### 5.3 标签层级（level）使用

```sql
-- 按 Level1 标签值（如部门 dep）统计请求量
SELECT level1Name, level1, sum(request_count) AS cnt
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY level1Name, level1 ORDER BY cnt DESC;
```

### 5.4 流式 / 非流式

```sql
SELECT ai_stream, sum(request_count) AS cnt
FROM bfe_ai_metrics_1m
WHERE ts_min >= toDateTime('2026-10-01 00:00:00', 'UTC')
GROUP BY ai_stream;   -- 0=非流式, 1=流式
```

---

## 6. 空值与特殊值约定

| 类型 | 空值/默认 |
|------|-----------|
| 维度字符串（`err_code`、`ai_auth_reject_reason` 等） | 空串 `''` 表示「无 / 未命中」（列 `DEFAULT ''` 归一） |
| 数值字段（tokens、耗时、长度） | `0`（列 `DEFAULT 0` 归一） |
| `err_msg`、`ai_intent_confidence`、`ai_intent_latency_us`、`ai_intent_cache_hit` | Nullable，NULL=未发生/未分类 |
| `ai_stream` / `is_trust_src_ip` / `mirror_hit` | UInt8，0 / 1 |
| 复杂类型（Array / Nested） | 明细表 `DEFAULT []`；聚合表已打平成标量 |

**判断错误**：`err_code != ''`
**判断认证拒绝**：`ai_auth_reject_reason != ''`
**判断限流**：聚合表用 `rate_limit_hits > 0`，明细表用 `length(ai_rate_limit_hits) > 0`

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
| 缓存状态 | `ai_cache_status` | `hit` / `miss` / `skip`，空=未启用 |
| 意图来源 | `ai_intent_source` | `explicit_header` / `classifier` / `cache` |
| 网络类型 | `client_network` | `Ipv4` / `Ipv6` |
| 限流类型 | `rate_limit_type` | `tpm` / `rpm` / `concurrency` |
| 币种 | `ai_cost_currency` | `RMB` / `USD` |

---

## 8. 文档更新历史

| 日期 | 版本 | 变更说明 |
|------|------|----------|
| 2026-10-01 | v1.0 | 初始版本：ClickHouse 侧六份 SQL 资产落地——MergeTree 明细表（排序键四列对齐 Doris UNIQUE KEY、按天分区 + TTL 7 天）、Kafka 引擎暂存表 + 消费 MV（JSON 打平 + UTC 墙钟）、SummingMergeTree 聚合表（40 维 + 24 指标显式求和列、排序键六列）、聚合 MV（toStartOfMinute 分钟桶）；`logid UInt64` 承载原值（Doris 截断存储）、Nullable 四列与 Default 归一口径、at-least-once 重复计数口径 |
| 2026-10-01 | v1.1 | 聚合表排序键由"前六列前缀"修正为全 40 个维度列：冒烟实测发现 SummingMergeTree 后台 merge 会折叠"排序键相同"的行（非排序键维度只保留一行值），前缀排序键导致维度粒度丢失、分布统计被污染（`ai_cache_status` 分布 1:2 变 2:1）；同步修正本文档 §3.1 与变更记录、设计方案的排序键论述 |
