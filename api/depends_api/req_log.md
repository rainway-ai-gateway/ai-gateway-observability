# BFE AI 请求日志字段说明（req_log.md）

本文档记录 BFE AI 网关请求日志的字段映射关系，作为 Doris 表设计、Grafana 看板等下游配置的字段依据。

**字段依据链路**：

```
bfe_access_pb/bfe_access.proto  →  log-reader/reader_modules/mod_kafka/field_registry.go  →  本文档
      (PB 字段定义)                      (JSON 输出字段定义)                                    (映射与含义)
```

## 版本说明

| 项 | 值 |
|----|----|
| bfe-access-pb | **v0.3.4**（本地 HEAD 与 log-reader `go.mod` 依赖一致） |
| 本地 PB 仓库 HEAD | `14cf80a`（v0.3.4） |
| log-reader | commit `5890368` |
| 生成日期 | 2026-08-26 |

## 输出属性说明

| 标记 | 含义 |
|------|------|
| 必选 | `Required=true`，始终输出 |
| 默认 | `Default=true`，在默认输出集内（可被配置排除） |
| 可选 | `Required=false` 且 `Default=false`，需显式配置才输出 |

## 一、字段明细

### 1.1 基础信息（BfeLog 顶层）

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `logid` | uint64 | 必选 | `BfeLog.logid` | required uint64 | 3 | 请求唯一日志 ID（若请求头含 `BFE_LOGID` 则复用） |
| `timestamp` | uint64 | 必选 | `BfeLog.timestamp` | required uint64 | 2 | 请求/会话结束时的 Unix 秒级时间戳 |
| `product` | string | 必选 | `BfeLog.request_log.product`，空则回退 `BfeLog.product` | optional string / ProductID 枚举 | 101 / 1 | 产品标识 |
| `hostid` | string | 必选 | （无 PB 对应） | — | — | 主机标识 `hostname_netns`，来自 `reader_util.GetHostId()` |
| `log_tag` | string | 可选 | `BfeLog.log_tag` | optional string | 26 | 日志标签：`req_<product>`（正常）/ `req_err_<product>`（错误） |

### 1.2 客户端连接

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `client_ip` | string | 必选 | `request_log.client_ip` / `client_ip6` | required uint32 / string | 9 / 14 | 客户端 IP；IPv6 时取 `client_ip6`，否则 `client_ip` 转点分字符串 |
| `client_network` | string | 可选 | `request_log.client_network` | NetType 枚举 | 13 | 客户端网络类型：`Ipv4` / `Ipv6` |
| `req_num` | uint32 | 可选 | `request_log.req_num` | required uint32 | 11 | 会话内请求序号（从 1 开始） |
| `session_id` | uint64 | 可选 | `request_log.session_id` | optional uint64 | 7 | 请求所属的会话 ID |

### 1.3 请求基本信息

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `err_code` | string | 必选 | `request_log.err_code` | required string | 3 | 错误码；成功请求为空串 |
| `err_msg` | string | 必选 | `request_log.err_msg` | required string | 4 | 附加错误详情 |
| `req_header_len` | uint32 | 必选 | `request_log.req_header_len` | optional uint32 | 5 | 请求头长度（字节） |
| `req_body_len` | uint32 | 必选 | `request_log.req_body_len` | optional uint32 | 6 | 请求体长度（字节，未实现） |

### 1.4 请求头

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `proto` | string | 必选 | `request_log.proto` | required string | 41 | 协议：http1.0 / http1.1 / https / spdy / http2 |
| `header_host` | string | 必选 | `request_log.header_host` | required string | 42 | 请求头 Host |
| `origin_uri` | string | 必选 | `request_log.origin_uri` | optional string | 43 | 原始请求 URI |
| `final_uri` | string | 默认 | `request_log.final_uri` | optional string | 44 | 最终路由 URI；与 origin 相同时为空 |
| `method` | string | 必选 | `request_log.method` | optional string | 52 | HTTP 方法：GET / POST / PUT 等 |
| `content_type` | string | 默认 | `request_log.content_type` | optional string | 54 | 请求 Content-Type |
| `referrer` | string | 可选 | `request_log.referrer` | optional string | 45 | 请求头 Referer |
| `user_agent` | string | 可选 | `request_log.user_agent` | optional string | 50 | 请求头 User-Agent |
| `x_forward_for` | string | 默认 | `request_log.x_forward_for` | optional string | 47 | 请求头 X-Forwarded-For |
| `accept_language` | string | 默认 | `request_log.accept_language` | optional string | 48 | 请求头 Accept-Language |
| `authorization` | string | 默认 | `request_log.authorization` | optional string | 49 | 请求头 Authorization |
| `transfer_encoding` | string | 默认 | `request_log.transfer_encoding` | optional string | 51 | 请求 Transfer-Encoding |
| `delegation` | string | 可选 | `request_log.delegation` | optional string | 53 | 委托域名 |
| `uid` | string | 可选 | `request_log.uid` | optional string | 57 | 请求头 UID |

### 1.5 Cookie 与请求头列表

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `cookie` | string | 可选 | `request_log.cookie` | optional string | 61 | 完整 Cookie 头 |
| `req_headers` | []object | 可选 | `request_log.req_headers` | repeated HttpHeader | 55 | 关注的请求头列表，元素 `{key, value}` |

### 1.6 路由信息

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `cluster` | string | 默认 | `request_log.cluster` | optional string | 102 | 目标集群名 |
| `sub_cluster` | string | 默认 | `request_log.sub_cluster` | optional string | 103 | 目标子集群名 |
| `backend_info` | string | 默认 | `request_log.backend_info` | InstanceInfo | 104 | 后端 `IP:Port`（多次重试时为最后一次） |
| `backend_retry` | uint32 | 默认 | `request_log.backend_retry` | optional uint32 | 105 | 后端重试次数 |

### 1.7 响应信息

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `res_status_code` | uint32 | 必选 | `request_log.res_status_code` | optional uint32 | 151 | 响应 HTTP 状态码 |
| `res_header_len` | uint32 | 必选 | `request_log.res_header_len` | optional uint32 | 152 | 响应头长度（字节） |
| `res_body_len` | uint32 | 必选 | `request_log.res_body_len` | optional uint32 | 153 | 响应体长度（字节） |
| `res_content_type` | string | 默认 | `request_log.res_content_type` | optional string | 154 | 响应 Content-Type |
| `res_location` | string | 可选 | `request_log.res_location` | optional string | 155 | 响应 Location（3xx 重定向） |
| `res_transfer_encoding` | string | 可选 | `request_log.res_transfer_encoding` | optional string | 156 | 响应 Transfer-Encoding |
| `res_headers` | []object | 可选 | `request_log.res_headers` | repeated HttpHeader | 158 | 关注的响应头列表，元素 `{key, value}` |

### 1.8 耗时（毫秒 ms）

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `all_time` | uint32 | 必选 | `request_log.all_time` | required uint32 | 201 | 请求总耗时（读请求起 → 发响应完） |
| `read_client_time` | uint32 | 必选 | `request_log.read_client_time` | optional uint32 | 202 | 读客户端请求耗时 |
| `cluster_serve_time` | uint32 | 必选 | `request_log.cluster_serve_time` | optional uint32 | 203 | 集群层服务耗时（含重试） |
| `backend_serve_time` | uint32 | 必选 | `request_log.backend_serve_time` | optional uint32 | 204 | 后端服务耗时（重试时为最后一次） |
| `write_client_time` | uint32 | 必选 | `request_log.write_client_time` | optional uint32 | 205 | 写客户端响应耗时 |
| `session_offset_time` | uint32 | 可选 | `request_log.session_offset_time` | optional uint32 | 206 | 会话内时间偏移（会话起 → 发响应完） |
| `connect_backend_time` | uint32 | 默认 | `request_log.connect_backend_time` | optional uint32 | 207 | 连接后端耗时（重试时仅记录最后一次） |
| `proxy_delay_time` | uint32 | 必选 | `request_log.proxy_delay_time` | optional uint32 | 208 | BFE 代理延迟（读请求后 → 连后端前，首次调用） |

### 1.9 AI 可观测 — API Key 与标签

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `ai_apikey_id` | string | 默认 | `request_log.ai_apikey_id` | optional string | 701 | API Key 内部 ID（虚拟 Key ID，非原始 Key） |
| `ai_apikeytags` | object | 默认 | `request_log.ai_apikeytags` | repeated ApikeyTag | 702 | API Key 标签，按 `taglevel`(1~5) 打平为 `{level1:{tagname,tagvalue}, …, level5:{…}}` |

### 1.10 AI 可观测 — 模型与 Token

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `ai_requested_model` | string | 默认 | `request_log.ai_requested_model` | optional string | 703 | 客户端请求的原始模型名 |
| `ai_target_model` | string | 默认 | `request_log.ai_target_model` | optional string | 704 | 网关实际路由到的模型名 |
| `ai_stream` | bool | 默认 | `request_log.ai_stream` | optional bool | 705 | 是否流式响应 |
| `ai_input_tokens` | int64 | 默认 | `request_log.ai_input_tokens` | optional int64 | 706 | 输入 Token 数 |
| `ai_output_tokens` | int64 | 默认 | `request_log.ai_output_tokens` | optional int64 | 707 | 输出 Token 数 |
| `ai_total_tokens` | int64 | 默认 | `request_log.ai_total_tokens` | optional int64 | 708 | 总 Token 消耗 |
| `ai_ttft_us` | int64 | 默认 | `request_log.ai_ttft_us` | optional int64 | 709 | 首 Token 延迟 TTFT（微秒，仅流式） |
| `ai_tpot_us` | int64 | 默认 | `request_log.ai_tpot_us` | optional int64 | 710 | 每输出 Token 延迟 TPOT（微秒，output_tokens>0 时） |
| `ai_cache_read_tokens` | int64 | 默认 | `request_log.ai_cache_read_tokens` | optional int64 | 781 | 缓存读取 Token 数 |
| `ai_cache_write_tokens` | int64 | 默认 | `request_log.ai_cache_write_tokens` | optional int64 | 782 | 缓存写入 Token 数 |
| `ai_audio_input_tokens` | int64 | 默认 | `request_log.ai_audio_input_tokens` | optional int64 | 783 | 音频输入 Token 数 |
| `ai_audio_output_tokens` | int64 | 默认 | `request_log.ai_audio_output_tokens` | optional int64 | 784 | 音频输出 Token 数 |
| `ai_image_count` | int64 | 默认 | `request_log.ai_image_count` | optional int64 | 785 | 生成图片数量（image_generation 模式） |

### 1.11 AI 可观测 — 模型/提供商/成本/重试

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `ai_provider` | string | 默认 | `request_log.ai_provider` | optional string | 714 | 上游模型提供商，如 openai / anthropic / baidu / aliyun |
| `ai_protocol` | string | 默认 | `request_log.ai_protocol` | optional string | 717 | AI 协议风格，如 openai / anthropic |
| `ai_mode` | string | 默认 | `request_log.ai_mode` | optional string | 716 | AI 请求模式，如 chat / image_generation / embedding / audio_speech |
| `ai_retry_count` | uint32 | 默认 | `request_log.ai_retry_count` | optional uint32 | 715 | 模型调用层重试次数（与 HTTP 层 backend_retry 解耦） |
| `ai_cost_value` | int64 | 默认 | `request_log.ai_cost_value` | optional int64 | 761 | 本次请求估算成本（定点整数，精度取决于 ai_cost_currency） |
| `ai_cost_currency` | string | 默认 | `request_log.ai_cost_currency` | optional string | 762 | 成本币种：RMB / USD |

### 1.12 AI 可观测 — 复杂类型（ARRAY）

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `ai_rate_limit_hits` | []object | 默认 | `request_log.ai_rate_limit_hits` | repeated RateLimitHit | 711 | 限流命中列表，元素 `{rate_limit_policy_id, rate_limit_type, rule_names[]}` |
| `ai_auth_reject_reason` | string | 默认 | `request_log.ai_auth_reject_reason` | optional string | 712 | 认证/授权拒绝原因 |
| `ai_auth_reject_quota_plans` | []string | 默认 | `request_log.ai_auth_reject_quota_plans` | repeated string | 713 | 被拒绝的配额计划 ID 列表 |
| `ai_route_rule_hits` | []object | 默认 | `request_log.ai_route_rule_hits` | repeated AIRouteRuleHit | 801 | 命中的 AI 路由规则，元素 `{rule_owner, rule_owner_type, rule_name}` |
| `ai_cluster_key_names` | []object | 默认 | `request_log.ai_cluster_key_names` | repeated ClusterKeyName | 802 | 尝试过的 (集群, Key) 组合，元素 `{cluster_name, key_name}` |
| `ai_auth_hit_quota_plans` | []string | 默认 | `request_log.ai_auth_hit_quota_plans` | repeated string | 841 | 成功请求命中的配额计划 ID 列表 |

### 1.13 地址信息（ConnAddrInfo 打平）

| JSON 字段 | JSON 类型 | 输出 | PB 字段路径 | PB 类型 | 编号 | 含义 |
|-----------|-----------|------|-------------|---------|------|------|
| `bfe_ip` | string | 可选 | `request_log.addr_info.bfe_ip` | required uint32 | 1 | 处理该请求的 BFE 服务器 IP（点分字符串） |
| `sock_src_ip` | string | 可选 | `request_log.addr_info.sock_src_ip` | required uint32 | 2 | Socket 源 IP（点分字符串） |
| `is_trust_src_ip` | bool | 默认 | `request_log.addr_info.is_trust_src_ip` | required bool | 3 | 源 IP 是否在可信 IP 列表 |
| `vip` | string | 可选 | `request_log.addr_info.vip` | optional uint32 | 4 | 目的 VIP（点分字符串） |
| `vip6` | string | 可选 | `request_log.addr_info.vip6` | optional string | 5 | 目的 VIP6 |

## 二、PB 字段未输出清单

以下 protobuf 字段在 `field_registry.go` 中**未注册**为 JSON 输出字段。

| PB 字段路径 | 编号 | 类型 | 未输出原因 |
|-------------|------|------|-----------|
| `BfeLog.log_type` | 200 | BfeLogType（required） | 日志类型（request/session）枚举，log-reader 未输出 |
| `BfeLog.session_log` | 211 | SessionLog | 整个会话日志未输出（见下方 SessionLog 子字段清单） |
| `request_log.client_ip6` | 14 | string | 仅在 IPv6 时内部用于组装 `client_ip`，不单独输出 |
| `request_log.addr_info` | 8 | ConnAddrInfo | 容器字段，其子字段已打平输出（见 1.13），本身不作为 JSON 字段 |

**SessionLog 子字段（随 `session_log` 整体未输出）**：

| 子字段 | 编号 | 类型 |
|--------|------|------|
| `err_code` | 2 | string（required） |
| `err_msg` | 3 | string（required） |
| `req_num` | 4 | uint32（required） |
| `read_len` | 5 | uint32（required） |
| `write_len` | 6 | uint32（required） |
| `addrInfo` | 7 | ConnAddrInfo（required） |
| `proto` | 8 | string |
| `product` | 9 | string |
| `start_time` | 41 | uint64（required） |
| `all_time` | 42 | uint32（required） |
| `rtt` | 43 | uint32 |
| `synRtt` | 44 | uint32 |
| `tls_version` | 101 | uint32 |
| `cipher_suite` | 102 | uint32 |
| `session_resume` | 103 | bool |
| `ocsp_staple` | 104 | bool |
| `handshake_time` | 105 | uint32 |
| `tls_cert_common_name` | 106 | string |
| `tls_cert_chain_id` | 107 | string |
| `lost_type` | 108 | LostType |

## 三、统计

| 项 | 数量 |
|----|------|
| log-reader JSON 输出字段 | 80 |
| 未输出的 PB 字段（含 SessionLog 子字段） | 4 + 20 = 24 |
| 无 PB 对应的 JSON 字段 | 1（`hostid`） |

## 四、关键映射说明

- **IP 字段转换**：`client_ip`、`bfe_ip`、`sock_src_ip`、`vip` 及 `backend_info` 中的 IP，PB 中为 uint32，输出为点分十进制字符串。
- **`client_ip` 双来源**：`client_network == Ipv6` 时取 `client_ip6`（字符串），否则取 `client_ip`（uint32 → 点分）。
- **`ai_apikeytags` 数组 → 对象**：PB 为 `repeated ApikeyTag`（含 `taglevel`），log-reader 按 `taglevel` 1~5 打平成 `{levelN:{tagname,tagvalue}}` 对象。
- **`backend_info` 结构 → 字符串**：PB 为 `InstanceInfo{ip_addr, port}`，输出为 `"ip:port"`。
- **`product` 双来源**：优先 `request_log.product`（字符串），为空时回退 `BfeLog.product`（枚举 `BFE=18` 的 `.String()`）。
- **`hostid` 无 PB 对应**：来自 log-reader 进程自身 `reader_util.GetHostId()`。
