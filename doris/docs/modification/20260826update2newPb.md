# 变更记录：升级到新 PB（bfe-access-pb）字段

- **日期**：2026-08-26
- **变更范围**：Doris 存储层（明细表 `bfe_ai_request_log`、聚合表 `bfe_ai_metrics_1m`、Routine Load、INSERT JOB、demo 样例）
- **变更原因**：log-reader 输出字段升级到新版 protobuf（`bfe-access-pb` 的 `BfeLog` 结构），Doris 侧表结构、Routine Load、INSERT JOB 与 demo 样例同步对齐。

> 字段来源以 log-reader 的字段注册表为准：
> `log-reader/reader_modules/mod_kafka/field_registry.go`（`github.com/bfenetworks/bfe-access-pb`）。

## 1. 变更总览

| 类别 | 明细表 `bfe_ai_request_log` | 聚合表 `bfe_ai_metrics_1m` |
|------|------------------------------|------------------------------|
| 新增字段 | 34 个 | 17 个（7 个维度列 + 10 个 level 维度列 + 7 个指标列） |
| 字段改名 | 3 个 | 4 个（含 10 个 tagslot → level 列） |
| 字段删除 | 1 个（`ai_apikeytags` ARRAY 列，改为打平列） | 10 个（`tagslot*` 列） |
| 格式调整 | `ai_apikeytags` 数组 → 对象 | — |

## 2. 字段改名（重命名）

### 明细表

| 旧字段 | 新字段 | 说明 |
|--------|--------|------|
| `ai_apikey` | `ai_apikey_id` | API Key 改为虚拟 Key ID（主键列同步修改） |
| `ai_mapped_model` | `ai_target_model` | 语义统一为「实际路由模型」 |
| `ai_prompt_tokens` | `ai_input_tokens` | 语义统一为「输入 Token」 |

### 聚合表

| 旧字段 | 新字段 | 说明 |
|--------|--------|------|
| `ai_apikey` | `ai_apikey_id` | 维度列，与明细表对齐 |
| `ai_mapped_model` | `ai_target_model` | 维度列，与明细表对齐 |
| `prompt_tokens` | `input_tokens` | 指标列，与明细表对齐 |
| `tagslot1name`~`tagslot5value`（10 列） | `level1Name`~`level5`（10 列） | 标签打平列重命名 |

## 3. 格式调整：`ai_apikeytags`

- **旧格式**：数组 `ARRAY<STRUCT<tagname, tagvalue>>`，元素顺序与层级对齐不直观。
- **新格式**：对象，以 `level1`~`level5` 作为 key，每个 value 为 `{tagname, tagvalue}`。

```json
{
  "ai_apikeytags": {
    "level1": {"tagname": "dep0", "tagvalue": "rd"},
    "level2": {"tagname": "dep2", "tagvalue": "teama"},
    "level3": {"tagname": "dep3", "tagvalue": "yyx"},
    "level4": {},
    "level5": {}
  }
}
```

Routine Load 中通过 `json_extract` 将对象打平成 10 个固定列：

```sql
level1Name = json_unquote(json_extract(ai_apikeytags, '$.level1.tagname')),
level1     = json_unquote(json_extract(ai_apikeytags, '$.level1.tagvalue')),
...
level5Name = json_unquote(json_extract(ai_apikeytags, '$.level5.tagname')),
level5     = json_unquote(json_extract(ai_apikeytags, '$.level5.tagvalue'))
```

## 4. 新增字段

### 4.1 明细表 `bfe_ai_request_log`

**基础 / 客户端连接**

| 字段 | 类型 | 说明 |
|------|------|------|
| `log_tag` | VARCHAR(64) | 日志标签：`req_<product>` / `req_err_<product>` |
| `client_network` | VARCHAR(16) | 客户端网络类型：`Ipv4` / `Ipv6` |
| `req_num` | INT | 会话内请求序号（从 1 开始） |
| `session_id` | BIGINT | 会话 ID |
| `bfe_ip` | VARCHAR(64) | BFE 服务器 IP |
| `sock_src_ip` | VARCHAR(64) | Socket 源 IP |
| `vip` | VARCHAR(64) | 目的 VIP |
| `vip6` | VARCHAR(128) | 目的 VIP6 |

**请求头 / Cookie**

| 字段 | 类型 | 说明 |
|------|------|------|
| `referrer` | VARCHAR(2048) | Referer 头 |
| `user_agent` | VARCHAR(1024) | User-Agent 头 |
| `delegation` | VARCHAR(256) | 委托域名 |
| `uid` | VARCHAR(256) | UID 头 |
| `cookie` | VARCHAR(4096) | Cookie 头 |
| `req_headers` | ARRAY\<STRUCT\<`key`, `value`\>\> | 请求头列表 |

**响应**

| 字段 | 类型 | 说明 |
|------|------|------|
| `res_location` | VARCHAR(2048) | 响应 Location（3xx） |
| `res_transfer_encoding` | VARCHAR(64) | 响应 Transfer-Encoding |
| `res_headers` | ARRAY\<STRUCT\<`key`, `value`\>\> | 响应头列表 |

**耗时**

| 字段 | 类型 | 说明 |
|------|------|------|
| `session_offset_time` | INT | 会话内时间偏移（毫秒） |

**AI — API Key 标签（打平）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `level1Name` / `level1` | VARCHAR(128) | Level1 标签名 / 值 |
| `level2Name` / `level2` | VARCHAR(128) | Level2 标签名 / 值 |
| `level3Name` / `level3` | VARCHAR(128) | Level3 标签名 / 值 |
| `level4Name` / `level4` | VARCHAR(128) | Level4 标签名 / 值 |
| `level5Name` / `level5` | VARCHAR(128) | Level5 标签名 / 值 |

**AI — 可观测指标（v0.2.0）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ai_cache_read_tokens` | BIGINT | 缓存读取 Token 数 |
| `ai_cache_write_tokens` | BIGINT | 缓存写入 Token 数 |
| `ai_audio_input_tokens` | BIGINT | 音频输入 Token 数 |
| `ai_audio_output_tokens` | BIGINT | 音频输出 Token 数 |
| `ai_image_count` | BIGINT | 图片数量 |
| `ai_provider` | VARCHAR(64) | 上游模型提供商 |
| `ai_protocol` | VARCHAR(64) | AI 协议 |
| `ai_mode` | VARCHAR(64) | AI 模式（`chat` / `audio` / `image`） |
| `ai_retry_count` | INT | 模型调用层重试次数 |
| `ai_cost_value` | BIGINT | 成本固定点整数值 |
| `ai_cost_currency` | VARCHAR(16) | 成本币种（`RMB` / `USD`） |

**AI — 复杂类型（ARRAY）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ai_route_rule_hits` | ARRAY\<STRUCT\<`rule_owner`,`rule_owner_type`,`rule_name`\>\> | AI 路由规则命中记录 |
| `ai_cluster_key_names` | ARRAY\<STRUCT\<`cluster_name`,`key_name`\>\> | 尝试过的集群与 Key 组合 |
| `ai_auth_hit_quota_plans` | ARRAY\<VARCHAR(128)\> | 成功请求时命中的配额计划 |

### 4.2 聚合表 `bfe_ai_metrics_1m`

**新增维度列**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ai_provider` | VARCHAR(64) | 上游模型提供商 |
| `ai_protocol` | VARCHAR(64) | AI 协议 |
| `ai_mode` | VARCHAR(64) | AI 模式 |
| `ai_cost_currency` | VARCHAR(16) | 成本币种 |
| `level1Name` ~ `level5Name` | VARCHAR(128) | 标签层级名 ×5 |
| `level1` ~ `level5` | VARCHAR(128) | 标签层级值 ×5 |

**新增指标列（SUM）**

| 字段 | 类型 | 说明 | 单位 |
|------|------|------|------|
| `ai_retry_count_sum` | BIGINT SUM | 模型层重试总次数 | 次 |
| `ai_cost_value_sum` | BIGINT SUM | 成本累计（固定点整数） | — |
| `cache_read_tokens` | BIGINT SUM | 缓存读取 Token 累计 | 个 |
| `cache_write_tokens` | BIGINT SUM | 缓存写入 Token 累计 | 个 |
| `ai_audio_input_tokens` | BIGINT SUM | 音频输入 Token 累计 | 个 |
| `ai_audio_output_tokens` | BIGINT SUM | 音频输出 Token 累计 | 个 |
| `ai_image_count` | BIGINT SUM | 图片数量累计 | 个 |

## 5. 删除 / 替换的字段

| 表 | 字段 | 说明 |
|----|------|------|
| 明细表 | `ai_apikeytags`（ARRAY 列） | 不再作为独立列存储，改由 Routine Load 打平为 `level1Name`~`level5` 10 个固定列 |
| 聚合表 | `tagslot1name`~`tagslot5value`（10 列） | 替换为 `level1Name`~`level5`（10 列） |

## 6. 同步修改的文件

| 文件 | 修改点 |
|------|--------|
| `sqls/bfe_ai_request_log.sql` | 明细表 DDL：改名、新增字段、打平 level 列、主键 `ai_apikey` → `ai_apikey_id` |
| `sqls/bfe_ai_metrics_1m.sql` | 聚合表 DDL：改名、新增维度/指标列、tagslot → level |
| `sqls/bfe_ai_log_load_routine.sql` | Routine Load COLUMNS 对齐新 JSON 字段，`ai_apikeytags` 对象打平 |
| `sqls/bfe_ai_metrics_1m_job.sql` | INSERT JOB SELECT 映射对齐改名/新增字段 |
| `demo/normal_request.json` | 更新为新 JSON 格式（对象 `ai_apikeytags`、全字段） |
| `demo/rate_limit.json` | 同上 |
| `demo/auth_reject.json` | 同上 |

## 7. 相关文档

- 表结构与字段语义：[../design/TABLE_DESIGN.md](../design/TABLE_DESIGN.md)
- Doris 端搭建指南：[../user/HOWTO.md](../user/HOWTO.md)
