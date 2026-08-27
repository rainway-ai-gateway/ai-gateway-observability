---
name: gen-req-log-api
description: Generate the BFE AI request-log field reference document (req_log.md) that maps every log-reader JSON field to its bfe-access-pb protobuf field (path, number, type) with meaning, and lists PB fields NOT emitted by log-reader. Use whenever bfe-access-pb or log-reader changes, to (re)generate api/depends_api/req_log.md before driving Doris/Grafana schema updates.
license: Apache-2.0
---

# Gen Req Log

生成 BFE AI 请求日志字段说明文档 `req_log.md`，记录 **PB 字段 → log-reader JSON 字段** 的完整映射与字段含义，作为 Doris 表设计、Grafana 看板等下游配置的字段依据。

## 输入（三个目录可参数化）

以下为默认值。若用户给出其它路径则以其为准；不确定时用 `question` 工具询问，不要臆测。

| 角色 | 默认目录 | 关键文件 |
|------|----------|----------|
| PB 仓库根 | `/home/yeyunxi/yingfei/opensouce/bfe-access-pb` | `bfe_access_pb/bfe_access.proto` |
| log-reader 仓库根 | `/home/yeyunxi/yingfei/opensouce/log-reader` | `reader_modules/mod_kafka/field_registry.go` |
| 可观测仓库根 | `/home/yeyunxi/yingfei/opensouce/ai-gateway-observability` | 输出 `api/depends_api/req_log.md` |

## 输出

生成/覆盖 `<observability>/api/depends_api/req_log.md`（目录不存在则先 `mkdir -p`）。

## 工作流程

### 0. 确定三个目录

先确认三个目录（PB、log-reader、observability）。默认值如上；用户可覆盖。

### 1. 版本对齐校验（重要，先做）

`field_registry.go` 编译依赖的是 log-reader `go.mod` 里的 `github.com/bfenetworks/bfe-access-pb` 版本，**可能比本地 PB 仓库 HEAD 新**。

1. 读 `log-reader/go.mod`，得到 bfe-access-pb 版本（如 `v0.3.4`）。
2. 若 `field_registry.go` 引用了本地 proto 中不存在的 getter（例如 `GetAiProtocol`、`GetAiMode`、`GetAiImageCount`），说明本地 proto 已过期，**以 log-reader 依赖的版本为准**：
   - `git -C <pb> fetch --tags origin`
   - `git -C <pb> show <version>:bfe_access_pb/bfe_access.proto` 读取该版本的 proto 内容。
3. 把实际采用的 PB 版本、以及它相对本地 HEAD 的差异，写进 req_log.md 的「版本说明」。

### 2. 解析 proto

枚举 `message BfeLog`、`message RequestLog`、`message SessionLog` 及嵌套 message（`ConnAddrInfo`、`InstanceInfo`、`HttpHeader`、`ApikeyTag`、`RateLimitHit`、`AIRouteRuleHit`、`ClusterKeyName`）和 enum（`ProductID`、`BfeLogType`、`NetType`、`LostType`）。

对每个字段记录：**字段名、`required/optional/repeated`、PB 类型、字段编号（=N）、注释含义**。

### 3. 解析 field_registry.go

遍历 `registerAllFields()` 中每个 `registerField(name, typ, required, def, extract, isZero)`，记录：

- `Name`：JSON 字段名
- `Type`：JSON 类型（`string` / `uint64` / `uint32` / `int64` / `bool` / `object` / `[]object` / `[]string`）
- `Required`：是否始终输出
- `Default`：是否在默认输出集
- extractor 调用的 PB getter（据此反推 PB 字段路径）

### 3.1 进一步分析 object / []object 类型字段

步骤 3 中 `Type` 为 `object` 或 `[]object` 的字段，不能只写「object」了事，必须展开内部结构：

- 给出每个子字段的**名字、类型、含义**；
- 若子字段仍是 object/数组，则递归展开，直到标量（string / int64 / uint32 / bool）为止；
- 每个 object 字段附一个 JSON 示例。

展开方法：

1. 看该字段在 `field_registry.go` 的 extractor，确认它构造的是「map」（`object`）还是「结构体切片」（`[]object`）。
2. 子字段名与 JSON 类型，以同包 `json_converter.go` 里的 JSON 结构体定义为准（`json:"..."` tag 即输出字段名）。
3. 子字段含义与 PB 类型，回查步骤 2 中对应的 proto message（`HttpHeader`、`ApikeyTag`、`RateLimitHit`、`AIRouteRuleHit`、`ClusterKeyName`）的注释。

当前 object 类型字段一览：

| JSON 字段 | JSON 类型 | 展开结构 |
|-----------|-----------|----------|
| `ai_apikeytags` | object | map：key 为 `level1`~`level5`，value 为 `ApikeyTagJSON{tagname, tagvalue}` |
| `req_headers` | []object | `HttpHeaderJSON[]{key, value}` |
| `res_headers` | []object | `HttpHeaderJSON[]{key, value}` |
| `ai_rate_limit_hits` | []object | `AiRateLimitHitJSON[]{rate_limit_policy_id, rate_limit_type, rule_names[]}` |
| `ai_route_rule_hits` | []object | `AIRouteRuleHitJSON[]{rule_owner, rule_owner_type, rule_name}` |
| `ai_cluster_key_names` | []object | `ClusterKeyNameJSON[]{cluster_name, key_name}` |

示例（object，固定 key 的 map）：

```json
"ai_apikeytags": {
    "level1": {"tagname": "dep0", "tagvalue": "rd"},
    "level2": {"tagname": "dep2", "tagvalue": "teama"},
    "level3": {"tagname": "dep3", "tagvalue": "yyx"},
    "level4": {},
    "level5": {}
}
```

示例（[]object，元素含数组子字段 `rule_names`）：

```json
"ai_rate_limit_hits": [
    {"rate_limit_policy_id": "rlp-0001", "rate_limit_type": "tpm", "rule_names": ["win2min", "win10min"]}
]
```

最终在 req_log.md「字段明细」中，对每个 object/[]object 字段输出：

- 一张「子字段表」，列 = `子字段名 | JSON 类型 | PB 字段路径 | PB 类型 | 含义`；
- 一个 JSON 示例；数组元素类型需标注（如 `rule_names: string[]`）。

### 4. 交叉映射

为每个 JSON 字段找到 PB 字段路径（如 `BfeLog.request_log.err_code`），应用下方「映射规则」。

### 5. 找出「未输出」的 PB 字段

列出 proto 中存在、但 `field_registry.go` 未注册为 JSON 输出的字段（如 `BfeLog.log_type`、整个 `session_log`、`client_ip6` 等），单列一节「PB 字段未输出清单」，并说明原因。

### 6. 生成 req_log.md

按下方「输出文档结构」生成；字段含义直接引用 proto 注释，缺省时结合 extractor 逻辑与上下文推断，不要编造。

## 映射规则（PB → JSON 转换）

| 场景 | 规则 | 示例 |
|------|------|------|
| 直通字段 | JSON 名 = PB 名（snake_case），类型对应 | `err_code` string ↔ `request_log.err_code` |
| uint32 IP → 点分字符串 | `ipUint32ToString()` | `client_ip`、`bfe_ip`、`sock_src_ip`、`vip` |
| IPv4/IPv6 选择 | `client_network==Ipv6` 取 `client_ip6`，否则 `client_ip` 转点分 | `client_ip` |
| enum → string | 用枚举 `.String()` | `client_network` → `Ipv4`/`Ipv6` |
| InstanceInfo → `"ip:port"` | `ipUint32ToString(ip_addr)+":"+port` | `backend_info` |
| repeated ApikeyTag → object | 按 `taglevel`(1~5) 打平成 `{levelN:{tagname,tagvalue}}` | `ai_apikeytags` |
| repeated RateLimitHit → []object | 保留 `rate_limit_policy_id`/`rate_limit_type`/`rule_names` | `ai_rate_limit_hits` |
| repeated AIRouteRuleHit → []object | 保留 `rule_owner`/`rule_owner_type`/`rule_name` | `ai_route_rule_hits` |
| repeated ClusterKeyName → []object | 保留 `cluster_name`/`key_name` | `ai_cluster_key_names` |
| repeated HttpHeader → []object | 保留 `key`/`value` | `req_headers`、`res_headers` |
| repeated string → []string | 直接透传 | `ai_auth_reject_quota_plans`、`ai_auth_hit_quota_plans` |
| product 特殊提取 | 优先 `request_log.product`，为空回退 `BfeLog.product` 枚举 | `product` |
| 顶层字段 | 直接来自 `BfeLog` | `logid`、`timestamp`、`log_tag` |
| 非 PB 字段 | `reader_util.GetHostId()`，无 PB 对应 | `hostid` |

## 输出文档结构（req_log.md）

按以下章节生成：

1. **标题与说明**：字段依据链路 `bfe_access.proto → field_registry.go → 本文档`。
2. **版本说明**：PB 版本、log-reader commit、生成日期、以及（如有）本地 PB 与依赖版本的差异。
3. **字段明细**（按 log-reader 分组：基础/连接/请求头/路由/响应/耗时/AI 指标/地址信息等）。每个字段一行：

   | JSON 字段 | JSON 类型 | required/default | PB 字段路径 | PB 类型 | 编号 | 含义 |

   对复杂类型（object/[]object）附结构说明（子字段列表）。
4. **PB 字段未输出清单**：

   | PB 字段路径 | 编号 | 类型 | 未输出原因 |

## 关键注意点

- **优先保证映射准确**：JSON 字段名与 PB 字段名大多一致，但 `hostid` 无 PB 对应、`product` 有双来源、`client_ip` 有 IPv4/IPv6 分叉、`ai_apikeytags` 数组→对象，务必按映射规则处理。
- **版本不一致必须显式标注**，不要默默用过期 proto。
- 若发现 proto 与 field_registry.go 存在其它矛盾（字段有 getter 但 proto 无、或反之），在文档中标注并在最终回复里提醒用户。
- 生成后简要说明：共多少个 JSON 字段、多少个 PB 字段未输出、以及版本对齐结论，供用户确认后再驱动 doris/grafana 变更。
