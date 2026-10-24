-- ============================================================
-- BFE AI 请求日志明细表（ClickHouse 版，与 Doris 版同名同列同口径）
-- 设计依据：clickhouse/docs/modifications/2026-10-01-clickhouse-dock/design-changes.md
-- ============================================================
-- 口径要点（与 doris/sqls/bfe_ai_request_log.sql 对齐）：
--   * log_time 存 UTC 墙钟，由消费 MV 以 toDateTime(timestamp, 'UTC') 换算；
--   * 字符串列 DEFAULT '' / 数值列 DEFAULT 0，对齐 Doris 聚合 JOB 的 COALESCE 归一口径，
--     保证 `col != ''` 等既有谓词行为一致；
--   * Nullable 仅限 Doris 语义允许 NULL 的列（err_msg、ai_intent_confidence、
--     ai_intent_latency_us、ai_intent_cache_hit）；
--   * logid 为 UInt64：样例/线上取值（如 10602749765076101032）超出 Int64 上限，
--     Doris 非严格模式截断为边界值存储，ClickHouse 直接以 UInt64 承载原值；
--   * level1Name~level5（10 列）与 rate_limit_policy_id/type/rule_name（3 列）为
--     导入层（消费 MV bfe_ai_log_load_mv）打平列，同 Doris Routine Load COLUMNS 打平；
--   * 保留 7 天由 TTL 承担（对齐 Doris dynamic_partition start=-7）。
-- ============================================================
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.bfe_ai_request_log
(
    `hostid`                  String            DEFAULT '' COMMENT '主机标识，格式 hostname_netns',
    `log_time`                DateTime('UTC')            COMMENT '日志产生时间（UTC 墙钟）',
    `ai_apikey_id`            String            DEFAULT '' COMMENT 'API Key ID',
    `ai_requested_model`      String            DEFAULT '' COMMENT '请求模型名',

    `logid`                   UInt64            DEFAULT 0  COMMENT 'BFE 请求唯一标识',
    `product`                 String            DEFAULT '' COMMENT '产品标识',
    `log_tag`                 String            DEFAULT '' COMMENT '日志标签：req_<product> / req_err_<product>',

    -- 客户端连接
    `client_ip`               String            DEFAULT '' COMMENT '客户端 IP',
    `client_network`          String            DEFAULT '' COMMENT '客户端网络类型：Ipv4/Ipv6',
    `is_trust_src_ip`         UInt8             DEFAULT 0  COMMENT '是否可信源 IP',
    `req_num`                 Int32             DEFAULT 0  COMMENT '会话内请求序号（从 1 开始）',
    `session_id`              Int64             DEFAULT 0  COMMENT '会话 ID',
    `bfe_ip`                  String            DEFAULT '' COMMENT 'BFE 服务器 IP',
    `sock_src_ip`             String            DEFAULT '' COMMENT 'Socket 源 IP',
    `vip`                     String            DEFAULT '' COMMENT '目的 VIP',
    `vip6`                    String            DEFAULT '' COMMENT '目的 VIP6',

    -- 错误信息
    `err_code`                String            DEFAULT '' COMMENT '错误码',
    `err_msg`                 Nullable(String)           COMMENT '错误详情',

    -- 请求头
    `proto`                   String            DEFAULT '' COMMENT 'HTTP 协议版本',
    `header_host`             String            DEFAULT '' COMMENT '请求 Host',
    `origin_uri`              String            DEFAULT '' COMMENT '原始请求 URI',
    `final_uri`               String            DEFAULT '' COMMENT '最终路由 URI',
    `method`                  String            DEFAULT '' COMMENT 'HTTP 方法',
    `content_type`            String            DEFAULT '' COMMENT '请求 Content-Type',
    `x_forward_for`           String            DEFAULT '' COMMENT 'X-Forwarded-For',
    `accept_language`         String            DEFAULT '' COMMENT 'Accept-Language',
    `authorization`           String            DEFAULT '' COMMENT 'Authorization 头',
    `transfer_encoding`       String            DEFAULT '' COMMENT 'Transfer-Encoding',
    `referrer`                String            DEFAULT '' COMMENT 'Referer 头',
    `user_agent`              String            DEFAULT '' COMMENT 'User-Agent 头',
    `delegation`              String            DEFAULT '' COMMENT '委托域名',
    `uid`                     String            DEFAULT '' COMMENT 'UID 头',
    `cookie`                  String            DEFAULT '' COMMENT 'Cookie 头',
    `req_headers`             Nested(`key` String, `value` String) COMMENT '请求头列表',
    `req_header_len`          Int32             DEFAULT 0  COMMENT '请求头长度（字节）',
    `req_body_len`            Int32             DEFAULT 0  COMMENT '请求体长度（字节）',

    -- 路由
    `cluster`                 String            DEFAULT '' COMMENT '目标集群',
    `sub_cluster`             String            DEFAULT '' COMMENT '目标子集群',
    `backend_info`            String            DEFAULT '' COMMENT '后端 IP:Port',
    `backend_retry`           Int8              DEFAULT 0  COMMENT '后端重试次数',

    -- 响应
    `res_status_code`         Int16             DEFAULT 0  COMMENT '响应状态码',
    `res_header_len`          Int32             DEFAULT 0  COMMENT '响应头长度（字节）',
    `res_body_len`            Int32             DEFAULT 0  COMMENT '响应体长度（字节）',
    `res_content_type`        String            DEFAULT '' COMMENT '响应 Content-Type',
    `res_location`            String            DEFAULT '' COMMENT '响应 Location（3xx）',
    `res_transfer_encoding`   String            DEFAULT '' COMMENT '响应 Transfer-Encoding',
    `res_headers`             Nested(`key` String, `value` String) COMMENT '响应头列表',

    -- 耗时（毫秒）
    `all_time`                Int32             DEFAULT 0  COMMENT '请求总耗时',
    `read_client_time`        Int32             DEFAULT 0  COMMENT '读客户端耗时',
    `cluster_serve_time`      Int32             DEFAULT 0  COMMENT '集群层耗时',
    `backend_serve_time`      Int32             DEFAULT 0  COMMENT '后端耗时',
    `write_client_time`       Int32             DEFAULT 0  COMMENT '写客户端耗时',
    `connect_backend_time`    Int32             DEFAULT 0  COMMENT '连接后端耗时',
    `proxy_delay_time`        Int32             DEFAULT 0  COMMENT '代理延迟',
    `session_offset_time`     Int32             DEFAULT 0  COMMENT '会话内时间偏移（毫秒）',

    -- AI 可观测 — API Key 标签（按层级打平，导入层打平列）
    `level1Name`              String            DEFAULT '' COMMENT 'Level1 标签名',
    `level1`                  String            DEFAULT '' COMMENT 'Level1 标签值',
    `level2Name`              String            DEFAULT '' COMMENT 'Level2 标签名',
    `level2`                  String            DEFAULT '' COMMENT 'Level2 标签值',
    `level3Name`              String            DEFAULT '' COMMENT 'Level3 标签名',
    `level3`                  String            DEFAULT '' COMMENT 'Level3 标签值',
    `level4Name`              String            DEFAULT '' COMMENT 'Level4 标签名',
    `level4`                  String            DEFAULT '' COMMENT 'Level4 标签值',
    `level5Name`              String            DEFAULT '' COMMENT 'Level5 标签名',
    `level5`                  String            DEFAULT '' COMMENT 'Level5 标签值',

    -- AI 可观测
    `ai_target_model`         String            DEFAULT '' COMMENT '实际路由模型名',
    `ai_stream`               UInt8             DEFAULT 0  COMMENT '是否流式：0=非流式, 1=流式',
    `ai_input_tokens`         Int64             DEFAULT 0  COMMENT '输入 Token 数',
    `ai_output_tokens`        Int64             DEFAULT 0  COMMENT '输出 Token 数',
    `ai_total_tokens`         Int64             DEFAULT 0  COMMENT '总 Token 数',
    `ai_cache_read_tokens`    Int64             DEFAULT 0  COMMENT '缓存读取 Token 数',
    `ai_cache_write_tokens`   Int64             DEFAULT 0  COMMENT '缓存写入 Token 数',
    `ai_audio_input_tokens`   Int64             DEFAULT 0  COMMENT '音频输入 Token 数',
    `ai_audio_output_tokens`  Int64             DEFAULT 0  COMMENT '音频输出 Token 数',
    `ai_image_count`          Int64             DEFAULT 0  COMMENT '图片数量',
    `ai_ttft_us`              Int64             DEFAULT 0  COMMENT '首 Token 延迟 TTFT（微秒）',
    `ai_tpot_us`              Int64             DEFAULT 0  COMMENT '每 Token 延迟 TPOT（微秒）',
    `ai_provider`             String            DEFAULT '' COMMENT '上游模型提供商',
    `ai_protocol`             String            DEFAULT '' COMMENT 'AI 协议',
    `ai_mode`                 String            DEFAULT '' COMMENT 'AI 模式',
    `ai_retry_count`          Int32             DEFAULT 0  COMMENT '模型调用层重试次数',
    `ai_cost_value`           Int64             DEFAULT 0  COMMENT '成本固定点整数值',
    `ai_cost_currency`        String            DEFAULT '' COMMENT '成本币种',
    `ai_route_rule_hits`      Array(Tuple(rule_owner String, rule_owner_type String, rule_name String)) COMMENT 'AI 路由规则命中记录',
    `ai_cluster_key_names`    Array(Tuple(cluster_name String, key_name String)) COMMENT '尝试过的集群与 Key 名称组合',
    `ai_rate_limit_hits`      Array(Tuple(rate_limit_policy_id String, rate_limit_type String, rule_names Array(String))) COMMENT '限流命中列表',
    -- 限流首个命中打平列（由消费 MV 在导入时打平，对齐 Doris Routine Load COLUMNS 打平）
    `rate_limit_policy_id`    String            DEFAULT '' COMMENT '限流策略 ID（取首个命中，打平列）',
    `rate_limit_type`         String            DEFAULT '' COMMENT '限流类型（取首个命中，打平列）',
    `rate_limit_rule_name`    String            DEFAULT '' COMMENT '限流规则名（取首条规则，打平列）',
    `ai_auth_reject_reason`   String            DEFAULT '' COMMENT '认证拒绝原因',
    `ai_auth_reject_quota_plans` Array(String)  DEFAULT [] COMMENT '被拒绝的配额计划',
    `ai_auth_hit_quota_plans` Array(String)     DEFAULT [] COMMENT '成功请求时命中的配额计划',

    -- AI 可观测 — 缓存/镜像/意图（v0.8 报表二期）
    `ai_cache_status`         String            DEFAULT '' COMMENT '缓存状态：hit/miss/skip，空=未启用',
    `mirror_hit`              UInt8             DEFAULT 0  COMMENT '镜像是否命中（0/1）',
    `mirror_cluster`          String            DEFAULT '' COMMENT '镜像集群',
    `ai_intent_question`      String            DEFAULT '' COMMENT '意图问题',
    `ai_intent_answer`        String            DEFAULT '' COMMENT '意图答案（含 unknown）',
    `ai_intent_confidence`    Nullable(Float64)          COMMENT '意图置信度（NULL=未分类）',
    `ai_intent_source`        String            DEFAULT '' COMMENT '意图来源：explicit_header/classifier/cache',
    `ai_intent_latency_us`    Nullable(Int64)            COMMENT '意图决策耗时（微秒，NULL=未分类）',
    `ai_intent_cache_hit`     Nullable(UInt8)            COMMENT '意图缓存命中（NULL=未分类，0/1）',
    `ai_intent_questions_version` String        DEFAULT '' COMMENT '意图问题集版本'
)
ENGINE = MergeTree
PARTITION BY toDate(log_time)
ORDER BY (hostid, log_time, ai_apikey_id, ai_requested_model)
TTL log_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;
