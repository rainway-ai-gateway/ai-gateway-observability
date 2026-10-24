-- ============================================================
-- BFE AI 分钟聚合表（ClickHouse 版，与 Doris 版 40 维 + 24 指标逐列同名）
-- 设计依据：clickhouse/docs/modifications/2026-10-01-clickhouse-dock/design-changes.md
-- ============================================================
-- 引擎与查询纪律：
--   * 排序键必须为全 40 个维度列：SummingMergeTree 在后台 merge 时会将
--     "排序键完全相同"的行合并求和，非排序键维度列只保留其中一行的值——
--     若排序键只取前缀子集（如仅 6 列），维度组合仅在未排序维度上有差异的
--     行会被折叠，维度粒度丢失、分布统计被污染（冒烟实测证实）；
--   * SummingMergeTree((...)) 显式列出 24 个待求和指标列，双保险防止
--     数值维度列（res_status_code/mirror_hit 等）参与求和；
--   * 查询侧必须 `GROUP BY <维度> + sum(<指标>)` 兜底，禁止 SELECT * 直读；
--   * 40 个维度列 NOT NULL（Default ''/0），对齐 Doris AGGREGATE KEY 的
--     COALESCE 归一口径；指标列全为 Int64；
--   * 保留 7 天由 TTL 承担（对齐 Doris dynamic_partition start=-7）。
-- ============================================================
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.bfe_ai_metrics_1m
(
    `ts_min`             DateTime('UTC') COMMENT '分钟时间桶',
    `hostid`             String        DEFAULT '' COMMENT '主机标识',
    `ai_apikey_id`       String        DEFAULT '' COMMENT 'API Key ID',
    `ai_requested_model` String        DEFAULT '' COMMENT '请求模型',
    `ai_target_model`    String        DEFAULT '' COMMENT '路由模型',
    `ai_stream`          UInt8         DEFAULT 0  COMMENT '流式标识',
    `product`            String        DEFAULT '' COMMENT '产品线',
    `cluster`            String        DEFAULT '' COMMENT '集群',
    `sub_cluster`        String        DEFAULT '' COMMENT '子集群',
    `backend_info`       String        DEFAULT '' COMMENT '后端节点',
    `method`             String        DEFAULT '' COMMENT 'HTTP 方法',
    `res_status_code`    Int16         DEFAULT 0  COMMENT '响应状态码',
    `err_code`           String        DEFAULT '' COMMENT '错误码',
    `header_host`        String        DEFAULT '' COMMENT '请求 Host',
    `ai_provider`        String        DEFAULT '' COMMENT '上游模型提供商',
    `ai_protocol`        String        DEFAULT '' COMMENT 'AI 协议',
    `ai_mode`            String        DEFAULT '' COMMENT 'AI 模式',
    `ai_cost_currency`   String        DEFAULT '' COMMENT '成本币种',
    `level1Name`         String        DEFAULT '' COMMENT 'Level1 标签名',
    `level1`             String        DEFAULT '' COMMENT 'Level1 标签值',
    `level2Name`         String        DEFAULT '' COMMENT 'Level2 标签名',
    `level2`             String        DEFAULT '' COMMENT 'Level2 标签值',
    `level3Name`         String        DEFAULT '' COMMENT 'Level3 标签名',
    `level3`             String        DEFAULT '' COMMENT 'Level3 标签值',
    `level4Name`         String        DEFAULT '' COMMENT 'Level4 标签名',
    `level4`             String        DEFAULT '' COMMENT 'Level4 标签值',
    `level5Name`         String        DEFAULT '' COMMENT 'Level5 标签名',
    `level5`             String        DEFAULT '' COMMENT 'Level5 标签值',
    `rate_limit_policy_id` String      DEFAULT '' COMMENT '限流策略ID',
    `rate_limit_type`    String        DEFAULT '' COMMENT '限流类型',
    `rate_limit_rule_name` String      DEFAULT '' COMMENT '限流规则名',
    `ai_auth_reject_reason` String     DEFAULT '' COMMENT '认证拒绝原因',
    `ai_auth_reject_quota_plans_slot1` String DEFAULT '' COMMENT '被拒绝配额计划槽位1',
    `ai_auth_reject_quota_plans_slot2` String DEFAULT '' COMMENT '被拒绝配额计划槽位2',
    `ai_auth_reject_quota_plans_slot3` String DEFAULT '' COMMENT '被拒绝配额计划槽位3',
    `ai_auth_reject_quota_plans_slot4` String DEFAULT '' COMMENT '被拒绝配额计划槽位4',
    `ai_auth_reject_quota_plans_slot5` String DEFAULT '' COMMENT '被拒绝配额计划槽位5',
    `ai_cache_status`    String        DEFAULT '' COMMENT '缓存状态（hit/miss/skip，空=未启用）',
    `mirror_hit`         UInt8         DEFAULT 0  COMMENT '镜像命中（0/1）',
    `ai_intent_answer`   String        DEFAULT '' COMMENT '意图答案（含unknown，空=未分类）',

    -- 聚合指标（SummingMergeTree 显式求和列）
    `request_count`      Int64 COMMENT '请求数',
    `error_count`        Int64 COMMENT '错误数',
    `auth_reject_count`  Int64 COMMENT '认证拒绝数',
    `input_tokens`       Int64 COMMENT '输入 Token 累计',
    `output_tokens`      Int64 COMMENT '输出 Token 累计',
    `total_tokens`       Int64 COMMENT '总 Token 累计',
    `ttft_us_sum`        Int64 COMMENT 'TTFT 累计（微秒）',
    `tpot_us_sum`        Int64 COMMENT 'TPOT 累计（微秒）',
    `req_header_bytes`   Int64 COMMENT '请求头字节累计',
    `req_body_bytes`     Int64 COMMENT '请求体字节累计',
    `res_header_bytes`   Int64 COMMENT '响应头字节累计',
    `res_body_bytes`     Int64 COMMENT '响应体字节累计',
    `rate_limit_hits`    Int64 COMMENT '限流命中次数',
    `backend_retries`    Int64 COMMENT '后端重试总次数',
    `all_time_sum`       Int64 COMMENT '总耗时累计（毫秒）',
    `cluster_serve_sum`  Int64 COMMENT '集群层耗时累计',
    `backend_serve_sum`  Int64 COMMENT '后端耗时累计',
    `ai_retry_count_sum` Int64 COMMENT '模型层重试总次数',
    `ai_cost_value_sum`  Int64 COMMENT '成本累计（固定点整数）',
    `cache_read_tokens`  Int64 COMMENT '缓存读取 Token 累计',
    `cache_write_tokens` Int64 COMMENT '缓存写入 Token 累计',
    `ai_audio_input_tokens`  Int64 COMMENT '音频输入 Token 累计',
    `ai_audio_output_tokens` Int64 COMMENT '音频输出 Token 累计',
    `ai_image_count`     Int64 COMMENT '图片数量累计'
)
ENGINE = SummingMergeTree((
    request_count, error_count, auth_reject_count,
    input_tokens, output_tokens, total_tokens,
    ttft_us_sum, tpot_us_sum,
    req_header_bytes, req_body_bytes, res_header_bytes, res_body_bytes,
    rate_limit_hits, backend_retries,
    all_time_sum, cluster_serve_sum, backend_serve_sum, ai_retry_count_sum,
    ai_cost_value_sum, cache_read_tokens, cache_write_tokens,
    ai_audio_input_tokens, ai_audio_output_tokens, ai_image_count))
PARTITION BY toDate(ts_min)
ORDER BY (ts_min, hostid, ai_apikey_id, ai_requested_model, ai_target_model, ai_stream,
          product, cluster, sub_cluster, backend_info, method, res_status_code,
          err_code, header_host, ai_provider, ai_protocol, ai_mode, ai_cost_currency,
          level1Name, level1, level2Name, level2, level3Name, level3,
          level4Name, level4, level5Name, level5,
          rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
          ai_auth_reject_reason,
          ai_auth_reject_quota_plans_slot1, ai_auth_reject_quota_plans_slot2,
          ai_auth_reject_quota_plans_slot3, ai_auth_reject_quota_plans_slot4,
          ai_auth_reject_quota_plans_slot5,
          ai_cache_status, mirror_hit, ai_intent_answer)
TTL ts_min + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;
