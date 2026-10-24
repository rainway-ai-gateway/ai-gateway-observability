USE ${STARROCKS_DATABASE};

-- 明细表：BFE AI 访问日志，列契约与 doris/sqls/bfe_ai_request_log.sql 逐列同名同型。
-- 建模差异：Doris 侧为 UNIQUE KEY，本表为 DUPLICATE KEY（append-only 日志天然唯一，
-- 不做更新去重；排序键四列与 Doris 一致，查询行为等价）。
CREATE TABLE bfe_ai_request_log (
    hostid                  VARCHAR(256)    COMMENT '主机标识，格式 hostname_netns',
    log_time                DATETIME        COMMENT '日志产生时间（UTC 墙钟）',
    ai_apikey_id            VARCHAR(256)    COMMENT 'API Key ID',
    ai_requested_model      VARCHAR(128)    COMMENT '请求模型名',

    logid                   BIGINT          COMMENT 'BFE 请求唯一标识',
    product                 VARCHAR(64)     COMMENT '产品标识',
    log_tag                 VARCHAR(64)     COMMENT '日志标签：req_<product> / req_err_<product>',

    -- 客户端连接
    client_ip               VARCHAR(64)     COMMENT '客户端 IP',
    client_network          VARCHAR(16)     COMMENT '客户端网络类型：Ipv4/Ipv6',
    -- 布尔列必须声明 BOOLEAN：payload 里是 JSON true/false，SR 的 JSON 导入只在 BOOLEAN
    -- 目标列上转成 1/0，声明成 TINYINT 会静默落 NULL（不报错、不计坏行；见
    -- docs/modifications/2026-10-22-issue-10-boolean-columns/design-changes.md）
    is_trust_src_ip         BOOLEAN         COMMENT '是否可信源 IP（JSON 布尔，落 0/1）',
    req_num                 INT             COMMENT '会话内请求序号（从 1 开始）',
    session_id              BIGINT          COMMENT '会话 ID',
    bfe_ip                  VARCHAR(64)     COMMENT 'BFE 服务器 IP',
    sock_src_ip             VARCHAR(64)     COMMENT 'Socket 源 IP',
    vip                     VARCHAR(64)     COMMENT '目的 VIP',
    vip6                    VARCHAR(128)    COMMENT '目的 VIP6',

    -- 错误信息
    err_code                VARCHAR(64)     COMMENT '错误码',
    err_msg                 VARCHAR(512)    COMMENT '错误详情',

    -- 请求头
    proto                   VARCHAR(16)     COMMENT 'HTTP 协议版本',
    header_host             VARCHAR(256)    COMMENT '请求 Host',
    origin_uri              VARCHAR(2048)   COMMENT '原始请求 URI',
    final_uri               VARCHAR(2048)   COMMENT '最终路由 URI',
    method                  VARCHAR(16)     COMMENT 'HTTP 方法',
    content_type            VARCHAR(128)    COMMENT '请求 Content-Type',
    x_forward_for           VARCHAR(1024)   COMMENT 'X-Forwarded-For',
    accept_language         VARCHAR(256)    COMMENT 'Accept-Language',
    authorization           VARCHAR(1024)   COMMENT 'Authorization 头',
    transfer_encoding       VARCHAR(64)     COMMENT 'Transfer-Encoding',
    referrer                VARCHAR(2048)   COMMENT 'Referer 头',
    user_agent              VARCHAR(1024)   COMMENT 'User-Agent 头',
    delegation              VARCHAR(256)    COMMENT '委托域名',
    uid                     VARCHAR(256)    COMMENT 'UID 头',
    cookie                  VARCHAR(4096)   COMMENT 'Cookie 头',
    -- 复杂嵌套列以 JSON 文本（VARCHAR）存储：SR 3.5 的 Routine Load 不支持
    -- VARCHAR 直转 ARRAY<STRUCT>（任务提交报 Not support cast），也不允许派生列
    -- 自引用（CAST(parse_json(col) ...) 会报 can't be found in column list）——
    -- 保留列名与 Doris 契约一致，类型按 SR 能力落地；查询侧用
    -- CAST(parse_json(col) AS ARRAY<STRUCT<...>>) 还原（见 bfe_ai_metrics_1m.sql）。
    req_headers             VARCHAR(65533)  COMMENT '请求头列表（JSON 文本，SR 侧以 VARCHAR 承载嵌套结构）',
    req_header_len          INT             COMMENT '请求头长度（字节）',
    req_body_len            INT             COMMENT '请求体长度（字节）',

    -- 路由
    cluster                 VARCHAR(256)    COMMENT '目标集群',
    sub_cluster             VARCHAR(256)    COMMENT '目标子集群',
    backend_info            VARCHAR(256)    COMMENT '后端 IP:Port',
    backend_retry           TINYINT         COMMENT '后端重试次数',

    -- 响应
    res_status_code         SMALLINT        COMMENT '响应状态码',
    res_header_len          INT             COMMENT '响应头长度（字节）',
    res_body_len            INT             COMMENT '响应体长度（字节）',
    res_content_type        VARCHAR(128)    COMMENT '响应 Content-Type',
    res_location            VARCHAR(2048)   COMMENT '响应 Location（3xx）',
    res_transfer_encoding   VARCHAR(64)     COMMENT '响应 Transfer-Encoding',
    res_headers             VARCHAR(65533)  COMMENT '响应头列表（JSON 文本，同 req_headers 口径）',

    -- 耗时（毫秒）
    all_time                INT             COMMENT '请求总耗时',
    read_client_time        INT             COMMENT '读客户端耗时',
    cluster_serve_time      INT             COMMENT '集群层耗时',
    backend_serve_time      INT             COMMENT '后端耗时',
    write_client_time       INT             COMMENT '写客户端耗时',
    connect_backend_time    INT             COMMENT '连接后端耗时',
    proxy_delay_time        INT             COMMENT '代理延迟',
    session_offset_time     INT             COMMENT '会话内时间偏移（毫秒）',

    -- AI 可观测 — API Key 标签（按层级打平，v1.1）
    level1Name              VARCHAR(128)    COMMENT 'Level1 标签名',
    level1                  VARCHAR(128)    COMMENT 'Level1 标签值',
    level2Name              VARCHAR(128)    COMMENT 'Level2 标签名',
    level2                  VARCHAR(128)    COMMENT 'Level2 标签值',
    level3Name              VARCHAR(128)    COMMENT 'Level3 标签名',
    level3                  VARCHAR(128)    COMMENT 'Level3 标签值',
    level4Name              VARCHAR(128)    COMMENT 'Level4 标签名',
    level4                  VARCHAR(128)    COMMENT 'Level4 标签值',
    level5Name              VARCHAR(128)    COMMENT 'Level5 标签名',
    level5                  VARCHAR(128)    COMMENT 'Level5 标签值',

    -- AI 可观测 (v0.2.0)
    ai_target_model         VARCHAR(128)    COMMENT '实际路由模型名',
    ai_stream               BOOLEAN         COMMENT '是否流式：0=非流式, 1=流式（JSON 布尔，落 0/1）',
    ai_input_tokens         BIGINT          COMMENT '输入 Token 数',
    ai_output_tokens        BIGINT          COMMENT '输出 Token 数',
    ai_total_tokens         BIGINT          COMMENT '总 Token 数',
    ai_cache_read_tokens    BIGINT          COMMENT '缓存读取 Token 数',
    ai_cache_write_tokens   BIGINT          COMMENT '缓存写入 Token 数',
    ai_audio_input_tokens   BIGINT          COMMENT '音频输入 Token 数',
    ai_audio_output_tokens  BIGINT          COMMENT '音频输出 Token 数',
    ai_image_count          BIGINT          COMMENT '图片数量',
    ai_ttft_us              BIGINT          COMMENT '首 Token 延迟 TTFT（微秒）',
    ai_tpot_us              BIGINT          COMMENT '每 Token 延迟 TPOT（微秒）',
    ai_provider             VARCHAR(64)     COMMENT '上游模型提供商',
    ai_protocol             VARCHAR(64)     COMMENT 'AI 协议',
    ai_mode                 VARCHAR(64)     COMMENT 'AI 模式',
    ai_retry_count          INT             COMMENT '模型调用层重试次数',
    ai_cost_value           BIGINT          COMMENT '成本固定点整数值',
    ai_cost_currency        VARCHAR(16)     COMMENT '成本币种',
    ai_route_rule_hits      VARCHAR(4096)   COMMENT 'AI 路由规则命中记录（JSON 文本，同 req_headers 口径）',
    ai_cluster_key_names    VARCHAR(4096)   COMMENT '尝试过的集群与 Key 名称组合（JSON 文本，同 req_headers 口径）',
    ai_rate_limit_hits      VARCHAR(4096)   COMMENT '限流命中列表（JSON 文本；打平列由 Routine Load 按 JSON 路径取首个命中）',
    -- 限流首个命中打平列（由 Routine Load 在导入时打平，与 Doris 版一致）
    rate_limit_policy_id    VARCHAR(128)    COMMENT '限流策略 ID（取首个命中，打平列）',
    rate_limit_type         VARCHAR(32)     COMMENT '限流类型（取首个命中，打平列）',
    rate_limit_rule_name    VARCHAR(128)    COMMENT '限流规则名（取首条规则，打平列）',
    ai_auth_reject_reason   VARCHAR(256)    COMMENT '认证拒绝原因',
    ai_auth_reject_quota_plans ARRAY<VARCHAR(128)> COMMENT '被拒绝的配额计划',
    ai_auth_hit_quota_plans ARRAY<VARCHAR(128)> COMMENT '成功请求时命中的配额计划',

    -- AI 可观测 — 缓存/镜像/意图（v0.8 报表二期）
    ai_cache_status         VARCHAR(16)     COMMENT '缓存状态：hit/miss/skip，空=未启用',
    mirror_hit              BOOLEAN         COMMENT '镜像是否命中',
    mirror_cluster          VARCHAR(128)    COMMENT '镜像集群',
    ai_intent_question      VARCHAR(64)     COMMENT '意图问题',
    ai_intent_answer        VARCHAR(64)     COMMENT '意图答案（含 unknown）',
    ai_intent_confidence    DOUBLE          COMMENT '意图置信度（NULL=未分类）',
    ai_intent_source        VARCHAR(32)     COMMENT '意图来源：explicit_header/classifier/cache',
    ai_intent_latency_us    BIGINT          COMMENT '意图决策耗时（微秒，NULL=未分类）',
    ai_intent_cache_hit     BOOLEAN         COMMENT '意图缓存命中（NULL=未分类）',
    ai_intent_questions_version VARCHAR(32) COMMENT '意图问题集版本'
)
DUPLICATE KEY(hostid, log_time, ai_apikey_id, ai_requested_model)
PARTITION BY RANGE(log_time) (
    PARTITION p_init VALUES LESS THAN ('${INIT_PARTITION_DATE}')
)
DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "compression" = "zstd"
);
