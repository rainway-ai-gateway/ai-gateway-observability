

CREATE TABLE bfe_ai_request_log (
    hostid                  VARCHAR(256)    COMMENT '主机标识，格式 hostname_netns',
    log_time                DATETIME        COMMENT '日志产生时间',
    ai_apikey               VARCHAR(256)    COMMENT 'API Key',
    ai_requested_model      VARCHAR(128)    COMMENT '请求模型名',

    logid                   BIGINT          COMMENT 'BFE 请求唯一标识',
    product                 VARCHAR(64)     COMMENT '产品标识',

    -- 客户端连接
    client_ip               VARCHAR(64)     COMMENT '客户端 IP',
    is_trust_src_ip         TINYINT         COMMENT '是否可信源 IP',

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

    -- 耗时（毫秒）
    all_time                INT             COMMENT '请求总耗时',
    read_client_time        INT             COMMENT '读客户端耗时',
    cluster_serve_time      INT             COMMENT '集群层耗时',
    backend_serve_time      INT             COMMENT '后端耗时',
    write_client_time       INT             COMMENT '写客户端耗时',
    connect_backend_time    INT             COMMENT '连接后端耗时',
    proxy_delay_time        INT             COMMENT '代理延迟',

    -- AI 可观测
    ai_apikeytags           ARRAY<STRUCT<
        tagname  : VARCHAR(128),
        tagvalue : VARCHAR(128)
    >>                                      COMMENT 'API Key 标签列表',
    ai_mapped_model         VARCHAR(128)    COMMENT '实际路由模型名',
    ai_stream               TINYINT         COMMENT '是否流式：0=非流式, 1=流式',
    ai_prompt_tokens        BIGINT          COMMENT '输入 Token 数',
    ai_output_tokens        BIGINT          COMMENT '输出 Token 数',
    ai_total_tokens         BIGINT          COMMENT '总 Token 数',
    ai_ttft_us              BIGINT          COMMENT '首 Token 延迟 TTFT（微秒）',
    ai_tpot_us              BIGINT          COMMENT '每 Token 延迟 TPOT（微秒）',
    ai_rate_limit_hits      ARRAY<STRUCT<
        rate_limit_policy_id : VARCHAR(128),
        rate_limit_type      : VARCHAR(32),
        rule_names           : ARRAY<VARCHAR(128)>
    >>                                      COMMENT '限流命中列表',
    ai_auth_reject_reason   VARCHAR(256)    COMMENT '认证拒绝原因',
    ai_auth_reject_quota_plans ARRAY<VARCHAR(128)> COMMENT '被拒绝的配额计划'
)
UNIQUE KEY(hostid, log_time, ai_apikey, ai_requested_model)
PARTITION BY RANGE(log_time) (
    PARTITION p_init VALUES LESS THAN ('2026-07-10')
)
DISTRIBUTED BY HASH(ai_apikey) BUCKETS 32
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
