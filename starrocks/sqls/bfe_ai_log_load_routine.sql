USE ${STARROCKS_DATABASE};

-- Routine Load：与 doris/sqls/bfe_ai_log_load_routine.sql 同构，两处 SR 方言改写：
--   1) level 打平用 get_json_string(ai_apikeytags, '$.levelN.x')——SR 3.5 的
--      json_extract 不支持 (varchar, varchar) 参数（任务运行时报 no matching function），
--      get_json_string 直接返回反引号去除后的字符串，无需再 json_unquote；
--   2) 限流打平用 get_json_string(ai_rate_limit_hits, '$[0].xxx')——COLUMNS 表达式
--      求值上下文中复杂列的 JSON 源以 VARCHAR 承载（子脚本会报 cannot subscript
--      VARCHAR），按 JSON 文本路径取首个命中的打平值。
-- 明细表五个嵌套列（req_headers/res_headers/ai_route_rule_hits/ai_cluster_key_names/
-- ai_rate_limit_hits）在 SR 侧以 VARCHAR 承载 JSON 文本（DDL 注释有说明）：SR 3.5
-- 不支持 VARCHAR 直转 ARRAY<STRUCT>、也不允许派生列自引用，裸映射即可落库。
-- 消费组独立（KAFKA_GROUP_ID，缺省 starrocks_bfe_ai_log），与 Doris 链路位点互不影响。
-- 注意：CREATE ROUTINE LOAD 只校验语法，列表达式在 BE 任务创建时才分析——
-- 表达式兼容性必须以真实消费（starrocks-it TC02）验证，不能只看任务创建成功。
CREATE ROUTINE LOAD bfe_ai_log_load ON bfe_ai_request_log
COLUMNS(
    logid,
    timestamp,
    -- 存储 UTC 墙钟（与 MySQL 链路 log-reader 写入口径一致）：FROM_UNIXTIME 按会话时区
    -- 解释 epoch，此处减去会话相对 UTC 的偏移，保证任何 FE 时区下结果一致。
    log_time         = DATE_SUB(FROM_UNIXTIME(timestamp), INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND),
    product,
    log_tag,
    hostid,
    client_ip,
    client_network,
    is_trust_src_ip,
    req_num,
    session_id,
    bfe_ip,
    sock_src_ip,
    vip,
    vip6,
    err_code,
    err_msg,
    req_header_len,
    req_body_len,
    proto,
    header_host,
    origin_uri,
    final_uri,
    method,
    content_type,
    x_forward_for,
    accept_language,
    authorization,
    transfer_encoding,
    referrer,
    user_agent,
    delegation,
    uid,
    cookie,
    req_headers,
    cluster,
    sub_cluster,
    backend_info,
    backend_retry,
    res_status_code,
    res_header_len,
    res_body_len,
    res_content_type,
    res_location,
    res_transfer_encoding,
    res_headers,
    all_time,
    read_client_time,
    cluster_serve_time,
    backend_serve_time,
    write_client_time,
    connect_backend_time,
    proxy_delay_time,
    session_offset_time,
    ai_apikey_id,
    ai_apikeytags,
    level1Name       = get_json_string(ai_apikeytags, '$.level1.tagname'),
    level1           = get_json_string(ai_apikeytags, '$.level1.tagvalue'),
    level2Name       = get_json_string(ai_apikeytags, '$.level2.tagname'),
    level2           = get_json_string(ai_apikeytags, '$.level2.tagvalue'),
    level3Name       = get_json_string(ai_apikeytags, '$.level3.tagname'),
    level3           = get_json_string(ai_apikeytags, '$.level3.tagvalue'),
    level4Name       = get_json_string(ai_apikeytags, '$.level4.tagname'),
    level4           = get_json_string(ai_apikeytags, '$.level4.tagvalue'),
    level5Name       = get_json_string(ai_apikeytags, '$.level5.tagname'),
    level5           = get_json_string(ai_apikeytags, '$.level5.tagvalue'),
    ai_requested_model,
    ai_target_model,
    ai_stream,
    ai_input_tokens,
    ai_output_tokens,
    ai_total_tokens,
    ai_cache_read_tokens,
    ai_cache_write_tokens,
    ai_audio_input_tokens,
    ai_audio_output_tokens,
    ai_image_count,
    ai_ttft_us,
    ai_tpot_us,
    ai_provider,
    ai_protocol,
    ai_mode,
    ai_retry_count,
    ai_cost_value,
    ai_cost_currency,
    ai_route_rule_hits,
    ai_cluster_key_names,
    ai_rate_limit_hits,
    rate_limit_policy_id = get_json_string(ai_rate_limit_hits, '$[0].rate_limit_policy_id'),
    rate_limit_type      = get_json_string(ai_rate_limit_hits, '$[0].rate_limit_type'),
    rate_limit_rule_name = get_json_string(ai_rate_limit_hits, '$[0].rule_names[0]'),
    ai_auth_reject_reason,
    ai_auth_reject_quota_plans,
    ai_auth_hit_quota_plans,
    ai_cache_status,
    mirror_hit,
    mirror_cluster,
    ai_intent_question,
    ai_intent_answer,
    ai_intent_confidence,
    ai_intent_source,
    ai_intent_latency_us,
    ai_intent_cache_hit,
    ai_intent_questions_version
)
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_batch_rows" = "250000",
    "max_error_number" = "1000",
    "format" = "json"
)
FROM KAFKA (
    "kafka_broker_list" = "${KAFKA_BROKER_LIST}",
    "kafka_topic" = "${KAFKA_TOPIC}",
    -- 首次创建从分区起始位置消费（缺省 OFFSET_END/LATEST 会在 job 创建瞬间
    -- 锁定末尾偏移，创建前已产生的日志消息被永久跳过；对日志入仓链路而言
    -- 历史消息应全部加载）。
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    "property.group.id" = "${KAFKA_GROUP_ID}",
    "property.client.id" = "${KAFKA_CLIENT_ID}"
);
