USE ${STARROCKS_DATABASE};

-- 分钟聚合：异步物化视图，名字即 bfe_ai_metrics_1m（api 查询契约与 Doris 侧一致）。
-- StarRocks 无 Doris 式 CREATE JOB，基表增量刷新（分区粒度按天）+ date_trunc 分钟桶，
-- 语义等价 Doris JOB 的滑窗，端到端延迟 1~2 分钟。
-- 注意：基表加列后本 MV 不会自动携带，需重建 MV 才能进聚合口径（与 Doris 侧约束一致）。
-- 分区说明：SR 3.5 要求 PARTITION BY 引用 SELECT 输出列且可溯源基表分区列，
-- ts_min（minute 桶）与 log_time 之间无法通过校验，故首列暴露按天粒度的 ts_day
-- 作为分区列（比 ts_min 粗，不改变聚合行数）；api 查询用显式列名不受影响，
-- 仅 SELECT * 会多出一列（对报表查询无影响）。
CREATE MATERIALIZED VIEW bfe_ai_metrics_1m
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
PARTITION BY ts_day
DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "partition_ttl" = "7 DAY"
)
AS
SELECT
    date_trunc('day', log_time)                 AS ts_day,
    date_trunc('minute', log_time)              AS ts_min,
    COALESCE(hostid, '')                        AS hostid,
    COALESCE(ai_apikey_id, '')                  AS ai_apikey_id,
    COALESCE(ai_requested_model, '')            AS ai_requested_model,
    COALESCE(ai_target_model, '')               AS ai_target_model,
    COALESCE(ai_stream, 0)                      AS ai_stream,
    COALESCE(product, '')                       AS product,
    COALESCE(cluster, '')                       AS cluster,
    COALESCE(sub_cluster, '')                   AS sub_cluster,
    COALESCE(backend_info, '')                  AS backend_info,
    COALESCE(method, '')                        AS method,
    COALESCE(res_status_code, 0)                AS res_status_code,
    COALESCE(err_code, '')                      AS err_code,
    COALESCE(header_host, '')                   AS header_host,
    COALESCE(ai_provider, '')                   AS ai_provider,
    COALESCE(ai_protocol, '')                   AS ai_protocol,
    COALESCE(ai_mode, '')                       AS ai_mode,
    COALESCE(ai_cost_currency, '')              AS ai_cost_currency,
    COALESCE(level1Name, '')                    AS level1Name,
    COALESCE(level1, '')                        AS level1,
    COALESCE(level2Name, '')                    AS level2Name,
    COALESCE(level2, '')                        AS level2,
    COALESCE(level3Name, '')                    AS level3Name,
    COALESCE(level3, '')                        AS level3,
    COALESCE(level4Name, '')                    AS level4Name,
    COALESCE(level4, '')                        AS level4,
    COALESCE(level5Name, '')                    AS level5Name,
    COALESCE(level5, '')                        AS level5,
    COALESCE(rate_limit_policy_id, '')          AS rate_limit_policy_id,
    COALESCE(rate_limit_type, '')               AS rate_limit_type,
    COALESCE(rate_limit_rule_name, '')          AS rate_limit_rule_name,
    COALESCE(ai_auth_reject_reason, '')         AS ai_auth_reject_reason,
    -- 配额计划槽位打平：StarRocks 数组下标从 1 开始，越界返回 NULL
    COALESCE(ai_auth_reject_quota_plans[1], '') AS ai_auth_reject_quota_plans_slot1,
    COALESCE(ai_auth_reject_quota_plans[2], '') AS ai_auth_reject_quota_plans_slot2,
    COALESCE(ai_auth_reject_quota_plans[3], '') AS ai_auth_reject_quota_plans_slot3,
    COALESCE(ai_auth_reject_quota_plans[4], '') AS ai_auth_reject_quota_plans_slot4,
    COALESCE(ai_auth_reject_quota_plans[5], '') AS ai_auth_reject_quota_plans_slot5,
    COALESCE(ai_cache_status, '')               AS ai_cache_status,
    COALESCE(mirror_hit, 0)                     AS mirror_hit,
    COALESCE(ai_intent_answer, '')              AS ai_intent_answer,
    -- 指标（与 doris/sqls/bfe_ai_metrics_1m_job.sql 口径逐一对齐）
    COUNT(1)                             AS request_count,
    SUM(CASE WHEN err_code != '' AND err_code IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN ai_auth_reject_reason != '' AND ai_auth_reject_reason IS NOT NULL THEN 1 ELSE 0 END) AS auth_reject_count,
    SUM(COALESCE(ai_input_tokens, 0))    AS input_tokens,
    SUM(COALESCE(ai_output_tokens, 0))   AS output_tokens,
    SUM(COALESCE(ai_total_tokens, 0))    AS total_tokens,
    SUM(COALESCE(ai_ttft_us, 0))         AS ttft_us_sum,
    SUM(COALESCE(ai_tpot_us, 0))         AS tpot_us_sum,
    SUM(COALESCE(req_header_len, 0))     AS req_header_bytes,
    SUM(COALESCE(req_body_len, 0))       AS req_body_bytes,
    SUM(COALESCE(res_header_len, 0))     AS res_header_bytes,
    SUM(COALESCE(res_body_len, 0))       AS res_body_bytes,
    -- 限流命中计数：ai_rate_limit_hits 在 SR 侧为 VARCHAR(JSON 文本)，parse_json 还原数组后取长度；
    -- 未命中为 '[]'/NULL → 长度 0/NULL → 不计数（与 doris ARRAY_SIZE 口径等价）
    SUM(CASE WHEN array_length(CAST(parse_json(ai_rate_limit_hits) AS ARRAY<STRUCT<rate_limit_policy_id VARCHAR(128), rate_limit_type VARCHAR(32), rule_names ARRAY<VARCHAR(128)>>>)) > 0 THEN 1 ELSE 0 END) AS rate_limit_hits,
    SUM(COALESCE(backend_retry, 0))      AS backend_retries,
    SUM(COALESCE(all_time, 0))           AS all_time_sum,
    SUM(COALESCE(cluster_serve_time, 0)) AS cluster_serve_sum,
    SUM(COALESCE(backend_serve_time, 0)) AS backend_serve_sum,
    SUM(COALESCE(ai_retry_count, 0))     AS ai_retry_count_sum,
    SUM(COALESCE(ai_cost_value, 0))      AS ai_cost_value_sum,
    SUM(COALESCE(ai_cache_read_tokens, 0))  AS cache_read_tokens,
    SUM(COALESCE(ai_cache_write_tokens, 0)) AS cache_write_tokens,
    SUM(COALESCE(ai_audio_input_tokens, 0))  AS ai_audio_input_tokens,
    SUM(COALESCE(ai_audio_output_tokens, 0)) AS ai_audio_output_tokens,
    SUM(COALESCE(ai_image_count, 0))         AS ai_image_count
FROM bfe_ai_request_log
GROUP BY ts_day, ts_min, hostid, ai_apikey_id, ai_requested_model, ai_target_model, ai_stream,
         product, cluster, sub_cluster, backend_info, method, res_status_code,
         err_code, header_host, ai_provider, ai_protocol, ai_mode, ai_cost_currency,
         level1Name, level1, level2Name, level2,
         level3Name, level3, level4Name, level4,
         level5Name, level5,
         rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
         ai_auth_reject_reason,
         ai_auth_reject_quota_plans_slot1, ai_auth_reject_quota_plans_slot2,
         ai_auth_reject_quota_plans_slot3, ai_auth_reject_quota_plans_slot4,
         ai_auth_reject_quota_plans_slot5,
         ai_cache_status, mirror_hit, ai_intent_answer;
