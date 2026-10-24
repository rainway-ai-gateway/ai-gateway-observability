-- ============================================================
-- 聚合物化视图：明细表 → 分钟聚合表（等价 Doris CREATE JOB 分钟聚合）
-- 设计依据：clickhouse/docs/modifications/2026-10-01-clickhouse-dock/design-changes.md
-- ============================================================
-- 机制与口径说明：
--   * 随明细插入同步触发（秒级延迟，优于 Doris JOB 的分钟级）；分钟桶由
--     toStartOfMinute(log_time) 天然对齐 UTC 整分钟，无需 Doris JOB 的
--     时间窗谓词（WHERE log_time >= 上一分钟起点 AND < 本分钟起点）；
--   * 指标口径与 doris/sqls/bfe_ai_metrics_1m_job.sql 逐一对齐：
--       - request_count      = count()
--       - error_count        = sum(err_code != '')              -- 错误数
--       - auth_reject_count  = sum(ai_auth_reject_reason != '') -- 认证拒绝数
--       - rate_limit_hits    = sum(length(ai_rate_limit_hits) > 0) -- 命中限流的请求数
--       - quota_plans_slotN  = ai_auth_reject_quota_plans[N]（越界取类型默认值 ''）
--       - 其余 17 个指标       = sum(<明细列>)（明细列非空，等价 Doris COALESCE 归一）
--   * 默认不挂 POPULATE，从 MV 创建时刻起积累（同 Doris 口径）；
--     历史窗口重算：INSERT INTO bfe_ai_metrics_1m SELECT ... FROM bfe_ai_request_log
--     GROUP BY <全 40 维>（同本 MV 的 SELECT）。
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DATABASE}.bfe_ai_metrics_1m_mv
TO ${CLICKHOUSE_DATABASE}.bfe_ai_metrics_1m AS
SELECT
    toStartOfMinute(log_time) AS ts_min,
    hostid,
    ai_apikey_id,
    ai_requested_model,
    ai_target_model,
    ai_stream,
    product,
    cluster,
    sub_cluster,
    backend_info,
    method,
    res_status_code,
    err_code,
    header_host,
    ai_provider,
    ai_protocol,
    ai_mode,
    ai_cost_currency,
    level1Name,
    level1,
    level2Name,
    level2,
    level3Name,
    level3,
    level4Name,
    level4,
    level5Name,
    level5,
    rate_limit_policy_id,
    rate_limit_type,
    rate_limit_rule_name,
    ai_auth_reject_reason,
    ai_auth_reject_quota_plans[1] AS ai_auth_reject_quota_plans_slot1,
    ai_auth_reject_quota_plans[2] AS ai_auth_reject_quota_plans_slot2,
    ai_auth_reject_quota_plans[3] AS ai_auth_reject_quota_plans_slot3,
    ai_auth_reject_quota_plans[4] AS ai_auth_reject_quota_plans_slot4,
    ai_auth_reject_quota_plans[5] AS ai_auth_reject_quota_plans_slot5,
    ai_cache_status,
    mirror_hit,
    ai_intent_answer,
    -- 指标（口径见文件头注释）
    count()                                 AS request_count,
    sum(toInt64(err_code != ''))            AS error_count,
    sum(toInt64(ai_auth_reject_reason != '')) AS auth_reject_count,
    sum(ai_input_tokens)                    AS input_tokens,
    sum(ai_output_tokens)                   AS output_tokens,
    sum(ai_total_tokens)                    AS total_tokens,
    sum(ai_ttft_us)                         AS ttft_us_sum,
    sum(ai_tpot_us)                         AS tpot_us_sum,
    sum(req_header_len)                     AS req_header_bytes,
    sum(req_body_len)                       AS req_body_bytes,
    sum(res_header_len)                     AS res_header_bytes,
    sum(res_body_len)                       AS res_body_bytes,
    sum(toInt64(length(ai_rate_limit_hits) > 0)) AS rate_limit_hits,
    sum(backend_retry)                      AS backend_retries,
    sum(all_time)                           AS all_time_sum,
    sum(cluster_serve_time)                 AS cluster_serve_sum,
    sum(backend_serve_time)                 AS backend_serve_sum,
    sum(ai_retry_count)                     AS ai_retry_count_sum,
    sum(ai_cost_value)                      AS ai_cost_value_sum,
    sum(ai_cache_read_tokens)               AS cache_read_tokens,
    sum(ai_cache_write_tokens)              AS cache_write_tokens,
    sum(ai_audio_input_tokens)              AS ai_audio_input_tokens,
    sum(ai_audio_output_tokens)             AS ai_audio_output_tokens,
    sum(ai_image_count)                     AS ai_image_count
FROM ${CLICKHOUSE_DATABASE}.bfe_ai_request_log
GROUP BY
    ts_min,
    hostid,
    ai_apikey_id,
    ai_requested_model,
    ai_target_model,
    ai_stream,
    product,
    cluster,
    sub_cluster,
    backend_info,
    method,
    res_status_code,
    err_code,
    header_host,
    ai_provider,
    ai_protocol,
    ai_mode,
    ai_cost_currency,
    level1Name,
    level1,
    level2Name,
    level2,
    level3Name,
    level3,
    level4Name,
    level4,
    level5Name,
    level5,
    rate_limit_policy_id,
    rate_limit_type,
    rate_limit_rule_name,
    ai_auth_reject_reason,
    ai_auth_reject_quota_plans_slot1,
    ai_auth_reject_quota_plans_slot2,
    ai_auth_reject_quota_plans_slot3,
    ai_auth_reject_quota_plans_slot4,
    ai_auth_reject_quota_plans_slot5,
    ai_cache_status,
    mirror_hit,
    ai_intent_answer;
