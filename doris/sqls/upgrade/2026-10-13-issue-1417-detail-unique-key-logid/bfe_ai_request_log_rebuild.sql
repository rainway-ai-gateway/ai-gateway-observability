USE ${DORIS_DATABASE};

-- ============================================================
-- 存量升级：明细表 bfe_ai_request_log 唯一键补请求级判别列 logid（bfe#1417）
-- 日期：2026-10-13（数据报表 v0.8）
-- 对应修改说明：doris/docs/modifications/2026-10-13-issue-1417-detail-unique-key-logid/design-changes.md
-- 操作步骤（HOWTO §11.5）：
--   1) 前置：SELECT COUNT(*) FROM bfe_ai_request_log WHERE logid IS NULL;（预期 0，
--      非 0 由本脚本的 IFNULL(logid, 0) 兜底）；
--   2) 按新 schema 建 _v2 —— 本脚本不建表，沿用 §11.3 聚合表重建的做法，用新 DDL
--      做表名替换，保证列清单单一来源（sqls/bfe_ai_request_log.sql）：
--        sed -e 's|CREATE TABLE bfe_ai_request_log|CREATE TABLE bfe_ai_request_log_v2|' \
--            sqls/bfe_ai_request_log.sql | mysql -h${DORIS_HOST} -P${DORIS_PORT} -u${DORIS_USER}
--   3) 存量环境先暂停消费（Kafka 消息留存，不丢）：
--        PAUSE ROUTINE LOAD FOR bfe_ai_log_load;
--   4) 执行本脚本（回灌 + 原子换名）；
--   5) 恢复消费（映射 COLUMNS 不含键信息，无需改动）：
--        RESUME ROUTINE LOAD FOR bfe_ai_log_load;
--   6) 验证（见文件末第 3 节）通过后清理旧表：
--        DROP TABLE bfe_ai_request_log_v2;
--
-- 为什么必须重建：Doris 的 KEY 列集合变更不支持在线 schema change（同 §11.3 先例）。
-- 两侧一致性：MySQL 侧同键（五列 + logid NOT NULL DEFAULT 0）见 ai-gateway-api@64f278e。
--
-- 说明：
--   1) logid 为 KEY 列 → 新表要求 NOT NULL；存量行可能为 NULL（历史写入侧缺陷），
--      回灌用 IFNULL(logid, 0) 归一——0 与旧四元组语义等价（0 参与键比较），
--      既不产生重复行、也不改变历史（历史行在旧键下已互相合并，不可回溯）；
--   2) 回灌范围：动态分区保留 7 天、明细按天滚动，数据量有界；如需缩短窗口，
--      在下方 SELECT 末尾追加 WHERE log_time >= '<起点>'；
--   3) 每环境执行一次（非幂等）：重复执行时 _v2 已不存在或已换名，请按 HOWTO 重建；
--   4) Doris 3.0.8 的原子换名语法为 REPLACE WITH TABLE ... PROPERTIES('swap'='true')；
--      不支持时降级为两次 RENAME（秒级窗口，期间查询报表不存在）。
-- ============================================================

-- 1) 回灌（列顺序与新表完全一致；logid 做 NULL 兜底）
INSERT INTO bfe_ai_request_log_v2
SELECT
    hostid, log_time, ai_apikey_id, ai_requested_model, IFNULL(logid, 0) AS logid,
    product, log_tag, client_ip, client_network, is_trust_src_ip,
    req_num, session_id, bfe_ip, sock_src_ip, vip,
    vip6, err_code, err_msg, proto, header_host,
    origin_uri, final_uri, method, content_type, x_forward_for,
    accept_language, authorization, transfer_encoding, referrer, user_agent,
    delegation, uid, cookie, req_headers, req_header_len,
    req_body_len, cluster, sub_cluster, backend_info, backend_retry,
    res_status_code, res_header_len, res_body_len, res_content_type, res_location,
    res_transfer_encoding, res_headers, all_time, read_client_time, cluster_serve_time,
    backend_serve_time, write_client_time, connect_backend_time, proxy_delay_time, session_offset_time,
    level1Name, level1, level2Name, level2, level3Name,
    level3, level4Name, level4, level5Name, level5,
    ai_target_model, ai_stream, ai_input_tokens, ai_output_tokens, ai_total_tokens,
    ai_cache_read_tokens, ai_cache_write_tokens, ai_audio_input_tokens, ai_audio_output_tokens, ai_image_count,
    ai_ttft_us, ai_tpot_us, ai_provider, ai_protocol, ai_mode,
    ai_retry_count, ai_cost_value, ai_cost_currency, ai_route_rule_hits, ai_cluster_key_names,
    ai_rate_limit_hits, rate_limit_policy_id, rate_limit_type, rate_limit_rule_name, ai_auth_reject_reason,
    ai_auth_reject_quota_plans, ai_auth_hit_quota_plans, ai_cache_status, mirror_hit, mirror_cluster,
    ai_intent_question, ai_intent_answer, ai_intent_confidence, ai_intent_source, ai_intent_latency_us,
    ai_intent_cache_hit, ai_intent_questions_version
FROM bfe_ai_request_log;

-- 2) 原子换名（换名后 bfe_ai_request_log 指向新表，_v2 指向旧表；Routine Load 同名继续生效）
ALTER TABLE bfe_ai_request_log REPLACE WITH TABLE bfe_ai_request_log_v2 PROPERTIES('swap' = 'true');

-- 3) 验证（人工确认，全部通过后再做第 4/5 节）
--    SHOW CREATE TABLE bfe_ai_request_log;
--      → UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model, logid)，logid 为 NOT NULL
--    SELECT logid FROM bfe_ai_request_log WHERE logid IS NULL;
--      → 空结果（键列非 NULL）
--    SELECT log_time, COUNT(*) AS c FROM bfe_ai_request_log
--     WHERE log_time >= NOW() - INTERVAL 10 MINUTE
--     GROUP BY log_time, hostid, ai_apikey_id, ai_requested_model
--     ORDER BY c DESC LIMIT 5;
--      → 同秒并发应 > 1（修复后；修复前恒为 1）

-- 4) 恢复消费（存量环境手工执行）
--    RESUME ROUTINE LOAD FOR bfe_ai_log_load;

-- 5) 清理（验证通过后；换名后 _v2 名指向旧表）
--    DROP TABLE bfe_ai_request_log_v2;