-- ============================================================
-- 存量升级：明细表新增缺价信号两列（bfe#1479）
-- 设计依据：doris/docs/modifications/2026-10-30-issue-1479-price-miss-columns/design-changes.md
-- ============================================================
-- 适用对象：已按 2026-10-30 之前的 SQL 资产建表的存量环境（新装环境直接用 sqls/bfe_ai_request_log.sql）。
--
-- 执行顺序（不可颠倒）：
--   1) 本脚本：明细表加两列（每环境**仅执行一次**，重复执行报
--      "Can not add column which already exists in base table"）；
--   2) 重建 Routine Load：Doris 不支持在线修改 COLUMNS，必须
--      STOP ROUTINE LOAD FOR bfe_ai_log_load;
--      DROP ROUTINE LOAD FOR bfe_ai_log_load;
--      再按 sqls/bfe_ai_log_load_routine.sql 重建（COLUMNS 已含两列）。
--      若先重建 job 而表上无两列，job 创建即报列不存在。
--
-- 列类型说明：
--   * ai_price_miss 必须是 BOOLEAN：payload 里是 JSON true/false，Doris 的 JSON
--     导入对 BOOLEAN 列转 1/0；从跨引擎一致性与 StarRocks issue #10 的教训出发，
--     统一用 BOOLEAN（TINYINT 在 SR 侧会静默落 NULL）；
--   * 两列均可空（不加 NOT NULL / DEFAULT），字段缺省即 NULL = 未上报，
--     与 MySQL 权威 DDL 的 DEFAULT NULL 一致；不能用 DEFAULT '' 把"未上报"抹平。
-- ============================================================

ALTER TABLE bfe_ai_request_log
    ADD COLUMN ai_price_miss BOOLEAN
        COMMENT '是否发生缺价回退（NULL=未上报，1/0）' AFTER ai_intent_questions_version,
    ADD COLUMN ai_price_warn VARCHAR(32)
        COMMENT '缺价形态：fallback/subitem_missing/no_entry/no_baseline/no_model_table/reject（NULL=未上报）' AFTER ai_price_miss;