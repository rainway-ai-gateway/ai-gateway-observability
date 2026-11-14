-- ============================================================
-- 存量升级：明细表新增缺价信号两列（bfe#1479）
-- 设计依据：clickhouse/docs/modifications/2026-10-30-issue-1479-price-miss-columns/design-changes.md
-- ============================================================
-- 本文件只含明细表的 ADD COLUMN（单语句，可重复执行）。完整升级顺序如下
-- （顺序不可颠倒，任一步失败都会让链路"少列"或"不消费"）：
--
--   1) 停消费：DROP TABLE <DB>.bfe_ai_log_load_mv;
--      旧 MV 的 SELECT 列表不含新列，不重建则两列永远为 NULL；
--      窗口内 Kafka 引擎表不被消费（位点不推进），消息不丢，仅延迟。
--   2) 重建 Kafka 暂存表（**不能 ALTER**）：
--        DROP TABLE <DB>.bfe_ai_log_kafka;
--        再按 sqls/bfe_ai_log_kafka.sql 重新创建。
--      ClickHouse 26.x 的 Kafka 存储引擎对 ALTER 一律返回
--      `Code: 48 ... Alter of type 'ADD_COLUMN' is not supported by storage Kafka`，
--      ADD/DROP COLUMN 都不支持，只能重建。Kafka 引擎表本身不存数据，
--      重建时保持 kafka_group_name 不变即可复用已提交位点继续消费
--      （集成测试 TC06 对该限制做了负向断言，避免有人改成 ALTER 写法）。
--   3) 明细表加列：执行本文件（幂等）。
--   4) 恢复消费：按 sqls/bfe_ai_log_load_mv.sql 重新创建消费 MV。
--
-- 新装环境无需任何升级动作：setup.sh 应用的 bfe_ai_request_log.sql /
-- bfe_ai_log_kafka.sql / bfe_ai_log_load_mv.sql 已含两列。
--
-- 列类型说明（为何 Nullable）：
--   * payload 里 ai_price_miss 是 JSON true/false，ai_price_warn 是 JSON 字符串；
--   * JSON null / 字段缺省落非 Nullable 列会变列默认值（0 / ''），"未上报"与
--     "显式未发生(0)"无法区分；Nullable 保证 NULL 语义与 MySQL 侧 DEFAULT NULL 一致。
-- ============================================================

ALTER TABLE ${CLICKHOUSE_DATABASE}.bfe_ai_request_log
    ADD COLUMN IF NOT EXISTS `ai_price_miss` Nullable(UInt8)
        COMMENT '是否发生缺价回退（NULL=未上报，0/1）',
    ADD COLUMN IF NOT EXISTS `ai_price_warn` Nullable(String)
        COMMENT '缺价形态：fallback/subitem_missing/no_entry/no_baseline/no_model_table/reject（NULL=未上报）';