USE ${DORIS_DATABASE};

-- ============================================================
-- 存量升级：明细表 bfe_ai_request_log 增加缓存/镜像/意图 10 列 + 限流打平 3 列
-- 日期：2026-09-29（数据报表二期，v0.8）
-- 对应修改说明：doris/docs/modifications/2026-09-29-report-cache-mirror-intent-doris-alignment/design-changes.md
--
-- 说明：
--   1. 在线 ALTER（LIGHT Schema Change），不阻塞读写；
--   2. 每个环境仅执行一次：Doris 3.0 不支持 ADD COLUMN IF NOT EXISTS，
--      重复执行会因 Duplicate column 报错（属预期，可忽略后跳过）；
--   3. 变量替换方式与 setup.sh 一致（sed 替换 ${DORIS_DATABASE}），
--      或直接手动替换后执行；
--   4. 限流打平 3 列（rate_limit_policy_id/type/rule_name）：既有 INSERT JOB 需要
--      从 ARRAY<STRUCT> 提取限流首个命中，而 Doris 3.0 不支持 struct 字段解引用，
--      改由 Routine Load 在导入时打平（json_extract），明细表需相应加列；
--   5. 本文件只负责加列；Routine Load 映射重建与聚合表重建见 HOWTO.md
--      「存量部署升级（v0.8 报表二期）」一节。
-- ============================================================

ALTER TABLE bfe_ai_request_log
    ADD COLUMN ai_cache_status         VARCHAR(16)     COMMENT '缓存状态：hit/miss/skip，空=未启用',
    ADD COLUMN mirror_hit              BOOLEAN         COMMENT '镜像是否命中',
    ADD COLUMN mirror_cluster          VARCHAR(128)    COMMENT '镜像集群',
    ADD COLUMN ai_intent_question      VARCHAR(64)     COMMENT '意图问题',
    ADD COLUMN ai_intent_answer        VARCHAR(64)     COMMENT '意图答案（含 unknown）',
    ADD COLUMN ai_intent_confidence    DOUBLE          COMMENT '意图置信度（NULL=未分类）',
    ADD COLUMN ai_intent_source        VARCHAR(32)     COMMENT '意图来源：explicit_header/classifier/cache',
    ADD COLUMN ai_intent_latency_us    BIGINT          COMMENT '意图决策耗时（微秒，NULL=未分类）',
    ADD COLUMN ai_intent_cache_hit     BOOLEAN         COMMENT '意图缓存命中（NULL=未分类）',
    ADD COLUMN ai_intent_questions_version VARCHAR(32) COMMENT '意图问题集版本',
    ADD COLUMN rate_limit_policy_id    VARCHAR(128)    COMMENT '限流策略 ID（取首个命中，打平列）',
    ADD COLUMN rate_limit_type         VARCHAR(32)     COMMENT '限流类型（取首个命中，打平列）',
    ADD COLUMN rate_limit_rule_name    VARCHAR(128)    COMMENT '限流规则名（取首条规则，打平列）';
