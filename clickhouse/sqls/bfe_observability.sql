-- ============================================================
-- 创建可观测性数据库（ClickHouse）
-- 库名由 setup.conf 的 CLICKHOUSE_DATABASE 指定（setup.sh 用 sed 替换）
-- ============================================================
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE};
