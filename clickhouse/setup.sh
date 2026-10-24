#!/usr/bin/env bash
# ============================================================
# BFE Observability - ClickHouse Setup Script
# ============================================================
# 在已有的 ClickHouse + Kafka 集群上创建可观测性数据库、
# 明细表、Kafka 引擎暂存表、消费 MV、聚合表和聚合 MV。
# 数据库名由配置文件中的 CLICKHOUSE_DATABASE 指定。
#
# 用法:
#   bash setup.sh                    # 使用默认 setup.conf
#   bash setup.sh setup_test.conf    # 使用测试配置（数据库 bfe_observability_test）
#   bash setup.sh my.conf            # 使用自定义配置文件
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-${SCRIPT_DIR}/setup.conf}"
SQL_DIR="${SCRIPT_DIR}/sqls"

# --- 颜色输出 ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "\n${GREEN}==>${NC} $*"; }

# --- 加载配置 ---
if [[ ! -f "${CONFIG_FILE}" ]]; then
    log_error "配置文件不存在: ${CONFIG_FILE}"
    exit 1
fi
# shellcheck source=setup.conf
source "${CONFIG_FILE}"

# 设置默认值
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-bfe_observability}"
KAFKA_BROKER_LIST="${KAFKA_BROKER_LIST:-172.18.1.244:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-bfe_ai_log}"
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-clickhouse_bfe_ai_log}"

# 构建 clickhouse-client 命令（SQL 经 stdin 传入，--multiquery 支持多语句）
CLICKHOUSE_CMD="clickhouse-client -h${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT} -u${CLICKHOUSE_USER} --multiquery"
if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
    CLICKHOUSE_CMD="${CLICKHOUSE_CMD} --password=${CLICKHOUSE_PASSWORD}"
fi

# --- 检查前置条件 ---
check_prerequisites() {
    log_step "检查前置条件"

    if ! command -v clickhouse-client &>/dev/null; then
        log_error "未找到 clickhouse-client，请安装 clickhouse-client"
        exit 1
    fi
    log_info "clickhouse-client 已就绪"

    if ! ${CLICKHOUSE_CMD} -q "SELECT 1" &>/dev/null; then
        log_error "无法连接 ClickHouse (${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT})，请检查配置"
        exit 1
    fi
    log_info "ClickHouse 连接成功 (${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT})"
}

# --- 执行 SQL 文件 ---
# SQL 文件使用 ${VAR} 占位符引用 setup.conf 中的变量，此处替换后经 stdin 执行
run_sql_file() {
    local desc="$1"
    local sql_file="$2"

    log_step "${desc}"

    if [[ ! -f "${sql_file}" ]]; then
        log_error "SQL 文件不存在: ${sql_file}"
        exit 1
    fi

    if ! sed \
        -e 's|\${CLICKHOUSE_DATABASE}|'"${CLICKHOUSE_DATABASE}"'|g' \
        -e 's|\${KAFKA_BROKER_LIST}|'"${KAFKA_BROKER_LIST}"'|g' \
        -e 's|\${KAFKA_TOPIC}|'"${KAFKA_TOPIC}"'|g' \
        -e 's|\${KAFKA_GROUP_ID}|'"${KAFKA_GROUP_ID}"'|g' \
        "${sql_file}" | ${CLICKHOUSE_CMD}; then
        log_error "${desc} — 失败"
        exit 1
    fi
    log_info "${desc} — 完成"
}

# --- 打印配置摘要 ---
print_config() {
    echo ""
    echo "============================================"
    echo "  BFE Observability ClickHouse Setup"
    echo "============================================"
    echo "  ClickHouse: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
    echo "  Database:   ${CLICKHOUSE_DATABASE}"
    echo "  Kafka:      ${KAFKA_BROKER_LIST}"
    echo "  Topic:      ${KAFKA_TOPIC}"
    echo "  Group:      ${KAFKA_GROUP_ID}"
    echo "============================================"
    echo ""
}

# --- 主流程 ---
main() {
    print_config

    check_prerequisites

    # Step 1: 创建数据库
    run_sql_file "创建数据库 ${CLICKHOUSE_DATABASE}" "${SQL_DIR}/bfe_observability.sql"

    # Step 2: 创建明细表
    run_sql_file "创建明细表 bfe_ai_request_log" "${SQL_DIR}/bfe_ai_request_log.sql"

    # Step 3: 创建 Kafka 引擎暂存表（需要替换 Kafka 参数）
    run_sql_file "创建 Kafka 引擎暂存表 bfe_ai_log_kafka" "${SQL_DIR}/bfe_ai_log_kafka.sql"

    # Step 4: 创建消费 MV（Kafka → 明细表打平）
    run_sql_file "创建消费 MV bfe_ai_log_load_mv" "${SQL_DIR}/bfe_ai_log_load_mv.sql"

    # Step 5: 创建聚合表
    run_sql_file "创建聚合表 bfe_ai_metrics_1m" "${SQL_DIR}/bfe_ai_metrics_1m.sql"

    # Step 6: 创建聚合 MV（明细 → 分钟聚合）
    run_sql_file "创建聚合 MV bfe_ai_metrics_1m_mv" "${SQL_DIR}/bfe_ai_metrics_1m_mv.sql"

    echo ""
    log_info "============================================"
    log_info "  全部创建完成！"
    log_info "============================================"
    echo ""
    echo "验证命令:"
    echo "  # 查看表与 MV"
    echo "  ${CLICKHOUSE_CMD} -q \"SHOW TABLES FROM ${CLICKHOUSE_DATABASE}\""
    echo ""
    echo "  # 查看 Kafka 消费进度（ lag 列应为非负且趋稳）"
    echo "  ${CLICKHOUSE_CMD} -q \"SELECT database, table, consumer_id, num_commits, num_messages_read FROM system.kafka_consumers WHERE database = '${CLICKHOUSE_DATABASE}'\""
    echo ""
    echo "  # 查看明细/聚合行数"
    echo "  ${CLICKHOUSE_CMD} -q \"SELECT count() FROM ${CLICKHOUSE_DATABASE}.bfe_ai_request_log\""
    echo "  ${CLICKHOUSE_CMD} -q \"SELECT count() FROM ${CLICKHOUSE_DATABASE}.bfe_ai_metrics_1m\""
    echo ""
}

main
