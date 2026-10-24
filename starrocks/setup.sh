#!/usr/bin/env bash
# ============================================================
# BFE Observability - StarRocks Setup Script
# ============================================================
# 在已有的 StarRocks + Kafka 集群上创建可观测性数据库、
# 明细表、分钟聚合物化视图（异步 MV）和 Routine Load。
# 数据库名由配置文件中的 STARROCKS_DATABASE 指定。
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
STARROCKS_HOST="${STARROCKS_HOST:-127.0.0.1}"
STARROCKS_PORT="${STARROCKS_PORT:-9030}"
STARROCKS_USER="${STARROCKS_USER:-root}"
STARROCKS_PASSWORD="${STARROCKS_PASSWORD:-}"
STARROCKS_DATABASE="${STARROCKS_DATABASE:-bfe_observability}"
KAFKA_BROKER_LIST="${KAFKA_BROKER_LIST:-172.18.1.244:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-bfe_ai_log}"
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-starrocks_bfe_ai_log}"
KAFKA_CLIENT_ID="${KAFKA_CLIENT_ID:-starrocks_bfe_ai_log}"
INIT_PARTITION_DATE="${INIT_PARTITION_DATE:-2026-10-02}"

# 构建 mysql 命令（StarRocks FE 的 MySQL 协议端口）
# 注意：--skip-comments 必须保留——StarRocks FE 解析器拒绝"仅含注释"的语句，
# 而 mysql 客户端默认把注释行作为独立语句发送，会导致 1064 语法错误。
if [[ -n "${STARROCKS_PASSWORD}" ]]; then
    MYSQL_CMD="mysql --skip-comments -h${STARROCKS_HOST} -P${STARROCKS_PORT} -u${STARROCKS_USER} -p${STARROCKS_PASSWORD}"
else
    MYSQL_CMD="mysql --skip-comments -h${STARROCKS_HOST} -P${STARROCKS_PORT} -u${STARROCKS_USER}"
fi

# --- 检查前置条件 ---
check_prerequisites() {
    log_step "检查前置条件"

    if ! command -v mysql &>/dev/null; then
        log_error "未找到 mysql 客户端，请安装 mysql-client"
        exit 1
    fi
    log_info "mysql 客户端已就绪"

    if ! ${MYSQL_CMD} -e "SELECT 1" &>/dev/null; then
        log_error "无法连接 StarRocks FE (${STARROCKS_HOST}:${STARROCKS_PORT})，请检查配置"
        exit 1
    fi
    log_info "StarRocks 连接成功 (${STARROCKS_HOST}:${STARROCKS_PORT})"
}

# --- 执行 SQL 文件 ---
# SQL 文件使用 ${VAR} 占位符引用 setup.conf 中的变量，此处替换后执行
run_sql_file() {
    local desc="$1"
    local sql_file="$2"

    log_step "${desc}"

    if [[ ! -f "${sql_file}" ]]; then
        log_error "SQL 文件不存在: ${sql_file}"
        exit 1
    fi

    if ! sed \
        -e 's|\${STARROCKS_DATABASE}|'"${STARROCKS_DATABASE}"'|g' \
        -e 's|\${INIT_PARTITION_DATE}|'"${INIT_PARTITION_DATE}"'|g' \
        -e 's|\${KAFKA_BROKER_LIST}|'"${KAFKA_BROKER_LIST}"'|g' \
        -e 's|\${KAFKA_TOPIC}|'"${KAFKA_TOPIC}"'|g' \
        -e 's|\${KAFKA_GROUP_ID}|'"${KAFKA_GROUP_ID}"'|g' \
        -e 's|\${KAFKA_CLIENT_ID}|'"${KAFKA_CLIENT_ID}"'|g' \
        "${sql_file}" | ${MYSQL_CMD}; then
        log_error "${desc} — 失败"
        exit 1
    fi
    log_info "${desc} — 完成"
}

# --- 打印配置摘要 ---
print_config() {
    echo ""
    echo "============================================"
    echo "  BFE Observability StarRocks Setup"
    echo "============================================"
    echo "  StarRocks FE: ${STARROCKS_HOST}:${STARROCKS_PORT}"
    echo "  Database:     ${STARROCKS_DATABASE}"
    echo "  Kafka:        ${KAFKA_BROKER_LIST}"
    echo "  Topic:        ${KAFKA_TOPIC}"
    echo "  Group ID:     ${KAFKA_GROUP_ID}"
    echo "  Init Part:    ${INIT_PARTITION_DATE}"
    echo "============================================"
    echo ""
}

# --- 主流程 ---
main() {
    print_config

    check_prerequisites

    # Step 1: 创建数据库
    run_sql_file "创建数据库 ${STARROCKS_DATABASE}" "${SQL_DIR}/bfe_observability.sql"

    # Step 2: 创建明细表
    run_sql_file "创建明细表 bfe_ai_request_log" "${SQL_DIR}/bfe_ai_request_log.sql"

    # Step 3: 创建分钟聚合物化视图（异步 MV，名字即 bfe_ai_metrics_1m）
    run_sql_file "创建物化视图 bfe_ai_metrics_1m" "${SQL_DIR}/bfe_ai_metrics_1m.sql"

    # Step 4: 创建 Routine Load（需要替换 Kafka 参数）
    run_sql_file "创建 Routine Load bfe_ai_log_load" "${SQL_DIR}/bfe_ai_log_load_routine.sql"

    echo ""
    log_info "============================================"
    log_info "  全部创建完成！"
    log_info "============================================"
    echo ""
    echo "验证命令:"
    echo "  # 查看 Routine Load 状态"
    echo "  ${MYSQL_CMD} -D${STARROCKS_DATABASE} -e \"SHOW ROUTINE LOAD FOR bfe_ai_log_load\\G\""
    echo ""
    echo "  # 查看物化视图状态（含刷新进度与错误）"
    echo "  ${MYSQL_CMD} -D${STARROCKS_DATABASE} -e \"SHOW MATERIALIZED VIEWS\\G\""
    echo ""
    echo "  # 查看表"
    echo "  ${MYSQL_CMD} -e \"USE ${STARROCKS_DATABASE}; SHOW TABLES\""
    echo ""
}

main
