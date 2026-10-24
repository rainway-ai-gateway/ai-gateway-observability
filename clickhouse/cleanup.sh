#!/usr/bin/env bash
# ============================================================
# BFE Observability - ClickHouse Cleanup Script
# ============================================================
# 清空指定数据库下的聚合 MV、聚合表、消费 MV、明细表、
# Kafka 引擎暂存表及数据库本身。
# 数据库名由配置文件中的 CLICKHOUSE_DATABASE 指定。
#
# 用法:
#   bash cleanup.sh                    # 使用默认 setup.conf
#   bash cleanup.sh setup_test.conf    # 使用测试配置（数据库 bfe_observability_test）
#   bash cleanup.sh -y my.conf         # 跳过确认，直接清理
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- 解析参数 ---
SKIP_CONFIRM=false
CONFIG_FILE="${SCRIPT_DIR}/setup.conf"
for arg in "$@"; do
    case "${arg}" in
        -y|--yes) SKIP_CONFIRM=true ;;
        -*) log_error "未知参数: ${arg}"; exit 1 ;;
        *) CONFIG_FILE="${arg}" ;;
    esac
done

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

# 对象名（与 setup.sh / SQL 文件保持一致，可用同名配置项覆盖）
METRICS_MV_NAME="${METRICS_MV_NAME:-bfe_ai_metrics_1m_mv}"
METRICS_TABLE="${METRICS_TABLE:-bfe_ai_metrics_1m}"
LOAD_MV_NAME="${LOAD_MV_NAME:-bfe_ai_log_load_mv}"
DETAIL_TABLE="${DETAIL_TABLE:-bfe_ai_request_log}"
KAFKA_TABLE="${KAFKA_TABLE:-bfe_ai_log_kafka}"

# 构建 clickhouse-client 命令
CLICKHOUSE_CMD="clickhouse-client -h${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT} -u${CLICKHOUSE_USER} --multiquery"
if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
    CLICKHOUSE_CMD="${CLICKHOUSE_CMD} --password=${CLICKHOUSE_PASSWORD}"
fi

# --- 检查前置条件 ---
check_prerequisites() {
    if ! command -v clickhouse-client &>/dev/null; then
        log_error "未找到 clickhouse-client，请安装 clickhouse-client"
        exit 1
    fi

    if ! ${CLICKHOUSE_CMD} -q "SELECT 1" &>/dev/null; then
        log_error "无法连接 ClickHouse (${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT})，请检查配置"
        exit 1
    fi
    log_info "ClickHouse 连接成功 (${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT})"
}

# --- 执行 SQL（可忽略"对象不存在"错误） ---
# 参数: 描述 SQL
run_sql() {
    local desc="$1"
    local sql="$2"
    local out
    local rc=0

    out=$(${CLICKHOUSE_CMD} -q "${sql}" 2>&1) || rc=$?

    if [[ ${rc} -eq 0 ]]; then
        log_info "${desc} — 完成"
        return 0
    fi

    # 对象不存在视为成功（幂等）
    if echo "${out}" | grep -qiE "doesn't exist|not exist|unknown database|unknown table"; then
        log_warn "${desc} — 不存在，跳过"
        return 0
    fi

    log_error "${desc} — 失败"
    echo "${out}"
    exit 1
}

# --- 打印配置摘要 ---
print_config() {
    echo ""
    echo "============================================"
    echo "  BFE Observability ClickHouse Cleanup"
    echo "============================================"
    echo "  ClickHouse: ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
    echo "  Database:   ${CLICKHOUSE_DATABASE}"
    echo "  MVs:        ${METRICS_MV_NAME}, ${LOAD_MV_NAME}"
    echo "  Tables:     ${METRICS_TABLE}, ${DETAIL_TABLE}, ${KAFKA_TABLE}"
    echo "============================================"
    echo ""
}

# --- 确认 ---
confirm() {
    if [[ "${SKIP_CONFIRM}" == true ]]; then
        return 0
    fi
    echo -e "${RED}⚠ 即将清空数据库 ${CLICKHOUSE_DATABASE} 下的对象（MV/表/数据库）${NC}"
    read -r -p "确认继续? [y/N] " ans
    case "${ans}" in
        y|Y|yes|YES) return 0 ;;
        *) log_warn "已取消"; exit 0 ;;
    esac
}

# --- 主流程 ---
# 删除顺序：MV 先于其目标表（物化视图依赖目标表存在）
main() {
    print_config
    confirm
    check_prerequisites

    # Step 1: 删除聚合 MV
    log_step "删除聚合 MV ${METRICS_MV_NAME}"
    run_sql "删除聚合 MV" \
        "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${METRICS_MV_NAME};"

    # Step 2: 删除聚合表
    log_step "删除聚合表 ${METRICS_TABLE}"
    run_sql "删除聚合表" \
        "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${METRICS_TABLE};"

    # Step 3: 删除消费 MV
    log_step "删除消费 MV ${LOAD_MV_NAME}"
    run_sql "删除消费 MV" \
        "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${LOAD_MV_NAME};"

    # Step 4: 删除明细表
    log_step "删除明细表 ${DETAIL_TABLE}"
    run_sql "删除明细表" \
        "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${DETAIL_TABLE};"

    # Step 5: 删除 Kafka 引擎暂存表
    log_step "删除 Kafka 引擎暂存表 ${KAFKA_TABLE}"
    run_sql "删除 Kafka 引擎暂存表" \
        "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${KAFKA_TABLE};"

    # Step 6: 删除数据库
    log_step "删除数据库 ${CLICKHOUSE_DATABASE}"
    run_sql "删除数据库" \
        "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE};"

    echo ""
    log_info "============================================"
    log_info "  清理完成！"
    log_info "============================================"
    echo ""
}

main
