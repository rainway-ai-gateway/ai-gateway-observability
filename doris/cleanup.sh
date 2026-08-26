#!/usr/bin/env bash
# ============================================================
# BFE Observability - Doris Cleanup Script
# ============================================================
# 清空指定数据库下的明细表、聚合表、Routine Load 和 INSERT JOB。
# 数据库名由配置文件中的 DORIS_DATABASE 指定。
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
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_PORT="${DORIS_PORT:-9030}"
DORIS_USER="${DORIS_USER:-root}"
DORIS_PASSWORD="${DORIS_PASSWORD:-}"
DORIS_DATABASE="${DORIS_DATABASE:-bfe_observability}"

# 对象名（与 setup.sh / SQL 文件保持一致，可用同名配置项覆盖）
ROUTINE_LOAD_NAME="${ROUTINE_LOAD_NAME:-bfe_ai_log_load}"
JOB_NAME="${JOB_NAME:-bfe_ai_metrics_1m_job}"
DETAIL_TABLE="${DETAIL_TABLE:-bfe_ai_request_log}"
METRICS_TABLE="${METRICS_TABLE:-bfe_ai_metrics_1m}"

# 构建 mysql 命令
if [[ -n "${DORIS_PASSWORD}" ]]; then
    MYSQL_CMD="mysql -h${DORIS_HOST} -P${DORIS_PORT} -u${DORIS_USER} -p${DORIS_PASSWORD}"
else
    MYSQL_CMD="mysql -h${DORIS_HOST} -P${DORIS_PORT} -u${DORIS_USER}"
fi

# --- 检查前置条件 ---
check_prerequisites() {
    if ! command -v mysql &>/dev/null; then
        log_error "未找到 mysql 客户端，请安装 mysql-client"
        exit 1
    fi

    if ! ${MYSQL_CMD} -e "SELECT 1" &>/dev/null; then
        log_error "无法连接 Doris FE (${DORIS_HOST}:${DORIS_PORT})，请检查配置"
        exit 1
    fi
    log_info "Doris 连接成功 (${DORIS_HOST}:${DORIS_PORT})"
}

# --- 执行 SQL（可忽略"对象不存在"错误） ---
# 参数: 描述 SQL
run_sql() {
    local desc="$1"
    local sql="$2"
    local out
    local rc=0

    out=$(${MYSQL_CMD} -e "${sql}" 2>&1) || rc=$?

    if [[ ${rc} -eq 0 ]]; then
        log_info "${desc} — 完成"
        return 0
    fi

    # 对象不存在视为成功（幂等）
    if echo "${out}" | grep -qiE "not exist|not operable|no job|does not exist|Unknown table|Unknown database"; then
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
    echo "  BFE Observability Doris Cleanup"
    echo "============================================"
    echo "  Doris FE:   ${DORIS_HOST}:${DORIS_PORT}"
    echo "  Database:   ${DORIS_DATABASE}"
    echo "  RoutineLoad:${ROUTINE_LOAD_NAME}"
    echo "  Job:        ${JOB_NAME}"
    echo "  Tables:     ${DETAIL_TABLE}, ${METRICS_TABLE}"
    echo "============================================"
    echo ""
}

# --- 确认 ---
confirm() {
    if [[ "${SKIP_CONFIRM}" == true ]]; then
        return 0
    fi
    echo -e "${RED}⚠ 即将清空数据库 ${DORIS_DATABASE} 下的对象（表/Routine Load/Job）${NC}"
    read -r -p "确认继续? [y/N] " ans
    case "${ans}" in
        y|Y|yes|YES) return 0 ;;
        *) log_warn "已取消"; exit 0 ;;
    esac
}

# --- 主流程 ---
main() {
    print_config
    confirm
    check_prerequisites

    # Step 1: 停止并删除 Routine Load
    log_step "删除 Routine Load ${ROUTINE_LOAD_NAME}"
    run_sql "删除 Routine Load" \
        "USE ${DORIS_DATABASE}; STOP ROUTINE LOAD FOR ${ROUTINE_LOAD_NAME};"

    # Step 2: 删除 INSERT JOB（Doris JOB 名全局唯一）
    log_step "删除 INSERT JOB ${JOB_NAME}"
    run_sql "删除 INSERT JOB" \
        "DROP JOB WHERE JobName = '${JOB_NAME}';"

    # Step 3: 删除明细表
    log_step "删除明细表 ${DETAIL_TABLE}"
    run_sql "删除明细表" \
        "DROP TABLE IF EXISTS ${DORIS_DATABASE}.${DETAIL_TABLE};"

    # Step 4: 删除聚合表
    log_step "删除聚合表 ${METRICS_TABLE}"
    run_sql "删除聚合表" \
        "DROP TABLE IF EXISTS ${DORIS_DATABASE}.${METRICS_TABLE};"

    echo ""
    log_info "============================================"
    log_info "  清理完成！"
    log_info "============================================"
    echo ""
    echo "验证命令:"
    echo "  ${MYSQL_CMD} -e \"USE ${DORIS_DATABASE}; SHOW TABLES;\""
    echo ""
}

main