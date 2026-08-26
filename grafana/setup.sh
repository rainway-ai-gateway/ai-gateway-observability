#!/usr/bin/env bash
# ============================================================
# BFE Observability - Grafana Setup Script
# ============================================================
# 自动配置 Grafana：
#   1. 写入 Doris 数据源（provisioning datasource）
#   2. 写入 Dashboard provisioning 配置并导入 Dashboard
#   3. 重启 Grafana 服务
#
# 用法:
#   bash setup.sh                    # 使用默认 setup.conf
#   bash setup.sh my.conf            # 使用自定义配置
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-${SCRIPT_DIR}/setup.conf}"

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
GRAFANA_DIR="${GRAFANA_DIR:-/home/yeyunxi/yingfei/opensouce/work/grafana-v11.5.1}"
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_PORT="${DORIS_PORT:-9030}"
DORIS_DATABASE="${DORIS_DATABASE:-bfe_observability}"
DORIS_USER="${DORIS_USER:-root}"
DORIS_PASSWORD="${DORIS_PASSWORD:-}"
DATASOURCE_NAME="${DATASOURCE_NAME:-Doris}"
DATASOURCE_UID="${DATASOURCE_UID:-doris}"

# 目标目录
PROVISIONING_DIR="${GRAFANA_DIR}/conf/provisioning"
DATASOURCES_DIR="${PROVISIONING_DIR}/datasources"
DASHBOARDS_DIR="${PROVISIONING_DIR}/dashboards"
DASHBOARDS_ADMIN_DIR="${DASHBOARDS_DIR}/admin"

# 源文件
SRC_DATASOURCE="${SCRIPT_DIR}/datasources/doris.yaml"
SRC_DASHBOARD="${SCRIPT_DIR}/dashboards/bfe-ai-gateway-observability.json"

# --- 检查前置条件 ---
check_prerequisites() {
    log_step "检查前置条件"

    if [[ ! -d "${GRAFANA_DIR}" ]]; then
        log_error "Grafana 目录不存在: ${GRAFANA_DIR}"
        exit 1
    fi
    if [[ ! -d "${PROVISIONING_DIR}" ]]; then
        log_error "Grafana provisioning 目录不存在: ${PROVISIONING_DIR}"
        exit 1
    fi
    if [[ ! -f "${SRC_DATASOURCE}" ]]; then
        log_error "数据源模板不存在: ${SRC_DATASOURCE}"
        exit 1
    fi
    if [[ ! -f "${SRC_DASHBOARD}" ]]; then
        log_error "Dashboard JSON 不存在: ${SRC_DASHBOARD}"
        exit 1
    fi

    log_info "Grafana 目录: ${GRAFANA_DIR}"
    log_info "Doris: ${DORIS_HOST}:${DORIS_PORT}/${DORIS_DATABASE}"
}

# --- 渲染模板 ---
# 将模板文件中的 ${VAR} 占位符替换为 setup.conf 中的变量值。
# 注意：Dashboard 中的 ${DS_DORIS} 是 Grafana 数据源输入占位符，替换为数据源 UID。
render_template() {
    local template="$1"
    sed \
        -e 's|\${DORIS_HOST}|'"${DORIS_HOST}"'|g' \
        -e 's|\${DORIS_PORT}|'"${DORIS_PORT}"'|g' \
        -e 's|\${DORIS_DATABASE}|'"${DORIS_DATABASE}"'|g' \
        -e 's|\${DORIS_USER}|'"${DORIS_USER}"'|g' \
        -e 's|\${DORIS_PASSWORD}|'"${DORIS_PASSWORD}"'|g' \
        -e 's|\${DATASOURCE_NAME}|'"${DATASOURCE_NAME}"'|g' \
        -e 's|\${DATASOURCE_UID}|'"${DATASOURCE_UID}"'|g' \
        -e 's|\${DS_DORIS}|'"${DATASOURCE_UID}"'|g' \
        "${template}"
}

# --- 写入数据源 ---
write_datasource() {
    log_step "写入 Doris 数据源"
    mkdir -p "${DATASOURCES_DIR}"

    render_template "${SRC_DATASOURCE}" > "${DATASOURCES_DIR}/doris.yaml"
    log_info "已生成 ${DATASOURCES_DIR}/doris.yaml"
}

# --- 写入 Dashboard 配置 ---
write_dashboards() {
    log_step "配置 Dashboard"
    mkdir -p "${DASHBOARDS_ADMIN_DIR}"

    # Dashboard provider（指向 admin 目录）
    cat > "${DASHBOARDS_DIR}/default.yaml" <<EOF
apiVersion: 1
providers:
  - name: 'bfe-ai-gateway'
    orgId: 1
    folder: ''
    folderUid: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: ${DASHBOARDS_ADMIN_DIR}
      foldersFromFilesStructure: false
EOF
    log_info "已生成 ${DASHBOARDS_DIR}/default.yaml"

    # 拷贝 Dashboard JSON，并渲染其中的 ${VAR} 占位符
    render_template "${SRC_DASHBOARD}" > "${DASHBOARDS_ADMIN_DIR}/bfe-ai-gateway-observability.json"
    log_info "已拷贝 Dashboard 到 ${DASHBOARDS_ADMIN_DIR}/"
}

# --- 重启 Grafana ---
restart_grafana() {
    log_step "重启 Grafana 服务"

    if pkill -f "grafana server" 2>/dev/null; then
        log_info "已停止现有 Grafana 进程"
        sleep 3
    else
        log_warn "未发现运行中的 Grafana 进程，直接启动"
    fi

    cd "${GRAFANA_DIR}/bin"
    nohup ./grafana server web > /dev/null 2>&1 &
    sleep 3

    if pgrep -f "grafana server" > /dev/null; then
        log_info "Grafana 已启动"
    else
        log_error "Grafana 启动失败，请手动检查日志"
        exit 1
    fi
}

# --- 主流程 ---
main() {
    check_prerequisites
    write_datasource
    write_dashboards
    restart_grafana

    echo ""
    log_info "============================================"
    log_info "  Grafana 配置完成！"
    log_info "============================================"
    echo "  数据源:   ${DATASOURCE_NAME} (uid: ${DATASOURCE_UID})"
    echo "  Doris:    ${DORIS_HOST}:${DORIS_PORT}/${DORIS_DATABASE}"
    echo "  Dashboard: http://localhost:3000"
    echo ""
}

main