# BFE Observability — Grafana 配置指南

本文档指导你使用 `setup.sh` 脚本，在已安装的 Grafana 上自动配置 Doris 数据源并导入 BFE AI Gateway 可观测 Dashboard。

## 1. 概述

脚本 `setup.sh` 基于 Grafana 的 **provisioning（文件式配置）** 机制，自动完成三件事：

1. 生成 Doris 数据源配置（`conf/provisioning/datasources/doris.yaml`）
2. 生成 Dashboard 供给配置并导入 Dashboard（`conf/provisioning/dashboards/`）
3. 重启 Grafana 服务使配置生效

## 2. 前置条件

| 组件 | 要求 |
|------|------|
| Grafana | 已安装并了解其安装目录（本示例为 v11.5.1） |
| Doris | 已部署，FE 的 MySQL 协议端口可访问（默认 9030），目标库已建好 |
| bash | 任意 Linux/macOS 环境 |

> Grafana 目录需要是「含 `bin/` 与 `conf/provisioning/` 的安装根目录」。

## 3. 文件结构

```
grafana/
├── setup.sh                          # 一键配置脚本
├── setup.conf                        # 配置文件（按需修改）
├── setup_test.conf                   # 测试配置文件
├── datasources/
│   └── doris.yaml                    # 数据源模板（供参考）
├── dashboards/
│   └── bfe-ai-gateway-observability.json  # Dashboard JSON（供参考，脚本会拷贝）
└── docs/
    ├── user/
    │   └── HOWTO.md                  # 本文档（Grafana 配置指南）
    ├── design/
    │   └── DASHBOARD_DESIGN.md       # Dashboard 设计文档
    └── modification/
        └── 20260826update2newPb.md   # 升级到新 PB 的变更记录
```

## 4. 配置

编辑 `setup.conf`：

```bash
# Grafana 安装目录（必须改为你的实际路径）
GRAFANA_DIR=/home/yeyunxi/yingfei/opensouce/work/grafana-v11.5.1

# Doris 数据源连接
DORIS_HOST=172.18.1.244
DORIS_PORT=9030
DORIS_DATABASE=bfe_observability
DORIS_USER=root
DORIS_PASSWORD=

# 数据源名称与 UID（面板 ${DS_DORIS} 占位符会替换为下面的 UID）
DATASOURCE_NAME=Doris
DATASOURCE_UID=doris
```

| 参数 | 说明 |
|------|------|
| `GRAFANA_DIR` | Grafana 安装根目录（`bin/`、`conf/provisioning/` 的上级） |
| `DORIS_HOST` / `DORIS_PORT` | Doris FE 地址与 MySQL 协议端口 |
| `DORIS_DATABASE` | 目标库，例如 `bfe_observability`（生产）或 `bfe_observability_test`（测试） |
| `DORIS_USER` / `DORIS_PASSWORD` | Doris 连接账号（无密码留空） |
| `DATASOURCE_NAME` | Grafana 中数据源显示名称，需与 Dashboard 面板里按名称引用的 `Doris` 一致 |
| `DATASOURCE_UID` | 数据源 UID；脚本会把 Dashboard 中的 `${DS_DORIS}` 占位符替换成它 |

## 5. 执行

```bash
cd /home/yeyunxi/yingfei/opensouce/ai-gateway-observability/grafana

# 使用默认 setup.conf
bash setup.sh ./setup.conf

# 使用测试配置（例如连接测试库）
bash setup.sh ./setup_test.conf
```

脚本按顺序执行：

| 步骤 | 操作 | 输出 |
|:---:|------|------|
| 1 | 检查前置条件（Grafana 目录、provisioning 目录、Dashboard 源文件） | — |
| 2 | 生成数据源配置 | `conf/provisioning/datasources/doris.yaml` |
| 3 | 生成 Dashboard 供给配置 + 拷贝 Dashboard | `conf/provisioning/dashboards/default.yaml`、`.../dashboards/admin/*.json` |
| 4 | 重启 Grafana | `pkill` + `nohup ./grafana server web` |

## 6. 验证

### 6.1. 通过 UI 验证

浏览器访问 `http://<grafana-host>:3000`（默认账号 `admin/admin`）：

1. **数据源**：`Connections → Data sources`，应能看到名为 `Doris`、类型为 MySQL 的数据源，`uid` 为 `doris`。
2. **Dashboard**：`Dashboards`，应能看到「BFE AI Gateway 可观测仪表盘」。

### 6.2. 通过 API 验证

```bash
# 查看数据源
curl -s -u admin:admin http://localhost:3000/api/datasources

# 查看已导入的 Dashboard
curl -s -u admin:admin "http://localhost:3000/api/search?type=dash-db"
```

### 6.3. 日志验证

```bash
grep -iE "provisioning" <GRAFANA_DIR>/data/log/grafana.log
```

正常应看到 `inserting datasource ... name=Doris uid=doris` 和 `finished to provision dashboards`，且无 `level=error`。

## 7. 关键实现说明

1. **`${DS_DORIS}` UID 替换**：Dashboard JSON 中面板混用了「数据源名 `Doris`」和「UID `${DS_DORIS}`」两种引用。Grafana 的文件式 provisioning **不会**像 UI 导入那样自动解析 `${DS_DORIS}`，因此脚本通过 `sed` 把 `${DS_DORIS}` 替换为 `DATASOURCE_UID`，并给数据源固定同一个 `uid`，保证两种引用都能正确关联。

2. **重启方式**：脚本用 `pkill -f "grafana server"` 停止旧进程，再用 `nohup ./grafana server web &` 从 `bin/` 目录后台启动。若你的 Grafana 由 systemd 管理，可自行把「重启」步骤替换为 `systemctl restart grafana-server`。

3. **幂等性**：脚本每次都会覆盖目标配置文件，重复执行是安全的（重新配置并重启）。

## 8. 常见问题

| 现象 | 原因 / 处理 |
|------|-------------|
| `Grafana 目录不存在` | `GRAFANA_DIR` 配置错误，确认是含 `bin/`、`conf/provisioning/` 的根目录 |
| `Dashboard JSON 不存在` | `dashboards/bfe-ai-gateway-observability.json` 源文件缺失 |
| Grafana 启动失败 | 手动执行 `<GRAFANA_DIR>/bin/grafana server web` 查看报错；检查端口 3000 是否被占用 |
| 面板无数据 | 检查 Doris 连接（`DORIS_*` 参数）与目标库是否已建表并接入数据 |
| 数据源默认被置为 `true` | 该库唯一数据源时 Grafana 会自动置默认，属正常行为 |

## 9. 文档更新历史

| 日期 | 版本 | 变更说明 |
|------|------|----------|
| 2026-08-25 | v1.0 | 初始版本：Grafana 数据源 + Dashboard 一键配置脚本及说明 |
