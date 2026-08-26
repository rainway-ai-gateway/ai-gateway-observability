# BFE AI Gateway 可观测性（ai-gateway-observability）

本项目提供 BFE AI Gateway 的可观测性配置：**Doris 存储层**（明细表 + 聚合表 + Kafka 接入）与 **Grafana 展示层**（数据源 + Dashboard）的一键部署脚本与设计文档。

## 数据链路

```
BFE（数据面） ──日志──▶ log-reader ──JSON──▶ Kafka ──Routine Load──▶ Doris
                                                                    ├─ bfe_ai_request_log   （明细表，原始请求日志）
                                                                    └─ bfe_ai_metrics_1m    （聚合表，INSERT JOB 每分钟聚合）
                                                                              │
                                                                              ▼
                                                                        Grafana（MySQL 数据源直连 Doris）
```

| 环节 | 说明 |
|------|------|
| log-reader → Kafka | 将 BFE 日志字段输出为 JSON |
| Kafka → Doris | Routine Load `bfe_ai_log_load` 实时写入明细表 |
| 明细 → 聚合 | INSERT JOB `bfe_ai_metrics_1m_job` 每分钟把上一分钟明细聚合到分钟表 |
| Doris → Grafana | Grafana 以 MySQL 协议直连 Doris FE（9030） |

## 目录结构

```
ai-gateway-observability/
├── README.md                          # 本文档
├── doris/
│   ├── setup.sh                       # 一键部署（建库/表/Routine Load/INSERT JOB）
│   ├── cleanup.sh                     # 清空脚本（删表/Routine Load/Job）
│   ├── setup.conf                     # 生产配置（库 bfe_observability）
│   ├── setup_test.conf                # 测试配置（库 bfe_observability_test）
│   ├── sqls/                          # DDL/DML SQL
│   │   ├── bfe_observability.sql      #   建库
│   │   ├── bfe_ai_request_log.sql     #   明细表
│   │   ├── bfe_ai_metrics_1m.sql      #   聚合表
│   │   ├── bfe_ai_log_load_routine.sql#   Routine Load
│   │   └── bfe_ai_metrics_1m_job.sql  #   INSERT JOB
│   ├── demo/                          # 演示 JSON 样例
│   └── docs/                          # 文档
│       ├── user/HOWTO.md              #   Doris 部署指南
│       ├── design/TABLE_DESIGN.md     #   表设计说明
│       └── modification/              #   变更记录
│           └── 20260826update2newPb.md#   升级到新 PB 字段变更
├── grafana/
│   ├── setup.sh                       # 一键配置（数据源 + Dashboard + 重启）
│   ├── setup.conf                     # 生产配置
│   ├── setup_test.conf                # 测试配置
│   ├── datasources/
│   │   └── doris.yaml                 # Doris 数据源模板
│   ├── dashboards/
│   │   └── bfe-ai-gateway-observability.json  # Dashboard 配置
│   └── docs/                          # 文档
│       ├── user/HOWTO.md              #   Grafana 配置指南
│       ├── design/DASHBOARD_DESIGN.md #   Dashboard 设计文档
│       └── modification/              #   变更记录
│           └── 20260826update2newPb.md#   升级到新 PB 字段变更
└── LICENSE
```

## 快速开始

### 1. 部署 Doris 侧（建库 / 建表 / Routine Load / INSERT JOB）

```bash
cd doris

# 生产环境（数据库 bfe_observability）
vim setup.conf
bash setup.sh

# 测试环境（数据库 bfe_observability_test，Kafka Topic 用 _test 后缀）
bash setup.sh ./setup_test.conf
```

清空（删表 / Routine Load / Job）：

```bash
bash cleanup.sh ./setup_test.conf    # 或 bash cleanup.sh
```

详细步骤见 [doris/docs/user/HOWTO.md](./doris/docs/user/HOWTO.md)，表结构与字段语义见 [doris/docs/design/TABLE_DESIGN.md](./doris/docs/design/TABLE_DESIGN.md)。

### 2. 部署 Grafana 侧（数据源 + Dashboard）

```bash
cd grafana

# 生产环境
vim setup.conf
bash setup.sh

# 测试环境
bash setup.sh ./setup_test.conf
```

脚本会写入 Doris 数据源、导入 Dashboard 并重启 Grafana。详细步骤见 [grafana/docs/user/HOWTO.md](./grafana/docs/user/HOWTO.md)，Dashboard 面板与 SQL 见 [grafana/docs/design/DASHBOARD_DESIGN.md](./grafana/docs/design/DASHBOARD_DESIGN.md)。

## 核心设计

- **两张表**：明细表 `bfe_ai_request_log`（UNIQUE KEY，单请求粒度）与聚合表 `bfe_ai_metrics_1m`（AGGREGATE KEY，1 分钟粒度，SUM 指标）。
- **参数化**：数据库名、Kafka 地址、Grafana 安装目录等均通过 `setup.conf` / `setup_test.conf` 配置，无需改动 SQL 或 Dashboard。

## 各组件配置

| 组件 | 说明 | 指南 |
|------|------|------|
| Doris | 建库、明细表、聚合表、Routine Load、INSERT JOB | [doris/docs/user/HOWTO.md](./doris/docs/user/HOWTO.md) |
| Grafana | Doris 数据源、BFE AI Gateway Dashboard | [grafana/docs/user/HOWTO.md](./grafana/docs/user/HOWTO.md) |

## 文档索引

| 文档 | 内容 |
|------|------|
| [doris/docs/user/HOWTO.md](./doris/docs/user/HOWTO.md) | Doris 部署与验证步骤 |
| [doris/docs/design/TABLE_DESIGN.md](./doris/docs/design/TABLE_DESIGN.md) | 两张表的结构、字段语义、Grafana 查询指南 |
| [doris/docs/modification/20260826update2newPb.md](./doris/docs/modification/20260826update2newPb.md) | 升级到新 PB 的字段变更记录 |
| [grafana/docs/user/HOWTO.md](./grafana/docs/user/HOWTO.md) | Grafana 数据源 + Dashboard 配置步骤 |
| [grafana/docs/design/DASHBOARD_DESIGN.md](./grafana/docs/design/DASHBOARD_DESIGN.md) | Dashboard 面板布局与 SQL |
| [grafana/docs/modification/20260826update2newPb.md](./grafana/docs/modification/20260826update2newPb.md) | 升级到新 PB 的 Dashboard 变更记录 |

## License

详见 [LICENSE](./LICENSE)。
