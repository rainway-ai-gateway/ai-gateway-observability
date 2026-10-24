# BFE AI Gateway 可观测性（ai-gateway-observability）

本项目提供 BFE AI Gateway 的可观测性配置：**存储层**（明细表 + 分钟聚合 + Kafka 接入）与 **Grafana 展示层**（数据源 + Dashboard）的一键部署脚本与设计文档。

存储层提供三种后端，语义逐列对齐、可按部署环境任选其一：

| 后端 | 状态 | 目录 |
|------|------|------|
| **Doris** | 默认形态，Grafana 数据源与 Dashboard 目前仅对接该后端 | [`doris/`](./doris) |
| **ClickHouse** | 已提供完整资产；本期不含 Grafana 面板 | [`clickhouse/`](./clickhouse) |
| **StarRocks** | 已提供完整资产；本期不含 Grafana 面板 | [`starrocks/`](./starrocks) |

## 数据链路

以 Doris 为参考形态的端到端链路：

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

三种后端的实现机制对照（表结构与聚合口径逐列一致，仅承载方式不同）：

| 环节 | Doris | ClickHouse | StarRocks |
|------|-------|-----------|-----------|
| Kafka 消费 + 打平 | Routine Load（`COLUMNS` 映射，限流首个命中导入时打平） | `ENGINE = Kafka` 暂存表 `bfe_ai_log_kafka` + 消费 MV `bfe_ai_log_load_mv`（JSON 打平 + UTC 墙钟换算） | Routine Load `bfe_ai_log_load_routine`（`get_json_string` 打平） |
| 明细表引擎 | `UNIQUE KEY`（含 `logid` 去重键） | `MergeTree`，按天分区，TTL 7 天 | `DUPLICATE KEY`，嵌套列以 VARCHAR 承载 JSON 文本 |
| 分钟聚合 | INSERT JOB `bfe_ai_metrics_1m_job` | 聚合 MV `bfe_ai_metrics_1m_mv`（`toStartOfMinute` + 40 维 GROUP BY） | 异步 MV `bfe_ai_metrics_1m`（`ts_day` 分区列 + `partition_ttl`） |
| Grafana 面板 | ✅ 已提供 | ⬜ 本期不提供 | ⬜ 本期不提供 |

> ClickHouse / StarRocks 本期不交付 Grafana 数据源与 Dashboard，报表可视化统一走 ai-gateway-web 报表页与 `/open-api/v1/report/*` API；趋势与告警类查询建议优先用 `bfe_ai_metrics_1m`，需要单条明细时再用 `bfe_ai_request_log`。

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
│   │   ├── bfe_ai_metrics_1m_job.sql  #   INSERT JOB
│   │   └── upgrade/                   #   存量升级 SQL（按变更日期分目录）
│   ├── demo/                          # 演示 JSON 样例
│   ├── tests/integration/             # 集成测试（DORIS01 场景）
│   └── docs/                          # 文档
│       ├── user/HOWTO.md              #   Doris 部署指南
│       ├── design/TABLE_DESIGN.md     #   表设计说明
│       └── modifications/             #   变更记录（每项变更一个目录）
│           ├── 2026-08-26-update-to-new-pb/                          # 升级到新 PB 字段变更
│           ├── 2026-09-29-report-cache-mirror-intent-doris-alignment/ # 报表二期：对齐缓存/镜像/意图字段
│           ├── 2026-10-01-routine-load-offset-beginning/             # Routine Load 起始偏移改 OFFSET_BEGINNING
│           └── 2026-10-13-issue-1417-detail-unique-key-logid/        # 明细表去重键加入 logid
├── clickhouse/
│   ├── setup.sh                       # 一键部署（建库/明细表/Kafka 引擎表/消费 MV/聚合表/聚合 MV）
│   ├── cleanup.sh                     # 清空脚本
│   ├── setup.conf                     # 生产配置（库 bfe_observability）
│   ├── setup_test.conf                # 测试配置（库 bfe_observability_test，Topic 用 _test 后缀）
│   ├── sqls/
│   │   ├── bfe_observability.sql      #   建库
│   │   ├── bfe_ai_request_log.sql     #   明细表（MergeTree，按天分区，TTL 7 天）
│   │   ├── bfe_ai_log_kafka.sql       #   Kafka 引擎暂存表（JSONEachRow）
│   │   ├── bfe_ai_log_load_mv.sql     #   消费 MV：JSON 打平 + UTC 墙钟换算
│   │   ├── bfe_ai_metrics_1m.sql      #   聚合表（SummingMergeTree，按天分区，TTL 7 天）
│   │   └── bfe_ai_metrics_1m_mv.sql   #   聚合 MV：分钟桶 + 40 维 GROUP BY
│   ├── demo/                          # 演示 JSON 样例
│   ├── tests/integration/             # 集成测试
│   └── docs/
│       ├── user/HOWTO.md              #   ClickHouse 部署指南
│       ├── design/TABLE_DESIGN.md     #   表设计说明
│       └── modifications/             #   变更记录
├── starrocks/
│   ├── setup.sh                       # 一键部署（建库/明细表/异步 MV/Routine Load）
│   ├── cleanup.sh                     # 清空脚本
│   ├── setup.conf                     # 生产配置（库 bfe_observability）
│   ├── setup_test.conf                # 测试配置（库 bfe_observability_test，Topic 用 _test 后缀）
│   ├── sqls/
│   │   ├── bfe_observability.sql      #   建库
│   │   ├── bfe_ai_request_log.sql     #   明细表（DUPLICATE KEY）
│   │   ├── bfe_ai_metrics_1m.sql      #   分钟聚合异步 MV（ts_day 分区 + partition_ttl）
│   │   └── bfe_ai_log_load_routine.sql#   Routine Load（get_json_string 打平）
│   ├── demo/                          # 演示 JSON 样例
│   ├── tests/integration/             # 集成测试（starrocks-it，TC01~TC05）
│   └── docs/
│       ├── user/HOWTO.md              #   StarRocks 部署指南
│       ├── design/TABLE_DESIGN.md     #   表设计说明
│       └── modifications/             #   变更记录
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
│           ├── 20260826update2newPb.md#   升级到新 PB 字段变更
│           ├── 20260828hideTemplateVars.md   # 隐藏模板变量
│           ├── 20260828providerBarchart.md   # 按提供商请求量改横向条形图
│           └── 20260829pieReduceAllValues.md # 饼图合并 All 取值
├── api/                               # 依赖 API 契约
│   └── depends_api/
├── skills/                            # 生成请求日志 API 的 skill
│   └── gen-req-log-api/
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

### 2. 部署 ClickHouse 侧（建库 / 明细表 / Kafka 引擎表 / MV）

```bash
cd clickhouse

# 生产环境
vim setup.conf
bash setup.sh

# 测试环境
bash setup.sh ./setup_test.conf
```

清空：`bash cleanup.sh ./setup_test.conf`（或 `bash cleanup.sh`）。详细步骤见 [clickhouse/docs/user/HOWTO.md](./clickhouse/docs/user/HOWTO.md)，表结构见 [clickhouse/docs/design/TABLE_DESIGN.md](./clickhouse/docs/design/TABLE_DESIGN.md)。

### 3. 部署 StarRocks 侧（建库 / 明细表 / 异步 MV / Routine Load）

要求 StarRocks 3.5+（FE MySQL 协议端口默认 9030）与 Kafka 2.8+。

```bash
cd starrocks

# 生产环境
vim setup.conf
bash setup.sh

# 测试环境
bash setup.sh ./setup_test.conf
```

清空：`bash cleanup.sh ./setup_test.conf`（或 `bash cleanup.sh`）。详细步骤见 [starrocks/docs/user/HOWTO.md](./starrocks/docs/user/HOWTO.md)，表结构见 [starrocks/docs/design/TABLE_DESIGN.md](./starrocks/docs/design/TABLE_DESIGN.md)。

### 4. 部署 Grafana 侧（数据源 + Dashboard）

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

- **两张表（三种后端同名同义）**：明细表 `bfe_ai_request_log`（单请求粒度，去重键含 `logid`，避免同秒并发请求被整行覆盖）与聚合表 `bfe_ai_metrics_1m`（1 分钟粒度，40 个 KEY 维度 + SUM 指标）。
- **按后端选择承载机制**：Doris 用 Routine Load + INSERT JOB；ClickHouse 用 Kafka 引擎表 + 消费 MV + 聚合 MV；StarRocks 用 Routine Load + 异步 MV。三种实现导出的列与聚合口径逐列对齐，切换后端不改变报表 SQL 语义。
- **参数化**：数据库名、Kafka 地址、各库连接信息、Grafana 安装目录等均通过各自的 `setup.conf` / `setup_test.conf` 配置，无需改动 SQL 或 Dashboard。
- **可回滚**：每个后端目录互相独立，删除目录 + 执行对应 `cleanup.sh` 即可完全回退，不影响其它后端与 Grafana 资产。

## 各组件配置

| 组件 | 说明 | 指南 |
|------|------|------|
| Doris | 建库、明细表、聚合表、Routine Load、INSERT JOB | [doris/docs/user/HOWTO.md](./doris/docs/user/HOWTO.md) |
| ClickHouse | 建库、明细表、Kafka 引擎表、消费 MV、聚合表与聚合 MV | [clickhouse/docs/user/HOWTO.md](./clickhouse/docs/user/HOWTO.md) |
| StarRocks | 建库、明细表、异步 MV、Routine Load | [starrocks/docs/user/HOWTO.md](./starrocks/docs/user/HOWTO.md) |
| Grafana | Doris 数据源、BFE AI Gateway Dashboard | [grafana/docs/user/HOWTO.md](./grafana/docs/user/HOWTO.md) |

## 文档索引

| 文档 | 内容 |
|------|------|
| [doris/docs/user/HOWTO.md](./doris/docs/user/HOWTO.md) | Doris 部署与验证步骤 |
| [doris/docs/design/TABLE_DESIGN.md](./doris/docs/design/TABLE_DESIGN.md) | 两张表的结构、字段语义、Grafana 查询指南 |
| [doris/docs/modifications/2026-08-26-update-to-new-pb/design-changes.md](./doris/docs/modifications/2026-08-26-update-to-new-pb/design-changes.md) | 升级到新 PB 的字段变更记录 |
| [doris/docs/modifications/2026-09-29-report-cache-mirror-intent-doris-alignment/design-changes.md](./doris/docs/modifications/2026-09-29-report-cache-mirror-intent-doris-alignment/design-changes.md) | 报表二期：Doris 对齐缓存/镜像/意图字段 |
| [doris/docs/modifications/2026-10-01-routine-load-offset-beginning/design-changes.md](./doris/docs/modifications/2026-10-01-routine-load-offset-beginning/design-changes.md) | Routine Load 起始偏移改为 `OFFSET_BEGINNING` |
| [doris/docs/modifications/2026-10-13-issue-1417-detail-unique-key-logid/design-changes.md](./doris/docs/modifications/2026-10-13-issue-1417-detail-unique-key-logid/design-changes.md) | 明细表去重键加入 `logid`（bfenetworks/bfe#1417） |
| [clickhouse/docs/user/HOWTO.md](./clickhouse/docs/user/HOWTO.md) | ClickHouse 部署步骤 |
| [clickhouse/docs/design/TABLE_DESIGN.md](./clickhouse/docs/design/TABLE_DESIGN.md) | ClickHouse 表结构、Kafka 引擎表与 MV 设计 |
| [starrocks/docs/user/HOWTO.md](./starrocks/docs/user/HOWTO.md) | StarRocks 部署步骤 |
| [starrocks/docs/design/TABLE_DESIGN.md](./starrocks/docs/design/TABLE_DESIGN.md) | StarRocks 表结构与异步 MV 设计 |
| [grafana/docs/user/HOWTO.md](./grafana/docs/user/HOWTO.md) | Grafana 数据源 + Dashboard 配置步骤 |
| [grafana/docs/design/DASHBOARD_DESIGN.md](./grafana/docs/design/DASHBOARD_DESIGN.md) | Dashboard 面板布局与 SQL |
| [grafana/docs/modification/20260826update2newPb.md](./grafana/docs/modification/20260826update2newPb.md) | 升级到新 PB 的 Dashboard 变更记录 |

## License

详见 [LICENSE](./LICENSE)。
