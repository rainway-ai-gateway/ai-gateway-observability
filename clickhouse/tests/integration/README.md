# ClickHouse 集成测试

本目录承载 `ai-gateway-observability/clickhouse` 的**真实 ClickHouse 集成测试**（平行于 `doris/tests/integration` 的 doris-it）：

- 被测对象是本仓库真实的 SQL 资产（`clickhouse/sqls/` 下的建库/明细表/Kafka 引擎表/消费 MV/聚合表/聚合 MV），不做任何副本或改写（仅做 `${VAR}` 变量替换，与 `setup.sh` 的 sed 行为一致）；
- 测试直连本机 ClickHouse（WSL2 单文件实例 26.10.1.1149，见 `environment/clickhouse-installation.md`，HTTP 8123 / Native TCP 9000），每个用例在独立的测试库（`<CLICKHOUSE_DATABASE>_it_*`）中创建/销毁对象；
- ClickHouse 未启动时全部用例自动 `SKIP`（不会失败），启动方式：`wsl bash /mnt/d/clickhouse/bin/start.sh`；
- TC02 走真实 Kafka 生产 → Kafka 引擎 → 消费 MV 链路，Kafka 未启动时该用例单独 `SKIP`，启动方式：`wsl bash /mnt/d/kafka/bin/start-kafka.sh`。

## 目录结构

```text
clickhouse/tests/integration/
├── README.md                                   # 本文档
├── go.mod                                      # 独立 Go module（clickhouse-it）
├── common/
│   ├── clickhouse.go                           # ClickHouse 连接、SQL 变量替换、执行/查询辅助
│   └── kafka.go                                # Kafka 生产辅助（建临时 topic、生产 demo 样例）
├── implementation/
│   └── scenario-CLICKHOUSE01-clickhouse-dock/
│       └── ch01_clickhouse_dock_test.go
└── 测试设计文档/
    └── scenario-CLICKHOUSE01-clickhouse对接/
        └── 场景说明.md
```

## 运行方式

在 `clickhouse/tests/integration/` 目录下执行：

```bash
# 运行全部集成测试
go test ./... -v

# 运行单个场景
go test ./implementation/scenario-CLICKHOUSE01-clickhouse-dock/ -v

# 运行单个测试例
go test ./implementation/scenario-CLICKHOUSE01-clickhouse-dock/ -run TestTC01 -v
```

## 连接配置

默认连接 `default@127.0.0.1:9000`（无密码，DSN 固定落在 default 库——测试对象全部建在独立测试库中，SQL 资产以 `${CLICKHOUSE_DATABASE}` 全限定，无需 USE），可用环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `CLICKHOUSE_HOST` | `127.0.0.1` | ClickHouse 地址（Native TCP） |
| `CLICKHOUSE_PORT` | `9000` | Native TCP 端口（HTTP 为 8123） |
| `CLICKHOUSE_USER` | `default` | 用户名 |
| `CLICKHOUSE_PASSWORD` | 空 | 密码（含 `@`/`:` 等特殊字符时需 URL 转义） |
| `CLICKHOUSE_DATABASE` | `bfe_observability_test` | 测试库基名：实际用例库为 `<基名>_it_<用例>_<纳秒后缀>` |
| `CLICKHOUSE_IT_KEEP_DB` | 空 | 置 `1` 时测试结束后保留测试库（调试用） |

## Kafka 配置（TC02 真实消费链路）

与 `setup.conf` 同名环境变量覆盖，默认值指向本机测试 Kafka（KRaft 单节点 4.3.1，见 `environment/kafka-installation.md`）：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `KAFKA_BROKER_LIST` | `127.0.0.1:9092` | Kafka broker 列表（逗号分隔） |
| `KAFKA_TOPIC` | `bfe_ai_log_it` | 测试消息 topic 基名：实际写入 `<基名>_<纳秒后缀>` 独立临时 topic（显式 4 分区，对齐资产 `kafka_num_consumers=3`），避免消费组位点/残留消息跨运行污染 |
| `KAFKA_GROUP_ID` | `clickhouse_bfe_ai_log_it` | Kafka 引擎表消费组 |

TC02 行为要点（详见 [TC-02 文档](./测试设计文档/scenario-CLICKHOUSE01-clickhouse对接/TC-02-消费打平.md)）：

- 实测 ClickHouse Kafka 引擎新消费组为 **earliest** 起点，生产后轮询明细表直至三条样例落库即可（deadline 240s，本机 9p 盘上消费组分配可达 1-2 分钟）；
- demo 样例为多条消息层改写后生产：`timestamp` 重写为当前时刻（demo 时间戳 2026-08-24 早于 TTL 7 天，不重写会被静默丢弃）、多行 JSON 压缩为单行（`JSONEachRow` 要求）；
- 测试 topic **只建不删**：本机 Kafka 数据目录在 /mnt/d（WSL 9p），删除 topic 会使 broker log dir failed 宕机；topic 命名唯一、仅 3 条消息；
- Kafka 客户端为 `segmentio/kafka-go v0.4.51`（v0.4.47 及更早版本的 CreateTopics API 与 Kafka 4.x 不兼容，连接会被 broker 直接断开）。

## 与 doris-it 的结构差异

| 项 | doris-it | clickhouse-it |
|----|----------|---------------|
| 驱动 | `go-sql-driver/mysql`（DSN `multiStatements=true` + `USE <db>` 前缀） | `clickhouse-go/v2` stdlib 模式（`clickhouse://user:pass@host:9000/dbname?dial_timeout=10s`，六份 SQL 均为单语句且全限定库名，无需 multiStatements/USE） |
| Kafka 消息生产 | 不生产（TC02 仅校验 Routine Load 任务创建被接受） | `segmentio/kafka-go` 真实生产三份 demo 样例到独立临时 topic（打平发生在 MV 消费路径，必须走真实消费链路；消息体做 timestamp 重写 + 单行压缩，不改 demo 文件） |
| Skip 逻辑 | Doris 不可达全部 Skip；无 Kafka 依赖 | ClickHouse 不可达全部 Skip；Kafka 不可达仅 TC02 额外 Skip |
| 聚合触发 | 手动执行 JOB 的 INSERT..SELECT（固定字面量窗口） | 聚合 MV 随明细插入同步触发（tiny sleep 保险） |

## 当前覆盖

| 场景 | 说明 |
|------|------|
| CLICKHOUSE01 ClickHouse 对接 | 基于真实 SQL 资产验证 v0.8 ClickHouse 侧落地：全新安装六步建 5 对象且幂等；真实 Kafka 消费打平（level 十列、限流三列、UTC 墙钟、UInt64 logid 原值、Nullable 语义）；明细→聚合 40 维 + 24 指标关键子集口径（GROUP BY + sum() 兜底范式）；存量升级 ALTER（加列成功、聚合表不自动携带新列的行为记录）；聚合表重建（DROP MV+表 → 按仓库 DDL 重建 → INSERT..SELECT 明细重算，口径不变） |

详细用例设计见 [测试设计文档](./测试设计文档/scenario-CLICKHOUSE01-clickhouse对接/场景说明.md)。
