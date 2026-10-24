# StarRocks 集成测试

本目录承载 `ai-gateway-observability/starrocks` 的**真实 StarRocks 集成测试**（平行于 `doris/tests/integration` 的 doris-it 与 `clickhouse/tests/integration` 的 clickhouse-it）：

- 被测对象是本仓库真实的 SQL 资产（`starrocks/sqls/` 下的建库/明细表/分钟聚合 MV/Routine Load），不做任何副本或改写（仅做 `${VAR}` 变量替换，与 `setup.sh` 的 sed 行为一致）；
- 测试直连本机 StarRocks FE 的 MySQL 协议端口（WSL2 单机 StarRocks 3.5.21，见 `starrocks-installation.md`，`127.0.0.1:9030`，root 无密码），每个用例在独立的测试库（`<STARROCKS_DATABASE>_it_*`）中创建/销毁对象；
- StarRocks 未启动时全部用例自动 `SKIP`（不会失败），启动方式：`wsl bash ~/starrocks/bin/start-all.sh`；
- TC02 走真实 Kafka 生产 → Routine Load 链路，Kafka 未启动时该用例单独 `SKIP`，启动方式：`wsl bash /mnt/d/kafka/bin/start-kafka.sh`。

## 目录结构

```text
starrocks/tests/integration/
├── README.md                                   # 本文档
├── go.mod / go.sum                             # 独立 Go module（starrocks-it）
├── common/
│   ├── starrocks.go                            # StarRocks 连接、SQL 变量替换、执行/查询辅助、MV/RL 状态辅助
│   └── kafka.go                                # Kafka 生产辅助（建临时 topic、生产 demo 样例）
├── implementation/
│   └── scenario-STARROCKS01-starrocks-dock/
│       └── sr01_starrocks_dock_test.go
└── 测试设计文档/
    └── scenario-STARROCKS01-starrocks对接/
        ├── 场景说明.md
        ├── TC-01-全新安装schema.md
        ├── TC-02-消费打平.md
        ├── TC-03-明细到聚合口径.md
        ├── TC-04-存量升级ALTER.md
        └── TC-05-聚合MV重建.md
```

## 运行方式

在 `starrocks/tests/integration/` 目录下执行：

```bash
# 运行全部集成测试
go test ./... -v

# 运行单个场景
go test ./implementation/scenario-STARROCKS01-starrocks-dock/ -v

# 运行单个测试例
go test ./implementation/scenario-STARROCKS01-starrocks-dock/ -run TestTC01 -v
```

## 连接配置

默认连接 `root@127.0.0.1:9030`（无密码，MySQL 协议），可用环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `STARROCKS_IT_HOST` | `127.0.0.1` | StarRocks FE 地址 |
| `STARROCKS_IT_PORT` | `9030` | MySQL 协议端口 |
| `STARROCKS_IT_USER` | `root` | 用户名 |
| `STARROCKS_IT_PASSWORD` | 空 | 密码 |
| `STARROCKS_IT_KEEP_DB` | 空 | 置 `1` 时测试结束后保留测试库（调试用） |

## Kafka 配置（TC02 真实消费链路）

与 `setup.conf` 同名环境变量覆盖，默认值指向本机测试 Kafka（KRaft 单节点 4.3.1，见 `kafka-installation.md`）：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `KAFKA_BROKER_LIST` | `127.0.0.1:9092` | Kafka broker 列表（逗号分隔） |
| `KAFKA_TOPIC` | `bfe_ai_log_it` | 测试消息 topic 基名：实际写入 `<基名>_<纳秒后缀>` 独立临时 topic（显式 4 分区，对齐资产 `desired_concurrent_number=3`），避免消费组位点/残留消息跨运行污染 |
| `KAFKA_GROUP_ID` | `starrocks_bfe_ai_log_it` | Routine Load 消费组 |

TC02 行为要点（详见 [TC-02 文档](./测试设计文档/scenario-STARROCKS01-starrocks对接/TC-02-消费打平.md)）：

- demo 样例为消息层改写后生产：`timestamp` 重写为当前时刻（demo 时间戳 2026-08-24 早于动态分区 `start=-7` 的既有分区，不重写会成为无分区错误行）、`logid` 重写到 Int64 域（SR 对超界 BIGINT 直接拒绝——INSERT 报错 / Routine Load 计错误行跳过，与 Doris 静默截断、CH UInt64 原值均为差异点）、多行 JSON 压缩为单行（等价真实 log-reader 产出）；
- Routine Load 创建后若因 Kafka 元数据获取失败进入 PAUSED，测试轮询期间自动 RESUME 自愈；
- 测试 topic **只建不删**：本机 Kafka 数据目录在 /mnt/d（WSL 9p），删除 topic 会使 broker log dir failed 宕机；topic 命名唯一、仅 3 条消息。

## 与 doris-it / clickhouse-it 的结构差异

| 项 | doris-it | clickhouse-it | starrocks-it |
|----|----------|---------------|--------------|
| 驱动 | `go-sql-driver/mysql` | `clickhouse-go/v2` stdlib | `go-sql-driver/mysql`（同 doris-it） |
| 聚合触发 | 手动执行 JOB 的 INSERT..SELECT（固定字面量窗口） | 聚合 MV 随插入同步触发 | **异步 MV 周期刷新（每分钟）**：轮询等待 + 跨周期不重复累计断言 |
| 重建回填 | 重建聚合表后手工 INSERT..SELECT 重算 | DROP+CREATE 后手工 INSERT..SELECT 重算 | **DROP MV+CREATE 后自动全量回填**（异步 MV 初始刷新），无需手工重算 |
| 重复应用 | 表/Job 非幂等（错误） | `IF NOT EXISTS` 幂等 | 表/MV/RL 非幂等（错误信息固化为断言）；建库幂等 |
| Kafka 消息生产 | 不生产（TC02 仅校验 Routine Load 任务创建被接受） | kafka-go 真实生产 | kafka-go 真实生产（同 clickhouse-it） |
| Skip 逻辑 | Doris 不可达全部 Skip | CH 不可达全部 Skip；Kafka 不可达仅 TC02 额外 Skip | StarRocks 不可达全部 Skip；Kafka 不可达仅 TC02 额外 Skip |

## 当前覆盖

| 场景 | 说明 |
|------|------|
| STARROCKS01 StarRocks 对接 | 基于真实 SQL 资产验证 v0.8 StarRocks 侧落地：全新安装建 3 对象且行为固化（建库幂等/表 MV RL 非幂等）；真实 Kafka 消费打平（level 十列、限流三列、UTC 墙钟、NULL 语义）；明细→聚合 40 维 + 24 指标关键子集口径 + 增量刷新不重复累计；存量升级 ALTER（SR 3.5 不支持 ADD COLUMN IF NOT EXISTS 负向断言、MV 不自动携带新列）；聚合 MV 重建（DROP+CREATE 自动回填，口径不变） |

详细用例设计见 [测试设计文档](./测试设计文档/scenario-STARROCKS01-starrocks对接/场景说明.md)。
