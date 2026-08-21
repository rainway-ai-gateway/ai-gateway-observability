# BFE Observability — Doris 端搭建指南

本文档指导你在已搭建好的 Doris + Kafka 集群上，使用 `setup.sh` 脚本一键创建 `bfe_observability` 数据库及其所有对象（表、Routine Load、INSERT JOB）。

## 1. 前提条件

| 组件 | 版本 | 说明 |
|------|------|------|
| Doris | 4.0+ | FE 节点已启动，query_port 可访问（默认 9030） |
| Kafka | 2.8+ | Broker 可访问，`bfe_ai_log` Topic 已创建 |
| mysql-client | 任意 | 用于连接 Doris FE 执行 SQL |

### 1.1. 创建 Kafka Topic（如尚未创建）

```bash
# 小规模示例（日均 100 万请求），根据实际规模调整分区数
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.18.1.244:9092 \
  --create \
  --topic bfe_ai_log \
  --partitions 2 --replication-factor 2 \
  --config retention.ms=604800000 \
  --config retention.bytes=10737418240 \
  --config compression.type=zstd
```

## 2. 文件结构

```
doris/
├── setup.sh                          # 一键部署脚本
├── setup.conf                        # 配置文件（按需修改）
├── HOWTO.md                          # 本文档
├── sqls/                             # DDL/DML SQL 文件（由脚本自动执行）
│   ├── bfe_observability.sql         # 创建数据库
│   ├── bfe_ai_request_log.sql        # 明细表
│   ├── bfe_ai_metrics_1m.sql         # 聚合表
│   ├── bfe_ai_log_load_routine.sql   # Routine Load（Kafka → Doris）
│   └── bfe_ai_metrics_1m_job.sql     # INSERT JOB（明细表 → 聚合表）
└── demo/                             # 演示用 JSON 消息样例
    ├── normal_request.json           # 正常 AI 请求
    ├── rate_limit.json               # 限流命中请求
    └── auth_reject.json              # 认证拒绝请求
```

## 3. 配置

编辑 `setup.conf`，填入你的实际环境信息：

```bash
# Doris FE 连接信息
DORIS_HOST=127.0.0.1          # Doris FE 地址
DORIS_PORT=9030               # Doris FE query_port
DORIS_USER=root               # Doris 用户名
DORIS_PASSWORD=               # Doris 密码（无密码留空）

# Kafka 连接信息
KAFKA_BROKER_LIST=172.18.1.244:9092   # Kafka Broker 地址
KAFKA_TOPIC=bfe_ai_log                # LogReader 推送的 Topic
KAFKA_GROUP_ID=doris_bfe_ai_log       # Routine Load consumer group
KAFKA_CLIENT_ID=doris_bfe_ai_log      # Routine Load client id

# 分区初始值（动态分区会自动管理后续分区）
INIT_PARTITION_DATE=2026-08-20
```

## 4. 执行部署

```bash
cd /path/to/ai-gateway-observability/doris
bash setup.sh
```

脚本会按顺序执行：

| 步骤 | 操作 | SQL 文件 |
|:---:|------|----------|
| 1 | 创建数据库 `bfe_observability` | `bfe_observability.sql` |
| 2 | 创建明细表 `bfe_ai_request_log` | `bfe_ai_request_log.sql` |
| 3 | 创建聚合表 `bfe_ai_metrics_1m` | `bfe_ai_metrics_1m.sql` |
| 4 | 创建 Routine Load（Kafka 消费） | `bfe_ai_log_load_routine.sql` |
| 5 | 创建 INSERT JOB（定时聚合） | `bfe_ai_metrics_1m_job.sql` |

> 脚本会将 `setup.conf` 中的 Kafka 连接信息自动注入到 Routine Load 的 SQL 中。

## 5. 验证

部署完成后，执行以下命令验证各组件状态：

```bash
# 连接 Doris
mysql -h127.0.0.1 -P9030 -uroot

# 查看所有表
USE bfe_observability;
SHOW TABLES;

# 查看 Routine Load 状态
SHOW ROUTINE LOAD FOR bfe_ai_log_load\G

# 查看 INSERT JOB 状态
SHOW JOBS FROM bfe_observability;
```

### 5.1. 端到端验证清单

| 序号 | 验证项 | 方法 |
|:---:|--------|------|
| 1 | LogReader 正常发送 | 检查 LogReader 日志，确认无 Kafka 发送错误 |
| 2 | Kafka 收到消息 | `kafka-console-consumer.sh --bootstrap-server <broker> --topic bfe_ai_log --max-messages 1` |
| 3 | Routine Load 运行中 | `SHOW ROUTINE LOAD FOR bfe_ai_log_load\G`，State 应为 `RUNNING` |
| 4 | 明细表有数据 | `SELECT COUNT(*) FROM bfe_ai_request_log WHERE log_time >= NOW() - INTERVAL 5 MINUTE` |
| 5 | INSERT JOB 运行中 | `SHOW JOBS FROM bfe_observability` |
| 6 | 聚合表有数据 | `SELECT COUNT(*) FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 5 MINUTE` |

## 6. 演示数据

### 6.1. 使用 Kafka 控制台生产者发送测试数据

在 LogReader 还未配置上线时，可以用以下命令手动向 Kafka 发送测试消息，验证 Routine Load 和数据链路：

```bash
# 发送正常 AI 请求（需先安装 kcat / kafkacat）
kcat -P -b 172.18.1.244:9092 -t bfe_ai_log < demo/normal_request.json

# 或使用 kafka-console-producer
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 172.18.1.244:9092 \
  --topic bfe_ai_log < demo/normal_request.json
```

发送后等待 1~2 分钟，查询明细表确认数据已入库：

```sql
SELECT logid, log_time, ai_apikey, ai_mapped_model, ai_total_tokens
FROM bfe_ai_request_log
WHERE log_time >= NOW() - INTERVAL 5 MINUTE
ORDER BY log_time DESC
LIMIT 10;
```

### 6.2. 正常 AI 请求

参考  [正常 AI 请求示例](./demo/normal_request.json)


### 6.3. 限流命中请求

参考  [限流命中请求](./demo/rate_limit.json)


### 6.4. 认证拒绝请求

参考  [认证拒绝请求](./demo/auth_reject.json)


> **零值字段**：Kafka 消息中零值字段不会被 LogReader 输出（Go `omitempty`），Doris 中对应列自动填充 NULL。

## 7. 数据流延迟

| 环节 | 延迟 | 说明 |
|------|------|------|
| BFE → PB 日志文件 | < 1s | 请求结束时写入 |
| LogReader tail → Kafka | < 1s | 近实时 tail |
| Kafka → Doris Routine Load | 1~5s | 批量提交间隔 |
| 明细表 → 聚合表 | ≤ 60s | INSERT JOB 每分钟执行 |
| **端到端总延迟** | **< 1 分钟** | 满足分钟级看板需求 |

## 8. 下一步：Grafana 集成

Doris 对象创建完成后，在 Grafana 中添加 **MySQL 数据源**连接 Doris FE（端口 9030），即可构建 Dashboard 看板。详细的 Grafana 面板 SQL 和告警配置见 [Grafna打通指南](./../grafana/../README.md)。

## 9. 重要提示：聚合表设计

本示例中的聚合表 `bfe_ai_metrics_1m` 包含 35 个维度列，**仅用于演示链路打通**。在实际生产环境中，维度过大和 1 分钟聚合粒度在 LLM 场景下往往不合理。建议：

- 按查询场景拆分为多个聚合表（核心流量、错误码、限流、认证拒绝等）
- 聚合粒度调整为 5 分钟或 15 分钟
- 稀疏维度（err_code、rate_limit_*）独立成表

