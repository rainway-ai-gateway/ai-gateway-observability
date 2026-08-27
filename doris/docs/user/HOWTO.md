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
├── cleanup.sh                        # 清空脚本（删表 / Routine Load / Job）
├── setup.conf                        # 配置文件（生产，数据库 bfe_observability）
├── setup_test.conf                   # 配置文件（测试，数据库 bfe_observability_test）
├── sqls/                             # DDL/DML SQL 文件（由脚本自动执行）
│   ├── bfe_observability.sql         # 创建数据库
│   ├── bfe_ai_request_log.sql        # 明细表
│   ├── bfe_ai_metrics_1m.sql         # 聚合表
│   ├── bfe_ai_log_load_routine.sql   # Routine Load（Kafka → Doris）
│   └── bfe_ai_metrics_1m_job.sql     # INSERT JOB（明细表 → 聚合表）
├── demo/                             # 演示用 JSON 消息样例
│   ├── normal_request.json           # 正常 AI 请求
│   ├── rate_limit.json               # 限流命中请求
│   └── auth_reject.json              # 认证拒绝请求
└── docs/
    ├── user/
    │   └── HOWTO.md                  # 本文档（Doris 部署指南）
    ├── design/
    │   └── TABLE_DESIGN.md           # 表设计说明
    └── modification/
        └── 20260826update2newPb.md   # 升级到新 PB 的字段变更记录
```

## 3. 配置

编辑 `setup.conf`，填入你的实际环境信息：

```bash
# Doris FE 连接信息
DORIS_HOST=127.0.0.1          # Doris FE 地址
DORIS_PORT=9030               # Doris FE query_port
DORIS_USER=root               # Doris 用户名
DORIS_PASSWORD=               # Doris 密码（无密码留空）
DORIS_DATABASE=bfe_observability   # 目标数据库名（脚本自动创建）

# Kafka 连接信息
KAFKA_BROKER_LIST=172.18.1.244:9092   # Kafka Broker 地址
KAFKA_TOPIC=bfe_ai_log                # LogReader 推送的 Topic
KAFKA_GROUP_ID=doris_bfe_ai_log       # Routine Load consumer group
KAFKA_CLIENT_ID=doris_bfe_ai_log      # Routine Load client id

# 分区初始值（动态分区会自动管理后续分区）
INIT_PARTITION_DATE=2026-08-20
```

> **数据库名可参数化**：SQL 文件中的 `bfe_observability` 会在执行时被替换为 `DORIS_DATABASE` 的值。测试环境可直接使用现成的 `setup_test.conf`（数据库 `bfe_observability_test`、Kafka Topic `bfe_ai_log_test`），无需修改 `setup.conf`。

## 4. 执行部署

```bash
cd /path/to/ai-gateway-observability/doris

# 生产环境（默认 setup.conf，数据库 bfe_observability）
bash setup.sh

# 测试环境（数据库 bfe_observability_test）
bash setup.sh ./setup_test.conf
```

脚本会按顺序执行：

| 步骤 | 操作 | SQL 文件 |
|:---:|------|----------|
| 1 | 创建数据库 `${DORIS_DATABASE}` | `bfe_observability.sql` |
| 2 | 创建明细表 `bfe_ai_request_log` | `bfe_ai_request_log.sql` |
| 3 | 创建聚合表 `bfe_ai_metrics_1m` | `bfe_ai_metrics_1m.sql` |
| 4 | 创建 Routine Load（Kafka 消费） | `bfe_ai_log_load_routine.sql` |
| 5 | 创建 INSERT JOB（定时聚合） | `bfe_ai_metrics_1m_job.sql` |

> 脚本会将配置中的 `DORIS_DATABASE` 和 Kafka 连接信息自动注入到对应 SQL 中执行。

## 5. 清空数据库对象

使用 `cleanup.sh` 删除指定数据库下的明细表、聚合表、Routine Load 和 INSERT JOB：

```bash
cd /path/to/ai-gateway-observability/doris

# 清空生产库（默认 setup.conf）
bash cleanup.sh

# 清空测试库（setup_test.conf）
bash cleanup.sh setup_test.conf

# 跳过确认直接清理
bash cleanup.sh -y setup_test.conf
```

> ⚠️ **注意**：Doris 的 INSERT JOB 名称是**全局唯一**的（不按库隔离）。`bfe_ai_metrics_1m_job` 这个名字在生产和测试之间共享，清理测试库时也会删除同名 Job。如需生产/测试完全隔离，请在各自配置中设置不同的 `JOB_NAME`（例如测试库用 `JOB_NAME=bfe_ai_metrics_1m_job_test`），并相应修改 `bfe_ai_metrics_1m_job.sql` 中的 Job 名。

## 6. 验证

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

> **v1.1 格式变更**：`ai_apikeytags` 已由数组格式改为对象格式，以 `level1`~`level5` 作为 key，解决了数组顺序不对齐的问题。Doris 表中对应拆分为 `level1Name`~`level5` 共 10 个固定列。

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
SELECT logid, log_time, ai_apikey_id, ai_target_model, ai_total_tokens
FROM bfe_ai_request_log
WHERE log_time >= NOW() - INTERVAL 5 MINUTE
ORDER BY log_time DESC
LIMIT 10;
```

### 6.2. 正常 AI 请求

参考  [正常 AI 请求示例](../../demo/normal_request.json)


### 6.3. 限流命中请求

参考  [限流命中请求](../../demo/rate_limit.json)


### 6.4. 认证拒绝请求

参考  [认证拒绝请求](../../demo/auth_reject.json)


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

Doris 对象创建完成后，在 Grafana 中添加 **MySQL 数据源**连接 Doris FE（端口 9030），即可构建 Dashboard 看板。详细的 Grafana 面板 SQL 和告警配置见 [Grafna打通指南](../../../README.md)。

## 9. 重要提示：聚合表设计

本示例中的聚合表 `bfe_ai_metrics_1m` 包含 37 个维度列，**仅用于演示链路打通**。在实际生产环境中，维度过大和 1 分钟聚合粒度在 LLM 场景下往往不合理。建议：

- 按查询场景拆分为多个聚合表（核心流量、错误码、限流、认证拒绝等）
- 聚合粒度调整为 5 分钟或 15 分钟
- 稀疏维度（err_code、rate_limit_*）独立成表

## 10. 文档更新历史

| 日期 | 版本 | 变更说明 |
|------|------|----------|
| 2026-08-25 | v1.6 | 新增 `ai_protocol`、`ai_mode`（聚合表维度列）与 `ai_audio_input_tokens`、`ai_audio_output_tokens`、`ai_image_count`（明细表 + 聚合表指标列），同步 Routine Load、INSERT JOB、demo 样例与 TABLE_DESIGN.md |
| 2026-08-24 | v1.5 | 新增 `cleanup.sh` 清空脚本：删除指定数据库下的明细表、聚合表、Routine Load 和 INSERT JOB |
| 2026-08-24 | v1.4 | 数据库名参数化：新增 `DORIS_DATABASE` 配置项，`setup.sh` 执行时将 SQL 中的 `bfe_observability` 替换为配置值；新增测试配置 `setup_test.conf`（数据库 `bfe_observability_test`） |
| 2026-08-24 | v1.3 | 明细表补齐 log-reader 中已注册但未配置输出的 18 个字段（`log_tag`、`client_network`、`req_num`、`session_id`、`referrer`、`user_agent`、`delegation`、`uid`、`cookie`、`req_headers`、`res_location`、`res_transfer_encoding`、`res_headers`、`session_offset_time`、`bfe_ip`、`sock_src_ip`、`vip`、`vip6`），并同步 Routine Load 与 demo 样例 |
| 2026-08-24 | v1.2 | 1) 新增 `ai_cache_read_tokens`、`ai_cache_write_tokens` 两个字段（明细表 + 聚合表 + Routine Load）；2) log-reader 取消 `omitempty`，零值字段也会输出，demo 样例更新为全字段格式 |
| 2026-08-21 | v1.1 | `ai_apikeytags` 由数组格式改为对象格式 (level1~level5)，明细表/聚合表中改为 `level1Name`~`level5` 固定列，支持按层级精确对齐。详见 [设计文档](../../../20260821v1.1.md) |
| 2026-08-20 | v1.0 | 初始版本：明细表 + 聚合表 + Routine Load + INSERT JOB 一键部署脚本 |

