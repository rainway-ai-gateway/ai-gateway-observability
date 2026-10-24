# BFE Observability — ClickHouse 端搭建指南

本文档指导你在已搭建好的 ClickHouse + Kafka 集群上，使用 `setup.sh` 脚本一键创建 `bfe_observability` 数据库及其所有对象（明细表、Kafka 引擎表、消费 MV、聚合表、聚合 MV）。表结构与聚合口径与 Doris 版逐列对齐，详见 [TABLE_DESIGN.md](../design/TABLE_DESIGN.md)。

## 1. 数据链路概述

```
Kafka topic bfe_ai_log
    │   （Kafka 引擎表消费，独立消费组 clickhouse_bfe_ai_log）
    ▼
bfe_ai_log_kafka            Kafka 引擎暂存表（JSONEachRow，仅暂存不落盘）
    │   （消费 MV 读取并解析）
    ▼
bfe_ai_log_load_mv          消费 MV：JSON 打平（level×10、rate_limit×3）+ UTC 墙钟换算
    │   （TO 目标表写入）
    ▼
bfe_ai_request_log          明细表（MergeTree，按天分区，TTL 7 天）
    │   （聚合 MV 随明细插入同步触发）
    ▼
bfe_ai_metrics_1m_mv        聚合 MV：toStartOfMinute 分钟桶 + 40 维 GROUP BY + 24 指标
    │   （TO 目标表写入）
    ▼
bfe_ai_metrics_1m           聚合表（SummingMergeTree，按天分区，TTL 7 天）
```

与 Doris 链路的机制对照：

| 环节 | Doris 实现 | ClickHouse 实现 |
|------|-----------|-----------------|
| Kafka 消费 + 导入打平 | Routine Load（COLUMNS 映射） | `ENGINE = Kafka` 暂存表 + 消费 MV `bfe_ai_log_load_mv` |
| 分钟级聚合 | `CREATE JOB` 每分钟 INSERT SELECT | 明细表上的聚合 MV `bfe_ai_metrics_1m_mv`（随插入同步触发，秒级延迟） |
| 7 天保留 | 动态分区 `start=-7` | 两表 `TTL ... + INTERVAL 7 DAY` |
| 消费位点 | 消费组 `doris_bfe_ai_log` | 消费组 `clickhouse_bfe_ai_log`（**必须不同**，见 §3） |

## 2. 前提条件

| 组件 | 版本 | 说明 |
|------|------|------|
| ClickHouse | 26.10+ | 集群已启动，需支持 JSON 类型（开发基线 26.10）；native TCP 端口可访问（默认 9000） |
| Kafka | 2.8+ | Broker 可访问，`bfe_ai_log` Topic 已创建 |
| clickhouse-client | 与集群版本匹配 | 本机执行 SQL 用 |

### 2.1. 创建 Kafka Topic（如尚未创建）

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

## 3. 配置

编辑 `setup.conf`，填入你的实际环境信息：

```bash
# --- Kafka 连接信息 ---
KAFKA_BROKER_LIST=172.18.1.244:9092   # Kafka Broker 地址（多个用逗号分隔）
KAFKA_TOPIC=bfe_ai_log                # LogReader 推送的 Topic
KAFKA_GROUP_ID=clickhouse_bfe_ai_log  # Kafka 引擎消费组（必须与 Doris / StarRocks 链路不同）

# --- ClickHouse 连接信息 ---
CLICKHOUSE_HOST=172.18.1.244    # ClickHouse 地址
CLICKHOUSE_PORT=9000            # native TCP 端口（HTTP 8123 仅作参考，脚本不走）
CLICKHOUSE_USER=default         # ClickHouse 用户名
CLICKHOUSE_PASSWORD=            # ClickHouse 密码（无密码留空）
CLICKHOUSE_DATABASE=bfe_observability   # 目标数据库名（脚本自动创建）
```

> **消费组必须与 Doris / StarRocks 链路不同**：同一条 Kafka 消息会被每个消费组独立消费。若复用 Doris 链路的消费组（`doris_bfe_ai_log`），两边会互相覆盖位点、导致其中一条链路丢数据。三条链路（doris / starrocks / clickhouse）应各用各的消费组，位点互不影响。

> **数据库名可参数化**：SQL 文件中的 `bfe_observability` 会在执行时被替换为 `CLICKHOUSE_DATABASE` 的值。测试环境可直接使用现成的 `setup_test.conf`（数据库 `bfe_observability_test`、Kafka Topic `bfe_ai_log_test`、消费组 `clickhouse_bfe_ai_log_test`），无需修改 `setup.conf`。

## 4. 执行部署

```bash
cd /path/to/ai-gateway-observability/clickhouse

# 生产环境（默认 setup.conf，数据库 bfe_observability）
bash setup.sh

# 测试环境（数据库 bfe_observability_test）
bash setup.sh ./setup_test.conf
```

脚本会按顺序执行：

| 步骤 | 操作 | SQL 文件 |
|:---:|------|----------|
| 1 | 创建数据库 `${CLICKHOUSE_DATABASE}` | `bfe_observability.sql` |
| 2 | 创建明细表 `bfe_ai_request_log` | `bfe_ai_request_log.sql` |
| 3 | 创建 Kafka 引擎暂存表 `bfe_ai_log_kafka` | `bfe_ai_log_kafka.sql` |
| 4 | 创建消费 MV `bfe_ai_log_load_mv`（Kafka → 明细） | `bfe_ai_log_load_mv.sql` |
| 5 | 创建聚合表 `bfe_ai_metrics_1m` | `bfe_ai_metrics_1m.sql` |
| 6 | 创建聚合 MV `bfe_ai_metrics_1m_mv`（明细 → 分钟聚合） | `bfe_ai_metrics_1m_mv.sql` |

> 脚本会将配置中的 `CLICKHOUSE_DATABASE` 和 Kafka 连接信息自动注入到对应 SQL 中执行。所有语句均为 `IF NOT EXISTS`，中断后重跑即可幂等续建。

## 5. 清空数据库对象

使用 `cleanup.sh` 删除指定数据库下的聚合 MV、聚合表、消费 MV、明细表、Kafka 引擎暂存表及数据库本身（MV 先于其目标表删除）：

```bash
cd /path/to/ai-gateway-observability/clickhouse

# 清空生产库（默认 setup.conf）
bash cleanup.sh

# 清空测试库（setup_test.conf）
bash cleanup.sh setup_test.conf

# 跳过确认直接清理
bash cleanup.sh -y setup_test.conf
```

## 6. 验证

部署完成后，执行以下命令验证各组件状态：

```bash
# 连接 ClickHouse（native TCP）
clickhouse-client -h172.18.1.244 --port=9000 -udefault --multiquery

# 查看库内全部对象（应见 5 个：2 张表 + 1 张 Kafka 表 + 2 个 MV）
SHOW TABLES FROM bfe_observability;

# 查看 Kafka 消费状态（26.10 列清单：consumer_id/num_commits/num_messages_read 等）
SELECT database, table, consumer_id, num_commits, num_messages_read
FROM system.kafka_consumers
WHERE database = 'bfe_observability';
```

`SHOW TABLES` 预期结果：

```
bfe_ai_log_kafka
bfe_ai_log_load_mv
bfe_ai_metrics_1m
bfe_ai_metrics_1m_mv
bfe_ai_request_log
```

### 6.1. 端到端验证清单

| 序号 | 验证项 | 方法 |
|:---:|--------|------|
| 1 | LogReader 正常发送 | 检查 LogReader 日志，确认无 Kafka 发送错误 |
| 2 | Kafka 收到消息 | `kafka-console-consumer.sh --bootstrap-server <broker> --topic bfe_ai_log --max-messages 1` |
| 3 | Kafka 引擎消费中 | `system.kafka_consumers` 有 `bfe_ai_log_kafka` 行且 `consumer_id` 非空、`num_messages_read` 增长 |
| 4 | 明细表有数据 | `SELECT count() FROM bfe_ai_request_log`（重放 demo 后约 1 分钟内增长） |
| 5 | 聚合表有数据 | `SELECT count() FROM bfe_ai_metrics_1m`（明细落库后秒级内出现） |

### 6.2. 行数观察

```bash
# 明细表行数
clickhouse-client -q "SELECT count() FROM bfe_observability.bfe_ai_request_log"

# 聚合表行数（每维组合每分钟一行，远小于明细）
clickhouse-client -q "SELECT count() FROM bfe_observability.bfe_ai_metrics_1m"

# 最近 5 分钟的明细
clickhouse-client -q "SELECT logid, log_time, ai_apikey_id, ai_target_model, ai_total_tokens \
  FROM bfe_observability.bfe_ai_request_log \
  WHERE log_time >= now() - INTERVAL 5 MINUTE ORDER BY log_time DESC LIMIT 10"
```

> 注意：消费从分区起始位点开始，首次部署后若 topic 中已有存量消息，明细行数会先追平历史再随新流量增长。

## 7. 演示数据

> 目录 `demo/` 自带三个样例消息（与 Doris 版相同）：正常请求、限流命中、认证拒绝。可直接用于端到端验证。

### 7.1. 使用 Kafka 控制台生产者发送测试数据

在 LogReader 还未配置上线时，可以用以下命令手动向 Kafka 发送测试消息，验证 Kafka 引擎表、消费 MV 和两级聚合链路：

```bash
# 发送正常 AI 请求（需先安装 kcat / kafkacat）
kcat -P -b 172.18.1.244:9092 -t bfe_ai_log < demo/normal_request.json

# 或使用 kafka-console-producer
docker exec -i kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 172.18.1.244:9092 \
  --topic bfe_ai_log < demo/normal_request.json
```

发送后等待 1~2 分钟，查询明细表确认数据已入库：

```sql
SELECT logid, log_time, ai_apikey_id, ai_target_model, ai_total_tokens,
       level1Name, level1          -- 打平列：由消费 MV 从 ai_apikeytags 提取
FROM bfe_ai_request_log
WHERE logid = 10602749765076101032   -- demo/normal_request.json 中的 logid
LIMIT 1;
```

`log_time` 应为样例 `timestamp`（epoch 秒）按 UTC 换算的墙钟，例如 `toDateTime(1787538975, 'UTC')` 的结果。

三个样例全部发送后，可验证聚合链路与口径（限流、认证拒绝各 +1）：

```sql
SELECT ts_min, request_count, error_count, auth_reject_count, rate_limit_hits
FROM bfe_ai_metrics_1m
ORDER BY ts_min DESC
LIMIT 5;
```

### 7.2. 正常 AI 请求

参考  [正常 AI 请求示例](../../demo/normal_request.json)

### 7.3. 限流命中请求

参考  [限流命中请求](../../demo/rate_limit.json)

### 7.4. 认证拒绝请求

参考  [认证拒绝请求](../../demo/auth_reject.json)

> **零值字段**：Kafka 消息中零值字段不会被 LogReader 输出（Go `omitempty`），ClickHouse 侧由列默认值归一为 `''`/`0`（Nullable 四列为 NULL），`col != ''` 等谓词行为与 Doris 一致。

## 8. 数据流延迟

| 环节 | 延迟 | 说明 |
|------|------|------|
| BFE → PB 日志文件 | < 1s | 请求结束时写入 |
| LogReader tail → Kafka | < 1s | 近实时 tail |
| Kafka → 明细表（Kafka 引擎 + 消费 MV） | 1~5s | 引擎批量拉取 + MV 打平写入 |
| 明细表 → 聚合表（聚合 MV） | 秒级 | MV 随插入同步触发，优于 Doris JOB 的分钟级 |
| **端到端总延迟** | **< 10s** | 满足分钟级看板需求 |

## 9. 常见问题

### 9.1. 消费不推进 / 行数不增长

按以下路径排查：

1. **消费是否启动**：`SELECT database, table, consumer_id, num_commits, num_messages_read FROM system.kafka_consumers WHERE database = 'bfe_observability'`。查不到 `bfe_ai_log_kafka` 行，说明 Kafka 表或消费 MV 未创建成功，重跑 `setup.sh`（幂等）。
2. **Kafka 侧位点**：确认消费组确实在拉取、LAG 是否收敛：

   ```bash
   docker exec kafka /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server 172.18.1.244:9092 \
     --describe --group clickhouse_bfe_ai_log
   ```

3. **消费组冲突**：确认 `KAFKA_GROUP_ID` 未与 Doris / StarRocks 链路重复（位点被另一链路覆盖会导致跳段或回放异常）。
4. **消息解析报错**：MV 表达式（如 `JSONExtractString` 类型不匹配）或 schema 对不上会导致消费停摆并报错重试。错误详情见 ClickHouse 服务日志（`/var/log/clickhouse-server/clickhouse-server.err.log` 或 `journalctl -u clickhouse-server`）；若实例开启了 `text_log`，可查 `system.text_log` 中 `level = 'Error'` 的记录。
5. **新增字段未容忍**：若报 `unknown field` 类错误，确认引擎表带 `input_format_skip_unknown_fields = 1`（已内置）；若反过来因引擎表缺列停摆，按"升级 ALTER 补列"模式给 `bfe_ai_log_kafka` 补列。

### 9.2. TTL 未立即删除数据

两表 TTL 为 7 天，但过期数据只在**后台 merge** 时才物理清除，因此 `count()` 不会在第 7 天整点瞬间下降，属正常现象。可用 `system.parts` 观察分区淘汰进度：

```sql
SELECT table, partition, active, name
FROM system.parts
WHERE database = 'bfe_observability'
ORDER BY table, partition;
```

### 9.3. 重复计数（at-least-once 口径）

ClickHouse Kafka 引擎 + MV 为 **at-least-once**：位点提交前若写入失败会重投，重复行会被明细表原样保留、并被聚合表（SummingMergeTree）重复累计。这与 Doris Routine Load 的事务性有差距，日志统计场景惯例接受小概率重复。运维上监控消费滞后与 `system.parts` 异常增长；发现重复后可按 §4.3 口径从明细表重算聚合表近 7 天数据兜底。

### 9.4. 与 Doris / StarRocks 并行部署

同一 Kafka topic 可同时挂多条链路，消费组（`doris_bfe_ai_log` / `starrocks_bfe_ai_log` / `clickhouse_bfe_ai_log`）位点独立，互不干扰，也无须为 ClickHouse 侧改 LogReader 或 topic 配置。

## 10. 重要提示：聚合表设计

本示例中的聚合表 `bfe_ai_metrics_1m` 包含 40 个维度列，**仅用于演示链路打通**。在实际生产环境中，维度过大和 1 分钟聚合粒度在 LLM 场景下往往不合理。建议：

- 按查询场景拆分为多个聚合表（核心流量、错误码、限流、认证拒绝等）
- 聚合粒度调整为 5 分钟或 15 分钟
- 稀疏维度（err_code、rate_limit_*）独立成表

## 11. 文档更新历史

| 日期 | 版本 | 变更说明 |
|------|------|----------|
| 2026-10-01 | v1.0 | 初始版本：ClickHouse 对接落地（Kafka 引擎表 + 消费 MV → 明细表 → 聚合 MV → 聚合表），六份 SQL 资产 + setup/cleanup 脚本；聚合 40 维 + 24 指标与 Doris 版逐列对齐 |
