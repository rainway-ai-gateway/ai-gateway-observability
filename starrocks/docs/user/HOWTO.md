# BFE Observability — StarRocks 端搭建指南

本文档指导你在已搭建好的 StarRocks + Kafka 集群上，使用 `setup.sh` 脚本一键创建 `bfe_observability` 数据库及其所有对象（明细表、分钟聚合物化视图、Routine Load）。

## 1. 前提条件

| 组件 | 版本 | 说明 |
|------|------|------|
| StarRocks | 3.5+ | FE 已启动，MySQL 协议端口可访问（默认 9030） |
| Kafka | 2.8+ | Broker 可访问，`bfe_ai_log` Topic 已创建 |
| mysql-client | 任意 | 用于连接 StarRocks FE 执行 SQL |

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
starrocks/
├── setup.sh                          # 一键部署脚本
├── cleanup.sh                        # 清空脚本（删表 / 物化视图 / Routine Load）
├── setup.conf                        # 配置文件（生产，数据库 bfe_observability）
├── setup_test.conf                   # 配置文件（测试，数据库 bfe_observability_test）
├── sqls/                             # DDL SQL 文件（由脚本自动执行）
│   ├── bfe_observability.sql         # 创建数据库
│   ├── bfe_ai_request_log.sql        # 明细表（DUPLICATE KEY）
│   ├── bfe_ai_log_load_routine.sql   # Routine Load（Kafka → StarRocks）
│   └── bfe_ai_metrics_1m.sql         # 分钟聚合物化视图（异步 MV，名字即 bfe_ai_metrics_1m）
├── demo/                             # 演示用 JSON 消息样例
│   ├── normal_request.json           # 正常 AI 请求
│   ├── rate_limit.json               # 限流命中请求
│   └── auth_reject.json              # 认证拒绝请求
└── docs/
    ├── user/
    │   └── HOWTO.md                  # 本文档（StarRocks 部署指南）
    ├── design/
    │   └── TABLE_DESIGN.md           # 表设计说明
    └── modifications/
        ├── 2026-10-02-starrocks-dock/           # StarRocks 对接变更记录
        └── 2026-10-22-issue-10-boolean-columns/ # 布尔列落 NULL 修复（issue #10，含存量升级步骤）
```

## 3. 配置

编辑 `setup.conf`，填入你的实际环境信息：

```bash
# --- Kafka 连接信息 ---
KAFKA_BROKER_LIST=172.18.1.244:9092   # Kafka broker 列表（多个用逗号分隔）
KAFKA_TOPIC=bfe_ai_log                # LogReader 发送日志的 Topic
KAFKA_GROUP_ID=starrocks_bfe_ai_log   # 消费组（必须与 Doris 链路的 group.id 不同！）
KAFKA_CLIENT_ID=starrocks_bfe_ai_log

# --- StarRocks FE 连接信息（MySQL 协议） ---
STARROCKS_HOST=127.0.0.1
STARROCKS_PORT=9030
STARROCKS_USER=root
STARROCKS_PASSWORD=
STARROCKS_DATABASE=bfe_observability

# --- 分区初始值 ---
INIT_PARTITION_DATE=2026-10-02        # 建议设置为部署日期前一天的日期
```

> **消费组隔离**：`KAFKA_GROUP_ID` 必须与 Doris 链路（`doris_bfe_ai_log`）、ClickHouse 链路（`clickhouse_bfe_ai_log`）不同，否则多个引擎共享位点会互相吞消息。

## 4. 部署

```bash
# 进入 starrocks 目录
cd starrocks

# 一键部署（默认读取 setup.conf）
bash setup.sh

# 或使用测试配置（数据库 bfe_observability_test，Topic bfe_ai_log_test）
bash setup.sh setup_test.conf
```

部署完成后，脚本会输出验证命令。

## 5. 验证

### 5.1. 检查 Routine Load 状态

```bash
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability -e "SHOW ROUTINE LOAD FOR bfe_ai_log_load\G"
```

关注 `State`（期望 `RUNNING`）与 `ReasonOfStateChanged`。若 Kafka 暂时不可达，状态会在 `RUNNING`/`PAUSED` 间变化，Kafka 恢复后自动恢复。

### 5.2. 检查物化视图状态

```bash
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability -e "SHOW MATERIALIZED VIEWS\G"
```

关注 `is_active`（期望 `true`）与 `last_refresh_state`（期望 `SUCCESS`）。异步 MV 每分钟触发一次刷新，数据流入后 1~2 分钟内可在 `bfe_ai_metrics_1m` 查到聚合结果。

### 5.3. 发送 demo 样例验证链路

```bash
# 逐个发送 demo 消息到 Kafka
cat demo/normal_request.json | docker exec -i kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 172.18.1.244:9092 --topic bfe_ai_log

# 查看明细（UTC 墙钟）
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability \
  -e "SELECT logid, log_time, ai_apikey_id, level1Name, level1, rate_limit_policy_id, ai_auth_reject_reason FROM bfe_ai_request_log LIMIT 5;"

# 查看分钟聚合（等待 1~2 分钟刷新后）
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability \
  -e "SELECT ts_min, request_count, error_count, auth_reject_count, rate_limit_hits, input_tokens, output_tokens, total_tokens FROM bfe_ai_metrics_1m LIMIT 5;"
```

## 6. 本机 WSL2 测试环境

本仓库配套的测试环境为 WSL2（Ubuntu）内单机 StarRocks 3.5.21（FE + BE 同机），Windows 经 localhost 转发访问（详见安装说明 `starrocks-installation.md`）：

```bash
# 启动 / 停止（WSL 重启后需重新启动，数据保留）
wsl bash ~/starrocks/bin/start-all.sh
wsl bash ~/starrocks/bin/stop-all.sh

# 验证 BE 存活
wsl bash -c "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW PROC \"/backends\";'" | awk 'NR>1 {print "Alive:", $21}'
```

低配说明：FE 堆 512M、BE 查询内存上限 1.5G，仅供功能验证；大批量导入/大表聚合需调大 `mem_limit`。

## 7. 清理

```bash
bash cleanup.sh                # 交互确认后清理
bash cleanup.sh -y             # 跳过确认
bash cleanup.sh setup_test.conf  # 清理测试库
```

## 8. 存量部署升级

适用于用**旧版 `sqls/`（`ai_stream` / `is_trust_src_ip` 声明为 `TINYINT`）**部署过的环境——症状是两列在明细表里**恒为 NULL**、`stream_requests` 恒 0、`logs?stream=true|false` 都查不到行。原因与修复依据见 [2026-10-22-issue-10-boolean-columns](../modifications/2026-10-22-issue-10-boolean-columns/design-changes.md)。

> SR 不支持 `ALTER TABLE ... MODIFY COLUMN` 从 `TINYINT` 改 `BOOLEAN`（报 `Can not change TINYINT to BOOLEAN`），因此升级是**重建式**的：让 Kafka 里的原始消息按新 DDL 重新解析一遍，顺带把历史值修好。

### 8.1. 升级前检查

```bash
# 1) 确认症状：两列非空计数远小于总行数（旧 DDL 下通常为 0）
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability -e "
SELECT COUNT(*) total, COUNT(ai_stream) ai_stream_non_null, COUNT(is_trust_src_ip) trust_non_null
FROM bfe_ai_request_log;"

# 2) 确认 Kafka 保留期 ≥ 期望的明细历史长度（RL 为 OFFSET_BEGINNING，重放从 retention 起点开始）
#    例：retention.ms=604800000 即 7 天

# 3) 确认 INIT_PARTITION_DATE 早于重放窗口起点（早于它的 log_time 落入 p_init 兜底分区）
grep INIT_PARTITION_DATE setup.conf
```

### 8.2. 操作步骤

```bash
# 1) 停消费（同名作业在 STOP 后仍可能报 "already used"，可先改名留档）
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability -e \
  "STOP ROUTINE LOAD FOR bfe_ai_log_load;"
# 可选：ALTER ROUTINE LOAD FOR bfe_observability.bfe_ai_log_load RENAME TO bfe_ai_log_load_old;

# 2) 拆对象（MV 绑定基表，必须一起重建）
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability -e \
  "DROP MATERIALIZED VIEW bfe_ai_metrics_1m; DROP TABLE bfe_ai_request_log;"

# 3) 用仓库最新 sqls/ 重建（建表 → MV → Routine Load，RL 从 OFFSET_BEGINNING 重放）
bash setup.sh setup.conf

# 4) 等重放追平（SHOW ROUTINE LOAD 的位点不再推进）后验收
mysql -h127.0.0.1 -P9030 -uroot -Dbfe_observability -e "
SELECT COUNT(*) total, COUNT(ai_stream) ai_stream_non_null,
       SUM(ai_stream = 1) stream1, SUM(ai_stream = 0) stream0
FROM bfe_ai_request_log;"
```

验收判据：`ai_stream_non_null = total`、`stream1 > 0`；与 MySQL/Doris 同窗口的 `ai_stream` 分布一致（窗口计算用 `UTC_TIMESTAMP()`，SR 会话时区为 CST）。

### 8.3. 注意事项

1. **历史值只能靠重放修复**：旧表里已被写成 NULL 的两列信息已丢失，`INSERT INTO 新表 SELECT ... FROM 旧表` 只会把 NULL 搬过去，因此不做"回灌旧表"；
2. **重放期间报表缺数**（分钟级到小时级，取决于消息量与 Kafka 消费速度），选低峰执行；不允许停数的环境可改用 `2026-10-22-issue-10-boolean-columns` §4.4 的平滑切换（新表 + 临时消费组重放 + `SWAP WITH`）；
3. **时间戳无分区会成错误行**：`INIT_PARTITION_DATE` 之后、动态分区窗口（`start=-7`）之外的消息会因无对应分区被跳过，必要时在重放前临时放宽 `dynamic_partition.start`；
4. 若 Kafka 保留期短于 7 天，明细历史会相应缩短（旧数据本就不可修复），需在变更单写明。

### 8.4. 缺价信号两列（bfe#1479，无需重建表）

模型缺少某接口定价时，BFE 会把缺价事实写到 `ai_price_miss`（`1` 发生 / `0` 显式未发生 / **NULL 未上报**）与 `ai_price_warn`（`fallback` / `subitem_missing` / `no_entry` / `no_baseline` / `no_model_table` / `reject`）。与 §8 的布尔类型修复不同，**这次是纯加列，不需要重建表、不需要重放 Kafka**（历史行两列为 NULL = 未上报）。设计见 [2026-10-30-issue-1479-price-miss-columns](../modifications/2026-10-30-issue-1479-price-miss-columns/design-changes.md)。

```sql
-- 1) 判断是否已有两列（SR 3.5 不支持 ADD COLUMN IF NOT EXISTS，先查再加）
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'bfe_observability' AND table_name = 'bfe_ai_request_log'
   AND column_name IN ('ai_price_miss','ai_price_warn');

-- 2) 加列（ai_price_miss 必须是 BOOLEAN：JSON true/false 落 TINYINT 会静默 NULL，见 issue #10）
ALTER TABLE bfe_ai_request_log
    ADD COLUMN ai_price_miss BOOLEAN      COMMENT '是否发生缺价回退（NULL=未上报，1/0）' AFTER ai_intent_questions_version,
    ADD COLUMN ai_price_warn VARCHAR(32)  COMMENT '缺价形态：fallback/subitem_missing/no_entry/no_baseline/no_model_table/reject（NULL=未上报）' AFTER ai_price_miss;
```

Schema Change 是**异步**的：`ALTER` 返回成功后要轮询 `information_schema.columns` 直到两列可见，再重建 Routine Load：

```sql
-- 3) 重建 RL（COLUMNS 扩 2 列；沿用原 KAFKA_GROUP_ID 才会从已提交位点续消费）
STOP ROUTINE LOAD FOR bfe_ai_log_load;
DROP ROUTINE LOAD FOR bfe_ai_log_load;
--  再执行新版 sqls/bfe_ai_log_load_routine.sql
```

> **顺序不可颠倒**：先加列、再重建 RL。明细表是 `DUPLICATE KEY` 模型、没有幂等覆盖，重建 RL 若从 `OFFSET_BEGINNING` 重放会产生重复行——沿用原消费组即可避免。
>
> **不要照抄 §8 的重建流程**：那是为了修 `TINYINT → BOOLEAN` 的历史值；本次加列无需重建表。

## 9. 常见问题

### 9.1. Routine Load 状态为 PAUSED

查看 `SHOW ROUTINE LOAD FOR bfe_ai_log_load\G` 的 `ReasonOfStateChanged`：
- Kafka 不可达 / Topic 不存在：检查 `KAFKA_BROKER_LIST` 与 Topic，恢复后 `RESUME ROUTINE LOAD FOR bfe_ai_log_load`。
- 错误行数超过 `max_error_number`（默认 1000）：检查消息格式与表结构是否匹配，必要时 `STOP` 后重新 `setup.sh`。

### 9.2. 物化视图刷新失败

`SHOW MATERIALIZED VIEWS\G` 查看 `last_refresh_error_code` / `last_refresh_error_message`。常见原因：BE 内存不足（低配环境）、基表被重建（重建基表后 MV 可能失效，重新执行 `sqls/bfe_ai_metrics_1m.sql` 重建）。

### 9.3. 查询报错 memory limit exceeded

低配 BE（1.5G）上避免大窗口聚合查询；需要更大内存时调整 BE `be.conf` 的 `mem_limit` 并重启 BE。

### 9.4. 保留期说明

明细表动态分区 `start=-7`（保留 7 天，过期分区自动删除）；物化视图 `partition_ttl=7 DAY`。若运行版本不支持 MV 的 `partition_ttl`，由 cleanup 脚本或定时任务兜底 `DROP PARTITION`。
