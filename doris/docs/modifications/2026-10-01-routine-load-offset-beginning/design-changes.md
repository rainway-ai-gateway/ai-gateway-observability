# Routine Load 消费起始位置显式指定为 OFFSET_BEGINNING

## 背景

`bfe_ai_log_load_routine.sql` 创建 Kafka Routine Load 时未指定
`kafka_default_offsets`，Doris 3.0.8 缺省行为为 **LATEST**（在 job
首次调度时锁定分区末尾偏移）：

- job 创建**之前**已写入 Kafka 的日志消息被**永久跳过**，不进入明细表；
- 只有当消息产生晚于 job 首次调度，才能被消费。job 创建与日志产生之间
  存在**时序竞态**：调度先捕获末尾偏移则后续消息可消费（成功），日志先
  到达则被全部跳过（失败）。
- 竞态失败的运行中进一步观察到 FE 侧空轮询后的任务异常：首个 task 报
  "task has been abandoned when scheduling task" 后 job 长期停留在
  RUNNING 但不再产生任何 task（SHOW ROUTINE LOAD Progress 恒为
  `{"0":"0"}`、无事务、BE 无执行日志），直至 job 被停止——表现为
  "间歇性完全不消费"。

对日志入仓链路而言，历史消息应全部加载，LATEST 语义不正确。

## 修改内容

`doris/sqls/bfe_ai_log_load_routine.sql` 的 FROM KAFKA 子句增加（Kafka
客户端风格属性，`property.` 前缀，落入 job CustomProperties）：

```sql
"property.kafka_default_offsets" = "OFFSET_BEGINNING",
```

首次创建时从分区起始位置消费，消除上述竞态；job 暂停/恢复后按已提交
偏移继续，不受影响。

## 影响与兼容

| 项 | 影响 |
|----|------|
| 已部署的 job | 无影响（该属性只在 job 创建时生效一次）；如需补历史数据应重建 job |
| 新部署 | 首次加载会消费 topic 内全部历史消息，符合日志入仓预期 |
| Kafka 消息体积/分区 | 无 |
| 明细表行数 | job 创建前已存在的有效消息也会被加载（此前被静默跳过） |

## 验证

1. 语法：SC36 集成测试应用该 SQL 文件创建 job（`apply bfe_ai_log_load_routine.sql`）；
2. 行为：SC36 中 warmup `{}` 记录（触发 broker 自动建 topic 的占位消息）位于
   分区起始位，消费时因 UNIQUE KEY 列（hostid 等）为 NULL 被计入错误行过滤
   （`max_error_number=1000` 内），不落表；随后真实日志行正常入库；
3. 重跑 SC36（TC01/TC02）连续通过，不再出现 detail 表 0 行超时。
