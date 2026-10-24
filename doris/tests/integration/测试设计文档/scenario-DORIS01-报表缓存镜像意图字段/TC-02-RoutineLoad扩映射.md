# TC-02 Routine Load 扩映射

## 用例编号与名称

TC-02 Routine Load 扩映射

## 所属场景

DORIS01 报表二期（v0.8）Doris 侧对齐缓存/镜像/意图字段

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 二期）
- `Doris`：本机 WSL2 单机 3.0.8

## 测试目的

验证扩映射后的 `bfe_ai_log_load_routine.sql` 能被 Doris 接受：任务创建成功，且 `COLUMNS(...)` 中含 10 个缓存/镜像/意图新列与 3 个限流打平列（`json_extract` 计算列）。Routine Load 不支持在线改 COLUMNS，本例等价于 HOWTO §11.2「重建 Routine Load」后的形态校验。

## 运行模式

单组件模式：直连真实 Doris FE，在独立测试库中创建 Routine Load（Kafka 不可达不影响任务创建，任务会后台重试连接）。

## 前置条件

1. Doris FE/BE 已启动，root 无密码可连。
2. 同 TC-01。

## 测试数据构造

- 独立测试库（同 TC-01），仅建明细表/聚合表（不建 INSERT JOB，避免全局 JOB 名干扰）。
- Kafka 参数替换为占位值（`127.0.0.1:9092` / `bfe_ai_log_it`），仅用于通过语法与创建校验。

## 执行步骤

1. 创建独立测试库并建明细表/聚合表。
2. 执行仓库 `bfe_ai_log_load_routine.sql`（`${VAR}` 替换后）。
3. `SHOW ROUTINE LOAD FOR bfe_ai_log_load`：断言任务存在。
4. `SHOW CREATE ROUTINE LOAD FOR bfe_ai_log_load`：断言 COLUMNS 中含 10 个新列（`ai_cache_status`…`ai_intent_questions_version`）与 3 个限流打平列（`rate_limit_policy_id`/`rate_limit_type`/`rate_limit_rule_name`）。

## 预期结果

- 任务创建成功，`SHOW ROUTINE LOAD` 返回 `bfe_ai_log_load` 记录。
- COLUMNS 定义中包含全部 13 个列名（含 `json_extract` 计算列表达式）。

## 清理

`t.Cleanup`：先 `STOP ROUTINE LOAD FOR bfe_ai_log_load`（避免 BE 持续重连不存在的 Kafka），再 `DROP DATABASE <测试库> FORCE`。
