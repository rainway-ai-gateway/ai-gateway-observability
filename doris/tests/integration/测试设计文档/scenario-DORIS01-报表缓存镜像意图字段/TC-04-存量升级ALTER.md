# TC-04 存量升级 ALTER

## 用例编号与名称

TC-04 存量升级 ALTER

## 所属场景

DORIS01 报表二期（v0.8）Doris 侧对齐缓存/镜像/意图字段

## 版本声明

- `ai-gateway-observability`：当前源码版本（v0.8 二期）
- `Doris`：本机 WSL2 单机 3.0.8

## 测试目的

验证存量部署升级脚本 `sqls/upgrade/2026-09-29-report-cache-mirror-intent/bfe_ai_request_log_alter.sql`：在 v1.x 旧表（无新列）上执行后 13 列就位且类型正确；并固化"每环境仅执行一次"的契约——Doris 3.0 不支持 `ADD COLUMN IF NOT EXISTS`，重复执行报错（`Can not add column which already exists ...`）属预期行为。对应 HOWTO §11.1。

## 运行模式

单组件模式：直连真实 Doris FE，在独立测试库中模拟旧表并执行升级脚本。

## 前置条件

1. Doris FE/BE 已启动，root 无密码可连。
2. 同 TC-01。

## 测试数据构造

- 独立测试库，按当前（新）schema 建明细表。
- 模拟旧表：一条 `ALTER TABLE ... DROP COLUMN <col>, ...` 删除全部 13 个新列（10 个缓存/镜像/意图列 + 3 个限流打平列），回到 v1.x 形态；通过 `information_schema` 断言 13 列已不存在。

## 执行步骤

1. 创建独立测试库并建明细表，删除 13 列模拟旧表。
2. 第一次执行升级脚本（`${DORIS_DATABASE}` 替换后）→ 通过 `information_schema.columns` 断言 13 列全部存在且类型与期望逐项相等（`ai_cache_status varchar(16)`、`mirror_hit tinyint`、`ai_intent_latency_us bigint` 等）。
3. 第二次执行同一脚本 → 预期失败，断言报错信息含 `already exists` 或 `duplicate`（大小写不敏感）。

## 预期结果

- 步骤 2：13 列类型断言全部通过。
- 步骤 3：第二次执行返回错误且不通过静默（固化"每环境一次"契约，防止误以为幂等而重复跑）。

## 清理

`t.Cleanup`：`DROP DATABASE <测试库> FORCE`。
