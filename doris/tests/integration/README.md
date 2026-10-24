# Doris 集成测试

本目录承载 `ai-gateway-observability/doris` 的**真实 Doris 集成测试**（目录组织参考 `bfe/tests/integration`）：

- 被测对象是本仓库真实的 SQL 资产（`sqls/` 下的建库/建表/Routine Load/INSERT JOB/存量升级 SQL），不做任何副本或改写（仅做 `${VAR}` 变量替换）；
- 测试直连本机 Doris（WSL2 单机 FE+BE，见 `environment/doris-installation.md`），在独立的测试库（`bfe_observability_it_*`）中创建/销毁对象；
- Doris 未启动时全部用例自动 `SKIP`（不会失败），启动方式：`wsl bash /mnt/d/doris/bin/start-all.sh`。

## 目录结构

```text
doris/tests/integration/
├── README.md                                   # 本文档
├── go.mod                                      # 独立 Go module（doris-it）
├── common/
│   └── doris.go                                # Doris 连接、SQL 变量替换、执行/查询辅助
├── implementation/
│   └── scenario-DORIS01-report-cache-mirror-intent/
│       └── doris01_report_cache_mirror_intent_test.go
└── 测试设计文档/
    └── scenario-DORIS01-报表缓存镜像意图字段/
        └── 场景说明.md
```

## 运行方式

在 `doris/tests/integration/` 目录下执行：

```bash
# 运行全部集成测试
go test ./... -v

# 运行单个场景
go test ./implementation/scenario-DORIS01-report-cache-mirror-intent/ -v

# 运行单个测试例
go test ./implementation/scenario-DORIS01-report-cache-mirror-intent/ -run TestTC01 -v
```

## 连接配置

默认连接 `root@127.0.0.1:9030`（无密码），可用环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `DORIS_IT_HOST` | `127.0.0.1` | Doris FE 地址 |
| `DORIS_IT_PORT` | `9030` | Doris FE query_port |
| `DORIS_IT_USER` | `root` | 用户名 |
| `DORIS_IT_PASSWORD` | 空 | 密码 |
| `DORIS_IT_KEEP_DB` | 空 | 置 `1` 时测试结束后保留测试库（调试用） |

## 当前覆盖

| 场景 | 说明 |
|------|------|
| DORIS01 报表二期缓存/镜像/意图字段 | 基于真实 SQL 资产验证 v0.8 二期 Doris 侧改动：全新安装 schema（明细表 +13 列、聚合表 40 维、INSERT JOB，Doris 创建 JOB 时即校验 DO 语句）、Routine Load 映射被 Doris 接受（含限流打平列 json_extract）、明细→聚合口径（含空值归一）、存量升级 ALTER（每环境一次，重复执行报 Duplicate column）、聚合表 `_v2`+`REPLACE WITH TABLE` 原子换名重建流程 |

详细用例设计见 [测试设计文档](./测试设计文档/scenario-DORIS01-报表缓存镜像意图字段/场景说明.md)。
