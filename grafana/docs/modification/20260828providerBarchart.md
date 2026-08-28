# 变更记录：按提供商请求量面板改为横向条形图（Top 10 + 其他）

- **日期**：2026-08-28
- **变更范围**：Grafana 展示层（Dashboard `bfe-ai-gateway-observability.json` 面板 `按提供商请求量`，id=19）
- **变更原因**：该面板原为饼图，而提供商请求量分布极端倾斜——单一提供商占比可达 99%+，其余每个仅 1~3 条。饼图在小切片宽度趋近 0 的情况下只能显示最大一块，无法查看其余提供商。

## 1. 变更总览

| 类别 | 内容 |
|------|------|
| 图表类型 | `piechart` → `barchart`（横向） |
| 新增选项 | `orientation: "horizontal"`、`showValue: "always"`（每个横条显示数值标签） |
| SQL 重构 | Top 10 + 「其他」聚合，长尾合并、单提供商一统天下的情况被拆开展示 |
| 排序 | 「其他」强制沉底，其余按请求量降序 |

## 2. 新 SQL

原 SQL 仅 `GROUP BY ai_provider ORDER BY count DESC`，无长尾合并。新 SQL 使用窗口函数做 Top 10 + 其他：

```sql
SELECT name, SUM(cnt) AS count
FROM (
  SELECT CASE WHEN rn <= 10 THEN ai_provider ELSE '其他' END AS name, cnt
  FROM (
    SELECT ai_provider, cnt,
           ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
    FROM (
      SELECT ai_provider, SUM(request_count) AS cnt
      FROM bfe_ai_metrics_1m
      WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo() AND ai_provider != ''
      GROUP BY ai_provider
    ) a
  ) b
) c
GROUP BY name
ORDER BY CASE WHEN name = '其他' THEN 1 ELSE 0 END, count DESC
```

要点：

- `ROW_NUMBER() OVER (ORDER BY cnt DESC)`：按请求量降序排名。
- `rn <= 10` 保留原 provider，否则归入「其他」。
- `ORDER BY CASE WHEN name = '其他' THEN 1 ELSE 0 END, count DESC`：保证「其他」始终沉底，避免其聚合值高于某些单体 provider 时排序错乱（不依赖 Grafana 的 `sortBy`）。

## 3. 面板配置变化

| 项 | 旧值 | 新值 |
|----|------|------|
| `type` | `piechart` | `barchart` |
| `options.legend` | list（bottom，value+percent） | hidden |
| `options.orientation` | — | `horizontal` |
| `options.showValue` | — | `always` |
| `fieldConfig.defaults.unit` | — | `none` |

## 4. 生效方式

重新导入 Dashboard JSON 后生效，二选一：

1. 在 Grafana 中重新导入 `grafana/dashboards/bfe-ai-gateway-observability.json`；
2. 重新运行 `grafana/setup.sh`。

## 5. 相关文档

- Dashboard 面板布局与 SQL（含 3.14 节已同步更新）：[../design/DASHBOARD_DESIGN.md](../design/DASHBOARD_DESIGN.md)
- Grafana 配置指南：[../user/HOWTO.md](../user/HOWTO.md)