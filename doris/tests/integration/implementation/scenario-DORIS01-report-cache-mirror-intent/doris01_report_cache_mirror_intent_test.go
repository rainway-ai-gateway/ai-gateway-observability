// 场景 DORIS01：数据报表二期（v0.8）Doris 侧对齐缓存/镜像/意图字段。
//
// 被测对象是本仓库真实 SQL 资产（doris/sqls/，仅做 ${VAR} 变量替换），
// 覆盖六个测试例（详见 ../../测试设计文档/scenario-DORIS01-报表缓存镜像意图字段/场景说明.md）：
//
// TC01 全新安装 schema：明细表 +10 列 / 聚合表 40 维 KEY / INSERT JOB 创建成功
// TC02 Routine Load 映射扩列被 Doris 接受（SHOW CREATE ROUTINE LOAD 含新 10 列）
// TC03 明细 → 聚合口径：新维度取值与空值归一（COALESCE ”/0）
// TC04 存量升级 ALTER：旧表加 13 列 + 重复执行报 Duplicate column（Doris 3.0 无 IF NOT EXISTS）
// TC05 聚合表重建：_v2 建表 + REPLACE WITH TABLE 原子换名（HOWTO §11.3 流程）
// TC06 明细表唯一键重建（bfe#1417）：旧四列键下同秒并发只留 1 行（缺陷复现）→ 补 logid 重建后
//
//	各自成行 + NULL logid 归 0（HOWTO §11.5 流程）
package doris01_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"doris-it/common"
)

// jobName 与 sqls/bfe_ai_metrics_1m_job.sql 中的 CREATE JOB 保持一致。
const jobName = "bfe_ai_metrics_1m_job"

// newDetailColumns 期望的明细表新增 10 列及其类型（小写，形如 varchar(16)）。
// 注意：Doris information_schema 将 BOOLEAN 报告为 tinyint。
var newDetailColumns = map[string]string{
	"ai_cache_status":             "varchar(16)",
	"mirror_hit":                  "tinyint",
	"mirror_cluster":              "varchar(128)",
	"ai_intent_question":          "varchar(64)",
	"ai_intent_answer":            "varchar(64)",
	"ai_intent_confidence":        "double",
	"ai_intent_source":            "varchar(32)",
	"ai_intent_latency_us":        "bigint",
	"ai_intent_cache_hit":         "tinyint",
	"ai_intent_questions_version": "varchar(32)",
}

// flattenColumns 限流首个命中打平列（Routine Load 导入时 json_extract 提取，JOB 直接消费）。
var flattenColumns = map[string]string{
	"rate_limit_policy_id": "varchar(128)",
	"rate_limit_type":      "varchar(32)",
	"rate_limit_rule_name": "varchar(128)",
}

func colNames(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestTC01FreshInstallSchema 全新安装 schema 断言。
func TestTC01FreshInstallSchema(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.UniqueDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, true /*withJob*/)

	// 1) 明细表新增 10 列 + 限流打平 3 列存在且类型正确
	for _, cols := range []map[string]string{newDetailColumns, flattenColumns} {
		got := common.ColumnTypes(t, db, dbName, "bfe_ai_request_log", colNames(cols))
		for col, wantType := range cols {
			gotType, ok := got[col]
			if !ok {
				t.Errorf("明细表缺少新列 %s", col)
				continue
			}
			if gotType != wantType {
				t.Errorf("明细表列 %s 类型 = %s，期望 %s", col, gotType, wantType)
			}
		}
	}

	// 2) 聚合表 KEY 维度 = 40（37 既有 + 3 新维度）
	ddl := common.QueryText(t, db, dbName, "SHOW CREATE TABLE bfe_ai_metrics_1m")
	for _, col := range []string{"ai_cache_status", "mirror_hit", "ai_intent_answer"} {
		if !strings.Contains(ddl, col) {
			t.Errorf("聚合表 DDL 缺少新维度 %s", col)
		}
	}
	// Doris SHOW CREATE TABLE 输出形如：AGGREGATE KEY(`ts_min`, `hostid`, ...)
	keyList := ddlBetween(t, ddl, "AGGREGATE KEY(", ")")
	if n := len(splitCSV(keyList)); n != 40 {
		t.Errorf("聚合表 AGGREGATE KEY 维度数 = %d，期望 40（37+3）", n)
	}

	// 3) INSERT JOB 创建成功（Doris 3.0 通过 jobs() 表函数查询，JOB 名全局唯一）
	var jobStatus string
	err := db.QueryRow("SELECT Status FROM jobs(\"type\"=\"insert\") WHERE Name = ?", jobName).Scan(&jobStatus)
	if err != nil {
		t.Errorf("未找到 INSERT JOB %s（jobs() 查询: %v）", jobName, err)
	}
}

// TestTC02RoutineLoadAccepted Routine Load 扩映射被 Doris 接受。
func TestTC02RoutineLoadAccepted(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.UniqueDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, false /*withJob*/)

	common.ExecFileIn(t, db, dbName, common.SQLPath(t, "bfe_ai_log_load_routine.sql"), vars)
	t.Cleanup(func() {
		// 先停任务再删库，避免 BE 持续重连不存在的 Kafka
		_, _ = db.Exec("USE " + dbName + ";\nSTOP ROUTINE LOAD FOR bfe_ai_log_load")
	})

	out := common.QueryText(t, db, dbName, "SHOW ROUTINE LOAD FOR bfe_ai_log_load")
	if !strings.Contains(out, "bfe_ai_log_load") {
		t.Fatalf("Routine Load 创建失败，SHOW ROUTINE LOAD 输出:\n%s", out)
	}

	// COLUMNS 映射含新增 10 列 + 限流打平 3 列
	createSQL := common.QueryText(t, db, dbName, "SHOW CREATE ROUTINE LOAD FOR bfe_ai_log_load")
	for _, cols := range []map[string]string{newDetailColumns, flattenColumns} {
		for col := range cols {
			if !strings.Contains(createSQL, col) {
				t.Errorf("Routine Load COLUMNS 缺少新列 %s", col)
			}
		}
	}
}

// TestTC03DetailToAggregate 明细 → 聚合口径验证（新维度取值 + 空值归一）。
func TestTC03DetailToAggregate(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.UniqueDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	// 不创建周期 JOB，避免调度写入干扰口径断言；本例手动执行 JOB 中的 INSERT..SELECT
	common.SetupFreshInstall(t, db, dbName, vars, false /*withJob*/)

	// demo 样例的新字段取值作为期望值（同时校验 demo 文件与 DDL 的一致性）
	demo := loadDemo(t)
	want := map[string]string{
		"ai_cache_status":    jsonStr(t, demo, "ai_cache_status"),
		"mirror_hit":         jsonBool01(t, demo, "mirror_hit"),
		"ai_intent_answer":   jsonStr(t, demo, "ai_intent_answer"),
		"input_tokens":       jsonStr(t, demo, "ai_input_tokens"),
		"output_tokens":      jsonStr(t, demo, "ai_output_tokens"),
		"total_tokens":       jsonStr(t, demo, "ai_total_tokens"),
		"mirror_cluster":     jsonStr(t, demo, "mirror_cluster"),
		"ai_intent_question": jsonStr(t, demo, "ai_intent_question"),
	}

	// 固定聚合窗口：上一分钟桶，避免跨分钟边界 flaky
	tsMin := time.Now().Add(-time.Minute).Truncate(time.Minute)
	tsMinLit := tsMin.Format("2006-01-02 15:04:05")
	tsNextLit := tsMin.Add(time.Minute).Format("2006-01-02 15:04:05")
	// 两行使用桶内不同 log_time：明细表 UNIQUE KEY 为 (hostid, log_time, ai_apikey_id,
	// ai_requested_model)，同 key 第二次写入会覆盖第一行
	logTimeDemoLit := tsMin.Add(20 * time.Second).Format("2006-01-02 15:04:05")
	logTimeLegacyLit := tsMin.Add(40 * time.Second).Format("2006-01-02 15:04:05")

	// 行 A：demo 全字段（含缓存命中 + 意图分类 + 镜像命中）
	fullInsert := fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, logid, product,
     ai_cache_status, mirror_hit, mirror_cluster,
     ai_intent_question, ai_intent_answer, ai_intent_confidence, ai_intent_source,
     ai_intent_latency_us, ai_intent_cache_hit, ai_intent_questions_version,
     ai_input_tokens, ai_output_tokens, ai_total_tokens)
VALUES
    ('it_host', '%s', 'it_key', 'it_model', 1001, 'it_product',
     '%s', %s, '%s',
     '%s', '%s', %s, '%s',
     %s, %s, '%s',
     %s, %s, %s)`,
		logTimeDemoLit,
		want["ai_cache_status"], want["mirror_hit"], want["mirror_cluster"],
		want["ai_intent_question"], want["ai_intent_answer"],
		jsonStr(t, demo, "ai_intent_confidence"), jsonStr(t, demo, "ai_intent_source"),
		jsonStr(t, demo, "ai_intent_latency_us"), jsonBool01(t, demo, "ai_intent_cache_hit"),
		jsonStr(t, demo, "ai_intent_questions_version"),
		want["input_tokens"], want["output_tokens"], want["total_tokens"])
	common.ExecIn(t, db, dbName, fullInsert)

	// 行 B：旧版消息（无新字段，应为 NULL → 聚合归一为 '' / 0）
	legacyInsert := fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, logid, product)
VALUES
    ('it_host', '%s', 'it_key', 'it_model', 1002, 'it_product')`, logTimeLegacyLit)
	common.ExecIn(t, db, dbName, legacyInsert)

	// 执行仓库 JOB 中的 INSERT..SELECT，仅把窗口换成固定字面量（其余原样）
	jobBody := extractJobInsertSelect(t, vars)
	windowed := rewindow(t, jobBody, tsMinLit, tsNextLit)
	common.ExecIn(t, db, dbName, windowed)

	// 断言聚合结果
	rows, err := db.Query(fmt.Sprintf(`SELECT ai_cache_status, mirror_hit, ai_intent_answer,
       request_count, input_tokens, output_tokens, total_tokens
FROM %s.bfe_ai_metrics_1m
WHERE ts_min = '%s'`, dbName, tsMinLit))
	if err != nil {
		t.Fatalf("查询聚合表失败: %v", err)
	}
	defer rows.Close()

	type aggRow struct {
		key  string
		vals [7]string
	}
	gotRows := map[string]aggRow{}
	for rows.Next() {
		var cols [7]sql.NullString
		ptrs := make([]interface{}, len(cols))
		for i := range cols {
			ptrs[i] = &cols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("扫描聚合行失败: %v", err)
		}
		vals := [7]string{}
		for i, c := range cols {
			if c.Valid {
				vals[i] = c.String
			}
		}
		gotRows[vals[0]+"|"+vals[1]+"|"+vals[2]] = aggRow{vals: vals}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历聚合结果失败: %v", err)
	}

	// 期望 2 行：demo 行（新维度原值）+ 旧消息行（空值归一）
	wantDemo := [7]string{want["ai_cache_status"], want["mirror_hit"], want["ai_intent_answer"], "1",
		want["input_tokens"], want["output_tokens"], want["total_tokens"]}
	wantLegacy := [7]string{"", "0", "", "1", "0", "0", "0"}

	assertAggRow := func(name string, want [7]string) {
		key := want[0] + "|" + want[1] + "|" + want[2]
		got, ok := gotRows[key]
		if !ok {
			t.Errorf("%s: 聚合结果缺少行 %q，实际: %v", name, key, gotRows)
			return
		}
		for i := 1; i < 7; i++ {
			if got.vals[i] != want[i] {
				t.Errorf("%s: 第 %d 列 = %q，期望 %q（整行 %v）", name, i, got.vals[i], want[i], got.vals)
			}
		}
	}
	assertAggRow("demo 行", wantDemo)
	assertAggRow("旧消息行", wantLegacy)
	if len(gotRows) != 2 {
		t.Errorf("聚合行数 = %d，期望 2（实际: %v）", len(gotRows), gotRows)
	}
}

// TestTC04UpgradeAlter 存量升级 ALTER：旧表加列（13 列）+ 重复执行报 Duplicate column（预期行为）。
// Doris 3.0 不支持 ADD COLUMN IF NOT EXISTS，脚本每环境仅执行一次，详见升级 SQL 头注释。
func TestTC04UpgradeAlter(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.UniqueDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, false /*withJob*/)

	// 模拟 v1.x 旧表：按新 schema 建表后删除 13 列
	colsAll := append(colNames(newDetailColumns), colNames(flattenColumns)...)
	var drops []string
	for _, c := range colsAll {
		drops = append(drops, "DROP COLUMN "+c)
	}
	common.ExecIn(t, db, dbName, "ALTER TABLE bfe_ai_request_log "+strings.Join(drops, ", "))
	if got := common.ColumnTypes(t, db, dbName, "bfe_ai_request_log", colsAll); len(got) != 0 {
		t.Fatalf("旧表模拟失败，仍存在新列: %v", got)
	}

	// 第一次执行升级 ALTER → 13 列就位
	alterFile := common.SQLPath(t, "upgrade", "2026-09-29-report-cache-mirror-intent", "bfe_ai_request_log_alter.sql")
	common.ExecFileIn(t, db, dbName, alterFile, vars)
	got := common.ColumnTypes(t, db, dbName, "bfe_ai_request_log", colsAll)
	for _, cols := range []map[string]string{newDetailColumns, flattenColumns} {
		for col, wantType := range cols {
			if got[col] != wantType {
				t.Errorf("升级后列 %s 类型 = %q，期望 %q", col, got[col], wantType)
			}
		}
	}

	// 第二次执行 → 预期失败（列已存在），验证每环境仅一次的契约
	// Doris 3.0 报错形如 "Can not add column which already exists in base table: xxx"
	content := "USE " + dbName + ";\n" + common.LoadSQL(t, alterFile, vars)
	_, err := db.Exec(content)
	if err == nil {
		t.Errorf("重复执行升级 ALTER 未报错，期望 column already exists（每环境仅执行一次）")
	} else {
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "already exists") && !strings.Contains(msg, "duplicate") {
			t.Errorf("重复执行升级 ALTER 的报错不符合预期: %v", err)
		}
	}
}

// TestTC05MetricsRebuildSwap 聚合表 _v2 + SWAP 重建流程（HOWTO §11.3）。
func TestTC05MetricsRebuildSwap(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.UniqueDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, false /*withJob*/)

	// 1) 按仓库新 DDL 建 _v2（表名替换，与 HOWTO §11.3 步骤一致）
	v2SQL := strings.Replace(common.LoadSQL(t, common.SQLPath(t, "bfe_ai_metrics_1m.sql"), vars),
		"CREATE TABLE bfe_ai_metrics_1m", "CREATE TABLE bfe_ai_metrics_1m_v2", 1)
	common.ExecIn(t, db, dbName, v2SQL)

	// 2) 写入哨兵行到 _v2
	tsMin := time.Now().Truncate(time.Minute).Format("2006-01-02 15:04:05")
	common.ExecIn(t, db, dbName, fmt.Sprintf(
		"INSERT INTO bfe_ai_metrics_1m_v2 (ts_min, hostid, ai_apikey_id, ai_cache_status, request_count, input_tokens) "+
			"VALUES ('%s', 'it_host', 'swap_key', 'hit', 5, 100)", tsMin))

	// 3) 原子换名（Doris 语法：REPLACE WITH TABLE ... PROPERTIES('swap'='true')）
	common.ExecIn(t, db, dbName, "ALTER TABLE bfe_ai_metrics_1m REPLACE WITH TABLE bfe_ai_metrics_1m_v2 PROPERTIES('swap'='true')")

	// 4) 换名后从新表名读到哨兵行
	var cnt, inputTokens string
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT CAST(SUM(request_count) AS CHAR), CAST(SUM(input_tokens) AS CHAR) FROM %s.bfe_ai_metrics_1m "+
			"WHERE ai_apikey_id='swap_key' AND ai_cache_status='hit'", dbName)).Scan(&cnt, &inputTokens); err != nil {
		t.Fatalf("查询换名后聚合表失败: %v", err)
	}
	if cnt != "5" || inputTokens != "100" {
		t.Errorf("换名后聚合数据异常: request_count=%s input_tokens=%s，期望 5/100", cnt, inputTokens)
	}

	// 5) 删除旧表（换名后即为 _v2）
	common.ExecIn(t, db, dbName, "DROP TABLE bfe_ai_metrics_1m_v2")
}

// TestTC06DetailUniqueKeyRebuild 明细表唯一键重建（HOWTO §11.5，bfe#1417）：
// 旧四列键下"同秒 + 同 Key + 同模型、仅 logid 不同"的并发请求只留 1 行（缺陷复现）；
// 按新 schema 建 _v2 并执行仓库升级脚本（回灌 + 原子换名）后各自成行，且 NULL logid 归 0。
func TestTC06DetailUniqueKeyRebuild(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.UniqueDBName(t))
	dbName := common.NewTestDB(t, db, vars)

	// 1) 模拟存量环境：由仓库 DDL 逆向替换出"四列键 + logid 可空"的旧表
	newDDL := common.LoadSQL(t, common.SQLPath(t, "bfe_ai_request_log.sql"), vars)
	oldDDL := strings.Replace(newDDL,
		"UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model, logid)",
		"UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model)", 1)
	oldDDL = strings.Replace(oldDDL, "logid                   BIGINT NOT NULL", "logid                   BIGINT", 1)
	if oldDDL == newDDL || strings.Contains(oldDDL, "ai_requested_model, logid)") {
		t.Fatalf("旧 schema 生成失败：逆向替换未生效（仓库 DDL 的字面量可能已变）")
	}
	common.ExecIn(t, db, dbName, oldDDL)

	// 2) 哨兵行 A/B：同秒 + 同 hostid/apikey/model、仅 logid 不同 → 旧键应合并为 1 行
	const (
		sameSecond = "2026-09-15 10:00:00"
		dedupKey   = "dedup_key"
		sameModel  = "same-second-model"
	)
	common.ExecIn(t, db, dbName, fmt.Sprintf(
		"INSERT INTO bfe_ai_request_log (hostid, log_time, ai_apikey_id, ai_requested_model, logid, ai_total_tokens) VALUES "+
			"('it_host','%s','%s','%s',1001,10),('it_host','%s','%s','%s',1002,20)",
		sameSecond, dedupKey, sameModel, sameSecond, dedupKey, sameModel))

	// common.QueryText 每行以 tab + 换行结尾，比较前需去空白
	qt := func(query string) string { return strings.TrimSpace(common.QueryText(t, db, dbName, query)) }

	if got := qt(fmt.Sprintf("SELECT COUNT(*) FROM bfe_ai_request_log WHERE ai_apikey_id='%s'", dedupKey)); got != "1" {
		t.Fatalf("旧四列键下同秒并发行数 = %s，期望 1（缺陷复现失败：唯一键可能已含 logid）", got)
	}

	// 3) 哨兵行 C：历史 NULL logid 形态（另一 4 元组）
	common.ExecIn(t, db, dbName,
		"INSERT INTO bfe_ai_request_log (hostid, log_time, ai_apikey_id, ai_requested_model, logid) "+
			"VALUES ('it_host','2026-09-15 10:00:01','null_key','m-1',NULL)")

	// 4) 按新 schema 建 _v2（与 HOWTO §11.5 步骤 5.2 的 sed 同构），再执行仓库升级脚本（回灌 + 换名）
	v2SQL := strings.Replace(newDDL, "CREATE TABLE bfe_ai_request_log", "CREATE TABLE bfe_ai_request_log_v2", 1)
	common.ExecIn(t, db, dbName, v2SQL)
	common.ExecFileIn(t, db, dbName,
		common.SQLPath(t, "upgrade", "2026-10-13-issue-1417-detail-unique-key-logid", "bfe_ai_request_log_rebuild.sql"),
		vars)

	// 5) 唯一键已含 logid（去反引号/空格后做字面量比较，兼容 Doris 的 SHOW CREATE 渲染）
	create := common.QueryText(t, db, dbName, "SHOW CREATE TABLE bfe_ai_request_log")
	norm := strings.ReplaceAll(strings.ReplaceAll(create, "`", ""), " ", "")
	if !strings.Contains(norm, "UNIQUEKEY(hostid,log_time,ai_apikey_id,ai_requested_model,logid)") {
		t.Errorf("换名后唯一键未含 logid：\n%s", create)
	}

	// 6) logid 已收紧为 NOT NULL
	var nullable string
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT IS_NULLABLE FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='bfe_ai_request_log' AND COLUMN_NAME='logid'",
		dbName)).Scan(&nullable); err != nil {
		t.Fatalf("查询 logid 可空性失败: %v", err)
	}
	if !strings.EqualFold(nullable, "NO") {
		t.Errorf("换名后 logid IS_NULLABLE = %s，期望 NO", nullable)
	}

	// 7) 回灌不恢复已丢行：旧表里 A/B 已被合并为 1 行，重建只能带走这一行
	//    （历史不可回溯——这正是 issue 声明的边界：旧行 logid 为 0/已被覆盖）
	if got := qt(fmt.Sprintf("SELECT COUNT(*) FROM bfe_ai_request_log WHERE ai_apikey_id='%s'", dedupKey)); got != "1" {
		t.Errorf("重建后历史行数 = %s，期望 1（回灌沿用旧表已合并的结果，历史不可回溯）", got)
	}
	if got := qt("SELECT CAST(IFNULL(MAX(logid), -1) AS CHAR) FROM bfe_ai_request_log WHERE ai_apikey_id='null_key'"); got != "0" {
		t.Errorf("NULL logid 行重建后 logid = %s，期望 0（IFNULL 兜底）", got)
	}

	// 8) 新键的判别力：重建后写入同一批（同秒 + 同 Key + 同模型、不同 logid）→ 各自成行
	common.ExecIn(t, db, dbName, fmt.Sprintf(
		"INSERT INTO bfe_ai_request_log (hostid, log_time, ai_apikey_id, ai_requested_model, logid, ai_total_tokens) VALUES "+
			"('it_host','%s','%s','%s',1001,10),('it_host','%s','%s','%s',1002,20)",
		sameSecond, dedupKey, sameModel, sameSecond, dedupKey, sameModel))
	if got := qt(fmt.Sprintf("SELECT COUNT(*) FROM bfe_ai_request_log WHERE ai_apikey_id='%s'", dedupKey)); got != "2" {
		t.Errorf("重建后同秒并发行数 = %s，期望 2（各自成行，缺陷消除）", got)
	}

	// 9) 旧表（换名后名为 _v2）可删除，新表名查询不受影响
	common.ExecIn(t, db, dbName, "DROP TABLE bfe_ai_request_log_v2")
	if got := qt(fmt.Sprintf("SELECT COUNT(*) FROM bfe_ai_request_log WHERE ai_apikey_id IN ('%s','null_key')", dedupKey)); got != "3" {
		t.Errorf("删除旧表后新表查询异常：行数 = %s，期望 3", got)
	}
}

// ---------- 辅助函数 ----------

// loadDemo 读取 demo/normal_request.json（字段按 json.Number 保留原文）。
func loadDemo(t *testing.T) map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(common.DemoPath(t, "normal_request.json"))
	if err != nil {
		t.Fatalf("读取 demo 样例失败: %v", err)
	}
	var m map[string]interface{}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("解析 demo JSON 失败: %v", err)
	}
	return m
}

func jsonStr(t *testing.T, m map[string]interface{}, key string) string {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("demo 样例缺少字段 %s", key)
	}
	return fmt.Sprintf("%v", v)
}

// jsonBool01 将 demo 中 bool 字段转为 1/0（Doris BOOLEAN/VALUES 字面量）。
func jsonBool01(t *testing.T, m map[string]interface{}, key string) string {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("demo 样例缺少字段 %s", key)
	}
	b, ok := v.(bool)
	if !ok {
		t.Fatalf("demo 字段 %s 不是 bool: %T", key, v)
	}
	if b {
		return "1"
	}
	return "0"
}

// extractJobInsertSelect 从仓库 JOB SQL 中提取 INSERT..SELECT 主体（去掉 CREATE JOB 包装）。
func extractJobInsertSelect(t *testing.T, vars map[string]string) string {
	t.Helper()
	content := common.LoadSQL(t, common.SQLPath(t, "bfe_ai_metrics_1m_job.sql"), vars)
	idx := strings.Index(content, "INSERT INTO")
	if idx < 0 {
		t.Fatalf("JOB SQL 中未找到 INSERT INTO:\n%s", content)
	}
	return strings.TrimRight(content[idx:], " \t\n;")
}

// rewindow 将 JOB INSERT..SELECT 的 WHERE 窗口替换为固定字面量窗口（GROUP BY 及其余部分原样保留）。
func rewindow(t *testing.T, body, from, to string) string {
	t.Helper()
	whereIdx := strings.Index(body, "WHERE log_time >=")
	groupIdx := strings.Index(body, "GROUP BY")
	if whereIdx < 0 || groupIdx < 0 || groupIdx < whereIdx {
		t.Fatalf("JOB INSERT..SELECT 结构不符合预期:\n%s", body)
	}
	return body[:whereIdx] +
		fmt.Sprintf("WHERE log_time >= '%s' AND log_time < '%s'\n", from, to) +
		body[groupIdx:]
}

// ddlBetween 返回 text 中 start 与首个 end 之间的内容（用于 AGGREGATE KEY 列表解析）。
func ddlBetween(t *testing.T, text, start, end string) string {
	t.Helper()
	i := strings.Index(text, start)
	if i < 0 {
		t.Fatalf("DDL 中未找到 %q", start)
	}
	rest := text[i+len(start):]
	j := strings.Index(rest, end)
	if j < 0 {
		t.Fatalf("DDL 中 %q 未闭合", start)
	}
	return rest[:j]
}

// splitCSV 按逗号切分并去空白与反引号（适配 SHOW CREATE TABLE 的反引号列名）。
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), "`")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
