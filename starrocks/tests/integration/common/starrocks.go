// Package common 提供 StarRocks 集成测试的公共 harness：
// 连接本机 StarRocks FE 的 MySQL 协议端口（默认 127.0.0.1:9030 root 无密码）、
// 加载仓库 SQL 资产并做与 setup.sh 一致的 ${VAR} 变量替换、在独立测试库中执行/清理。
//
// StarRocks 未启动时所有用例自动 Skip（不视为失败）。
//
// 与 doris-it 同构（同为 MySQL 协议 + go-sql-driver），差异点：
//   - 库名变量为 ${STARROCKS_DATABASE}；
//   - 无 INSERT JOB（分钟聚合为异步物化视图，SetupFreshInstall 的 withAgg 建 MV）；
//   - Routine Load 的 SHOW/RESUME 辅助（TC02 真实消费链路使用）；
//   - go driver 单包多语句发送（COM_QUERY 服务端切分），不受 mysql CLI
//     "注释单独成包"问题影响（见 starrocks/docs/modifications §4.5）。
package common

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	DefaultHost = "127.0.0.1"
	DefaultPort = "9030"
	DefaultUser = "root"
)

// envOr 返回环境变量值，未设置时返回默认值。
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// keepDB 报告是否保留测试库（STARROCKS_IT_KEEP_DB=1，调试用）。
func keepDB() bool {
	return os.Getenv("STARROCKS_IT_KEEP_DB") == "1"
}

// dsn 构建 go-sql-driver DSN（multiStatements 以支持多语句 SQL 文件，同 doris-it）。
func dsn() string {
	host := envOr("STARROCKS_IT_HOST", DefaultHost)
	port := envOr("STARROCKS_IT_PORT", DefaultPort)
	user := envOr("STARROCKS_IT_USER", DefaultUser)
	pass := envOr("STARROCKS_IT_PASSWORD", "")
	cred := user
	if pass != "" {
		cred = user + ":" + pass
	}
	return fmt.Sprintf("%s@tcp(%s)/?multiStatements=true&timeout=10s&parseTime=false",
		cred, net.JoinHostPort(host, port))
}

// TestDB 连接 StarRocks FE；不可达时 Skip（提示启动方式）。
func TestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("构建 DSN 失败: %v", err)
	}
	pingDone := make(chan error, 1)
	go func() { pingDone <- db.Ping() }()
	select {
	case err = <-pingDone:
	case <-time.After(5 * time.Second):
		err = fmt.Errorf("ping 超时")
	}
	if err != nil {
		_ = db.Close()
		t.Skipf("StarRocks 不可达 (%v)，跳过集成测试；请先执行: wsl bash ~/starrocks/bin/start-all.sh", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// FindStarRocksDir 从当前工作目录向上查找 starrocks/ 目录（以 sqls/bfe_ai_request_log.sql 为标记）。
func FindStarRocksDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取工作目录失败: %v", err)
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "sqls", "bfe_ai_request_log.sql")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("未找到 starrocks/ 目录（自 %s 向上）", wd)
		}
	}
}

// SQLPath 返回仓库 SQL 资产路径，elems 为相对 sqls/ 的子路径。
func SQLPath(t *testing.T, elems ...string) string {
	t.Helper()
	return filepath.Join(append([]string{FindStarRocksDir(t), "sqls"}, elems...)...)
}

// DemoPath 返回 demo JSON 样例路径（starrocks/demo/）。
func DemoPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(FindStarRocksDir(t), "demo", name)
}

// SubstituteVars 替换 SQL 内容中的 ${VAR}（与 setup.sh 的 sed 行为一致）。
func SubstituteVars(content string, vars map[string]string) string {
	for k, v := range vars {
		content = strings.ReplaceAll(content, "${"+k+"}", v)
	}
	return content
}

// DefaultVars 返回常用变量（INIT_PARTITION_DATE 取明天，保证写入当刻数据落入既有分区）。
// KAFKA 默认值指向本机 WSL2 Kafka（同 clickhouse-it），可用环境变量覆盖。
func DefaultVars(dbName string) map[string]string {
	return map[string]string{
		"STARROCKS_DATABASE":  dbName,
		"INIT_PARTITION_DATE": time.Now().Add(24 * time.Hour).Format("2006-01-02"),
		"KAFKA_BROKER_LIST":   envOr("KAFKA_BROKER_LIST", "127.0.0.1:9092"),
		"KAFKA_TOPIC":         envOr("KAFKA_TOPIC", "bfe_ai_log_it"),
		"KAFKA_GROUP_ID":      envOr("KAFKA_GROUP_ID", "starrocks_bfe_ai_log_it"),
		"KAFKA_CLIENT_ID":     envOr("KAFKA_CLIENT_ID", "starrocks_bfe_ai_log_it"),
	}
}

// LoadSQL 读取 SQL 文件并做变量替换。
func LoadSQL(t *testing.T, path string, vars map[string]string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 SQL 文件失败 %s: %v", path, err)
	}
	return SubstituteVars(string(raw), vars)
}

// ExecIn 在指定库中执行 SQL 内容（自动加 USE 前缀，单连接执行，避免连接池 USE 漂移）。
// dbName 为空时不加 USE 前缀。
func ExecIn(t *testing.T, db *sql.DB, dbName string, content string) {
	t.Helper()
	if dbName != "" {
		content = "USE " + dbName + ";\n" + content
	}
	_, err := db.Exec(content)
	if err != nil {
		t.Fatalf("SQL 执行失败 [%s]: %v\n---- SQL ----\n%s", dbName, err, truncate(content))
	}
}

// ExecE 同 ExecIn 但返回错误（用于幂等性/负路径断言）。
func ExecE(db *sql.DB, dbName string, content string) error {
	if dbName != "" {
		content = "USE " + dbName + ";\n" + content
	}
	_, err := db.Exec(content)
	return err
}

// ExecFileIn 加载并执行仓库 SQL 资产文件。
func ExecFileIn(t *testing.T, db *sql.DB, dbName string, sqlFile string, vars map[string]string) {
	t.Helper()
	ExecIn(t, db, dbName, LoadSQL(t, sqlFile, vars))
}

// QueryText 执行查询并把全部结果按行拼接为文本（用于 SHOW */SHOW CREATE TABLE 的子串断言）。
// dbName 为空时不加 USE 前缀。
func QueryText(t *testing.T, db *sql.DB, dbName string, query string) string {
	t.Helper()
	if dbName != "" {
		query = "USE " + dbName + ";\n" + query
	}
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("查询失败 [%s]: %v\nSQL: %s", dbName, err, query)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("读取列名失败: %v", err)
	}
	var sb strings.Builder
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("扫描行失败: %v", err)
		}
		for _, v := range vals {
			if v.Valid {
				sb.WriteString(v.String)
			}
			sb.WriteByte('\t')
		}
		sb.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历结果失败: %v", err)
	}
	return sb.String()
}

// RowStrings 执行查询并返回首行（各列以字符串形式，NULL 为空串）。
func RowStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("查询失败: %v\nSQL: %s", err, query)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("查询无结果行: %s", query)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("读取列名失败: %v", err)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("扫描行失败: %v", err)
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		if v.Valid {
			out[i] = v.String
		}
	}
	return out
}

// WaitFor 轮询直至 cond 成立（断言失败即 t.Fatalf；desc 用于失败信息）。
func WaitFor(t *testing.T, desc string, deadline, interval time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("等待超时: %s（deadline=%s）", desc, deadline)
}

// ColumnTypes 返回 information_schema.columns 中指定列的 DATA_TYPE(+长度) 映射。
// 值为形如 "varchar(16)"、"boolean"、"double"、"bigint" 的小写类型描述。
// 对物化视图同样适用（SR 的 MV 以 VIEW 形态出现在 information_schema 中）。
func ColumnTypes(t *testing.T, db *sql.DB, dbName, table string, columns []string) map[string]string {
	t.Helper()
	query := fmt.Sprintf(`SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM information_schema.columns
WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME IN ('%s')`,
		dbName, table, strings.Join(columns, "','"))
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("查询 information_schema 失败: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, dataType string
		var maxLen sql.NullInt64
		if err := rows.Scan(&name, &dataType, &maxLen); err != nil {
			t.Fatalf("扫描 information_schema 行失败: %v", err)
		}
		desc := strings.ToLower(dataType)
		if maxLen.Valid {
			desc = fmt.Sprintf("%s(%d)", desc, maxLen.Int64)
		}
		out[name] = desc
	}
	return out
}

// ColumnTypesDesc 返回 `DESC <db>.<table>` 的列名 → 声明类型映射（小写，形如 "boolean"、
// "varchar(128)"、"bigint"）。
//
// 与 ColumnTypes 的差别：information_schema 把 BOOLEAN 呈现为 tinyint(1)，与真正的
// TINYINT 无法区分——要断言"某布尔列确实声明为 BOOLEAN"必须走 DESC。TINYINT 声明会让
// JSON 布尔静默落 NULL（issue #10），故布尔列的类型断言一律用本函数。
func ColumnTypesDesc(t *testing.T, db *sql.DB, dbName, table string) map[string]string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("DESC %s.%s", dbName, table))
	if err != nil {
		t.Fatalf("DESC %s.%s 失败: %v", dbName, table, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("读取 DESC 列名失败: %v", err)
	}
	out := map[string]string{}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("扫描 DESC 行失败: %v", err)
		}
		if vals[0].Valid {
			out[vals[0].String] = strings.ToLower(vals[1].String)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历 DESC 结果失败: %v", err)
	}
	return out
}

// ColumnCount 返回指定表（或 MV）的列数。
func ColumnCount(t *testing.T, db *sql.DB, dbName, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns
WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, dbName, table).Scan(&n); err != nil {
		t.Fatalf("查询列数失败 %s.%s: %v", dbName, table, err)
	}
	return n
}

// Objects 返回 information_schema.tables 中对象名 → TABLE_TYPE 映射
// （明细表为 BASE TABLE，异步物化视图为 VIEW）。
func Objects(t *testing.T, db *sql.DB, dbName string) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.tables
WHERE TABLE_SCHEMA=?`, dbName)
	if err != nil {
		t.Fatalf("查询 information_schema.tables 失败: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("扫描 information_schema.tables 行失败: %v", err)
		}
		out[name] = typ
	}
	return out
}

// NewDBName 生成独立测试库名（≤64 字符，含纳秒后缀防并行冲突）。
func NewDBName(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(t.Name())
	name = strings.NewReplacer("/", "_", "-", "_", "testtc", "tc").Replace(name)
	if len(name) > 40 {
		name = name[:40]
	}
	return fmt.Sprintf("bfe_observability_it_%s_%d", name, time.Now().UnixNano()%1000000)
}

// DropDB 删除测试库（FORCE 级联删表）。
func DropDB(t *testing.T, db *sql.DB, dbName string) {
	t.Helper()
	_, err := db.Exec("DROP DATABASE IF EXISTS " + dbName + " FORCE")
	if err != nil {
		t.Logf("删除测试库 %s 失败（可手动清理）: %v", dbName, err)
	}
}

// NewTestDB 创建独立测试库并注册清理；返回库名。库名取自 vars["STARROCKS_DATABASE"]
//（bfe_observability.sql 中 CREATE DATABASE IF NOT EXISTS）。
func NewTestDB(t *testing.T, db *sql.DB, vars map[string]string) string {
	t.Helper()
	dbName := vars["STARROCKS_DATABASE"]
	DropDB(t, db, dbName)
	ExecFileIn(t, db, "", SQLPath(t, "bfe_observability.sql"), vars)
	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE SCHEMA_NAME=?", dbName).Scan(&cnt); err != nil || cnt != 1 {
		t.Fatalf("测试库创建失败: db=%q count=%d err=%v", dbName, cnt, err)
	}
	t.Cleanup(func() {
		if keepDB() {
			t.Logf("STARROCKS_IT_KEEP_DB=1，保留测试库 %s", dbName)
			return
		}
		DropDB(t, db, dbName)
	})
	return dbName
}

// SetupFreshInstall 执行全新安装路径（建明细表 / 物化视图 / Routine Load，
// 与 setup.sh 的 Step 2–4 同序；建库由 NewTestDB 完成）。
func SetupFreshInstall(t *testing.T, db *sql.DB, dbName string, vars map[string]string, withMV, withRoutineLoad bool) {
	t.Helper()
	ExecFileIn(t, db, dbName, SQLPath(t, "bfe_ai_request_log.sql"), vars)
	if withMV {
		ExecFileIn(t, db, dbName, SQLPath(t, "bfe_ai_metrics_1m.sql"), vars)
	}
	if withRoutineLoad {
		ExecFileIn(t, db, dbName, SQLPath(t, "bfe_ai_log_load_routine.sql"), vars)
	}
}

// routineLoadName 与 sqls/bfe_ai_log_load_routine.sql 中的 CREATE ROUTINE LOAD 保持一致。
const routineLoadName = "bfe_ai_log_load"

// routineLoadLine 在 SHOW ALL ROUTINE LOAD 结果中定位本库指定 job 的数据行（不存在返回空串）。
// 列序：Id, Name, CreateTime, PauseTime, EndTime, DbName, TableName, State, ...
func routineLoadLine(t *testing.T, db *sql.DB, dbName string) string {
	t.Helper()
	text := QueryText(t, db, dbName, "SHOW ALL ROUTINE LOAD")
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) > 7 && fields[1] == routineLoadName && fields[5] == dbName {
			return line
		}
	}
	return ""
}

// RoutineLoadExists 报告指定库下 Routine Load 是否存在。
func RoutineLoadExists(t *testing.T, db *sql.DB, dbName string) bool {
	t.Helper()
	return routineLoadLine(t, db, dbName) != ""
}

// ResumeRoutineLoadIfPaused 恢复 Routine Load（仅当数据行状态含 PAUSED 时执行，
// 幂等）。本机 9p 盘上 Kafka 元数据获取偶发失败会使 job 创建即 PAUSED。
func ResumeRoutineLoadIfPaused(t *testing.T, db *sql.DB, dbName string) {
	t.Helper()
	if line := routineLoadLine(t, db, dbName); line != "" && strings.Contains(line, "PAUSED") {
		ExecIn(t, db, dbName, "RESUME ROUTINE LOAD FOR "+routineLoadName)
		t.Logf("Routine Load %s 曾处于 PAUSED，已执行 RESUME", routineLoadName)
	}
}

// MVActive 报告物化视图是否 is_active（SHOW MATERIALIZED VIEWS 文本断言）。
func MVActive(t *testing.T, db *sql.DB, dbName, mvName string) bool {
	t.Helper()
	text := QueryText(t, db, dbName, "SHOW MATERIALIZED VIEWS")
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, mvName) && strings.Contains(line, "true") {
			return true
		}
	}
	return false
}

func truncate(s string) string {
	const max = 2000
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n... (truncated)"
}
