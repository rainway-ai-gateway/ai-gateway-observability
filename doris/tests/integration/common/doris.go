// Package common 提供 Doris 集成测试的公共 harness：
// 连接本机 Doris（默认 127.0.0.1:9030 root 无密码）、加载仓库 SQL 资产并做
// 与 setup.sh 一致的 ${VAR} 变量替换、在独立测试库中执行/清理。
//
// Doris 未启动时所有用例自动 Skip（不视为失败）。
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

// keepDB 报告是否保留测试库（DORIS_IT_KEEP_DB=1，调试用）。
func keepDB() bool {
	return os.Getenv("DORIS_IT_KEEP_DB") == "1"
}

// dsn 构建 go-sql-driver DSN（multiStatements 以支持多语句 SQL 文件）。
func dsn() string {
	host := envOr("DORIS_IT_HOST", DefaultHost)
	port := envOr("DORIS_IT_PORT", DefaultPort)
	user := envOr("DORIS_IT_USER", DefaultUser)
	pass := envOr("DORIS_IT_PASSWORD", "")
	cred := user
	if pass != "" {
		cred = user + ":" + pass
	}
	return fmt.Sprintf("%s@tcp(%s)/?multiStatements=true&timeout=10s&parseTime=false",
		cred, net.JoinHostPort(host, port))
}

// TestDB 连接 Doris；不可达时 Skip（提示启动方式）。
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
		t.Skipf("Doris 不可达 (%v)，跳过集成测试；请先执行: wsl bash /mnt/d/doris/bin/start-all.sh", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// FindDorisDir 从当前工作目录向上查找 doris/ 目录（以 sqls/bfe_ai_request_log.sql 为标记）。
func FindDorisDir(t *testing.T) string {
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
			t.Fatalf("未找到 doris/ 目录（自 %s 向上）", wd)
		}
	}
}

// SQLPath 返回仓库 SQL 资产路径，elems 为相对 sqls/ 的子路径。
func SQLPath(t *testing.T, elems ...string) string {
	t.Helper()
	return filepath.Join(append([]string{FindDorisDir(t), "sqls"}, elems...)...)
}

// DemoPath 返回 demo JSON 样例路径。
func DemoPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(FindDorisDir(t), "demo", name)
}

// SubstituteVars 替换 SQL 内容中的 ${VAR}（与 setup.sh 的 sed 行为一致）。
func SubstituteVars(content string, vars map[string]string) string {
	for k, v := range vars {
		content = strings.ReplaceAll(content, "${"+k+"}", v)
	}
	return content
}

// DefaultVars 返回常用变量（INIT_PARTITION_DATE 取明天，保证写入当刻数据落入初始分区）。
func DefaultVars(dbName string) map[string]string {
	return map[string]string{
		"DORIS_DATABASE":       dbName,
		"INIT_PARTITION_DATE":  time.Now().Add(24 * time.Hour).Format("2006-01-02"),
		"KAFKA_BROKER_LIST":    "127.0.0.1:9092",
		"KAFKA_TOPIC":          "bfe_ai_log_it",
		"KAFKA_GROUP_ID":       "doris_bfe_ai_log_it",
		"KAFKA_CLIENT_ID":      "doris_bfe_ai_log_it",
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

// ColumnTypes 返回 information_schema.columns 中指定列的 DATA_TYPE(+长度) 映射。
// 值为形如 "varchar(16)"、"boolean"、"double"、"bigint" 的小写类型描述。
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

// UniqueDBName 生成独立测试库名（≤64 字符，含纳秒后缀防并行冲突）。
func UniqueDBName(t *testing.T) string {
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

// NewTestDB 创建独立测试库并注册清理；返回库名。库名取自 bfe_observability.sql（CREATE DATABASE IF NOT EXISTS）。
func NewTestDB(t *testing.T, db *sql.DB, vars map[string]string) string {
	t.Helper()
	dbName := vars["DORIS_DATABASE"]
	DropDB(t, db, dbName)
	ExecFileIn(t, db, "", SQLPath(t, "bfe_observability.sql"), vars)
	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE SCHEMA_NAME=?", dbName).Scan(&cnt); err != nil || cnt != 1 {
		t.Fatalf("测试库创建失败: db=%q count=%d err=%v", dbName, cnt, err)
	}
	t.Cleanup(func() {
		if keepDB() {
			t.Logf("DORIS_IT_KEEP_DB=1，保留测试库 %s", dbName)
			return
		}
		DropDB(t, db, dbName)
	})
	return dbName
}

// insertJobName 与 sqls/bfe_ai_metrics_1m_job.sql 中的 CREATE JOB 保持一致。
// 注意：Doris INSERT JOB 名全局唯一（不按库隔离，HOWTO §5），测试需先清理残留任务。
const insertJobName = "bfe_ai_metrics_1m_job"

// DropInsertJob 删除全局 INSERT JOB（幂等，不存在时忽略）。
func DropInsertJob(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("DROP JOB WHERE JobName = '" + insertJobName + "'")
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "no job") {
		t.Logf("清理 INSERT JOB %s 失败（可手动 DROP JOB WHERE JobName='%s'）: %v",
			insertJobName, insertJobName, err)
	}
}

// SetupFreshInstall 执行全新安装路径（建明细表/聚合表，可选 INSERT JOB）。
func SetupFreshInstall(t *testing.T, db *sql.DB, dbName string, vars map[string]string, withJob bool) {
	t.Helper()
	ExecFileIn(t, db, dbName, SQLPath(t, "bfe_ai_request_log.sql"), vars)
	ExecFileIn(t, db, dbName, SQLPath(t, "bfe_ai_metrics_1m.sql"), vars)
	if withJob {
		DropInsertJob(t, db) // JOB 名全局唯一，先清理历史残留
		ExecFileIn(t, db, dbName, SQLPath(t, "bfe_ai_metrics_1m_job.sql"), vars)
		t.Cleanup(func() { DropInsertJob(t, db) })
	}
}

func truncate(s string) string {
	const max = 2000
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n... (truncated)"
}
