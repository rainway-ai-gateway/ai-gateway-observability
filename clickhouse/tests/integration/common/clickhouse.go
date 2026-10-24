// Package common 提供 ClickHouse 集成测试的公共 harness：
// 连接本机 ClickHouse（默认 127.0.0.1:9000 default 用户无密码）、加载仓库 SQL 资产并做
// 与 setup.sh 一致的 ${VAR} 变量替换、在独立测试库中执行/清理。
//
// ClickHouse 未启动时所有用例自动 Skip（不视为失败，CI 友好）。
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

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	DefaultHost     = "127.0.0.1"
	DefaultPort     = "9000"
	DefaultUser     = "default"
	DefaultDatabase = "bfe_observability_test" // 测试库基名（CLICKHOUSE_DATABASE）
)

// envOr 返回环境变量值，未设置时返回默认值。
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// keepDB 报告是否保留测试库（CLICKHOUSE_IT_KEEP_DB=1，调试用）。
func keepDB() bool {
	return os.Getenv("CLICKHOUSE_IT_KEEP_DB") == "1"
}

// dsn 构建 clickhouse-go stdlib DSN（native TCP）。
// 固定落在 default 库：clickhouse-go 要求 DSN 中的库存在，default 恒存在；
// 测试对象全部建在独立测试库中（SQL 资产以 ${CLICKHOUSE_DATABASE} 全限定，无需 USE）。
func dsn() string {
	host := envOr("CLICKHOUSE_HOST", DefaultHost)
	port := envOr("CLICKHOUSE_PORT", DefaultPort)
	user := envOr("CLICKHOUSE_USER", DefaultUser)
	pass := envOr("CLICKHOUSE_PASSWORD", "")
	cred := user
	if pass != "" {
		cred = user + ":" + pass
	}
	return fmt.Sprintf("clickhouse://%s@%s/default?dial_timeout=10s",
		cred, net.JoinHostPort(host, port))
}

// TestDB 连接 ClickHouse；不可达时 Skip（提示启动方式）。
func TestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("clickhouse", dsn())
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
		t.Skipf("ClickHouse 不可达 (%v)，跳过集成测试；请先执行: wsl bash /mnt/d/clickhouse/bin/start.sh", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// FindClickHouseDir 从当前工作目录向上查找 clickhouse/ 目录（以 sqls/bfe_observability.sql 为标记）。
func FindClickHouseDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取工作目录失败: %v", err)
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "sqls", "bfe_observability.sql")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("未找到 clickhouse/ 目录（自 %s 向上）", wd)
		}
	}
}

// SQLPath 返回仓库 SQL 资产路径，elems 为相对 sqls/ 的子路径。
func SQLPath(t *testing.T, elems ...string) string {
	t.Helper()
	return filepath.Join(append([]string{FindClickHouseDir(t), "sqls"}, elems...)...)
}

// DemoPath 返回 demo JSON 样例路径。
func DemoPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(FindClickHouseDir(t), "demo", name)
}

// SubstituteVars 替换 SQL 内容中的 ${VAR}（与 setup.sh 的 sed 行为一致）。
func SubstituteVars(content string, vars map[string]string) string {
	for k, v := range vars {
		content = strings.ReplaceAll(content, "${"+k+"}", v)
	}
	return content
}

// DefaultVars 返回常用变量；Kafka 变量可用同名环境变量覆盖（与 setup.conf 一致）。
// 测试库名由 NewDBName 生成（CLICKHOUSE_DATABASE 为基名，默认 bfe_observability_test）。
func DefaultVars(dbName string) map[string]string {
	return map[string]string{
		"CLICKHOUSE_DATABASE": dbName,
		"KAFKA_BROKER_LIST":   envOr("KAFKA_BROKER_LIST", "127.0.0.1:9092"),
		"KAFKA_TOPIC":         envOr("KAFKA_TOPIC", "bfe_ai_log_it"),
		"KAFKA_GROUP_ID":      envOr("KAFKA_GROUP_ID", "clickhouse_bfe_ai_log_it"),
	}
}

// NewDBName 生成独立测试库名：以 CLICKHOUSE_DATABASE（默认 bfe_observability_test）为基名，
// 追加用例名与纳秒后缀防并行冲突。
func NewDBName(t *testing.T) string {
	t.Helper()
	base := envOr("CLICKHOUSE_DATABASE", DefaultDatabase)
	name := strings.ToLower(t.Name())
	name = strings.NewReplacer("/", "_", "-", "_", "testtc", "tc").Replace(name)
	if len(name) > 40 {
		name = name[:40]
	}
	return fmt.Sprintf("%s_it_%s_%d", base, name, time.Now().UnixNano()%1000000)
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

// ExecIn 执行 SQL 内容。与 doris-it 不同，ClickHouse 侧不需要 USE 前缀：
// 仓库 SQL 资产均以 ${CLICKHOUSE_DATABASE} 全限定，且六份资产均为单语句
// （无需 multiStatements），测试内临时 SQL 同样全限定表名。
func ExecIn(t *testing.T, db *sql.DB, content string) {
	t.Helper()
	_, err := db.Exec(content)
	if err != nil {
		t.Fatalf("SQL 执行失败: %v\n---- SQL ----\n%s", err, truncate(content))
	}
}

// ExecFileIn 加载并执行仓库 SQL 资产文件。
func ExecFileIn(t *testing.T, db *sql.DB, sqlFile string, vars map[string]string) {
	t.Helper()
	ExecIn(t, db, LoadSQL(t, sqlFile, vars))
}

// QueryText 执行查询并把全部结果按行拼接为文本（用于 SHOW */system 表的子串断言）。
func QueryText(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("查询失败: %v\nSQL: %s", err, query)
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

// RowStrings 查询单行并按列返回字符串切片（NULL → 空串且不占位丢失），用于逐列断言。
// 查不到行时 Fatalf。
func RowStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("查询失败: %v\nSQL: %s", err, query)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("读取列名失败: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("查询无结果行: %s", query)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("扫描行失败: %v", err)
	}
	if rows.Next() {
		t.Fatalf("查询返回多行，期望单行: %s", query)
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		if v.Valid {
			out[i] = v.String
		}
	}
	return out
}

// ColumnTypes 返回 system.columns 中指定列的 ClickHouse 类型映射（形如 "String"、"Nullable(Float64)"）。
func ColumnTypes(t *testing.T, db *sql.DB, dbName, table string, columns []string) map[string]string {
	t.Helper()
	query := fmt.Sprintf(`SELECT name, type FROM system.columns
WHERE database='%s' AND table='%s' AND name IN ('%s')`,
		dbName, table, strings.Join(columns, "','"))
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("查询 system.columns 失败: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("扫描 system.columns 行失败: %v", err)
		}
		out[name] = typ
	}
	return out
}

// Objects 返回库内全部表与物化视图（ClickHouse 中 MV 与表同列 system.tables）的 name → engine 映射。
func Objects(t *testing.T, db *sql.DB, dbName string) map[string]string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(
		`SELECT name, engine FROM system.tables WHERE database='%s'`, dbName))
	if err != nil {
		t.Fatalf("查询 system.tables 失败: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			t.Fatalf("扫描 system.tables 行失败: %v", err)
		}
		out[name] = engine
	}
	return out
}

// WaitFor 轮询 cond 直至其为 true，超时 Fatal（desc 用于报错定位）。
// 用于等待 Kafka 消费等异步链路，替代固定 sleep。
func WaitFor(t *testing.T, desc string, deadline, interval time.Duration, cond func() bool) {
	t.Helper()
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("等待超时（%s）：%s", deadline, desc)
}

// DropDB 删除测试库（级联删除库内表与 MV）。
func DropDB(t *testing.T, db *sql.DB, dbName string) {
	t.Helper()
	_, err := db.Exec("DROP DATABASE IF EXISTS " + dbName)
	if err != nil {
		t.Logf("删除测试库 %s 失败（可手动清理）: %v", dbName, err)
	}
}

// NewTestDB 创建独立测试库（setup.sh Step 1，执行仓库 bfe_observability.sql）并注册清理。
// 返回库名（即 vars["CLICKHOUSE_DATABASE"]）。
func NewTestDB(t *testing.T, db *sql.DB, vars map[string]string) string {
	t.Helper()
	dbName := vars["CLICKHOUSE_DATABASE"]
	DropDB(t, db, dbName)
	ExecFileIn(t, db, SQLPath(t, "bfe_observability.sql"), vars)
	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM system.databases WHERE name=?", dbName).Scan(&cnt); err != nil || cnt != 1 {
		t.Fatalf("测试库创建失败: db=%q count=%d err=%v", dbName, cnt, err)
	}
	t.Cleanup(func() {
		if keepDB() {
			t.Logf("CLICKHOUSE_IT_KEEP_DB=1，保留测试库 %s", dbName)
			return
		}
		DropDB(t, db, dbName)
	})
	return dbName
}

// SetupFreshInstall 按 setup.sh 的 Step 2–6 创建对象（Step 1 建库由 NewTestDB 完成，同一文件）。
// withKafka=false 时不建 Kafka 表与消费 MV（与导入链路无关的用例使用，
// 避免无 Kafka 环境下 Kafka 引擎后台重连的日志噪音）。
func SetupFreshInstall(t *testing.T, db *sql.DB, vars map[string]string, withKafka bool) {
	t.Helper()
	ExecFileIn(t, db, SQLPath(t, "bfe_ai_request_log.sql"), vars)
	if withKafka {
		ExecFileIn(t, db, SQLPath(t, "bfe_ai_log_kafka.sql"), vars)
		ExecFileIn(t, db, SQLPath(t, "bfe_ai_log_load_mv.sql"), vars)
	}
	ExecFileIn(t, db, SQLPath(t, "bfe_ai_metrics_1m.sql"), vars)
	ExecFileIn(t, db, SQLPath(t, "bfe_ai_metrics_1m_mv.sql"), vars)
}

func truncate(s string) string {
	const max = 2000
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n... (truncated)"
}
