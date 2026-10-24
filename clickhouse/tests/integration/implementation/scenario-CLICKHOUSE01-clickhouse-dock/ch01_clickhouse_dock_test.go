// 场景 CLICKHOUSE01：ClickHouse 对接（报表存储新引擎，等价 Doris 既有链路）。
//
// 被测对象是本仓库真实 SQL 资产（clickhouse/sqls/，仅做 ${VAR} 变量替换），
// 覆盖五个测试例（详见 ../../测试设计文档/scenario-CLICKHOUSE01-clickhouse对接/场景说明.md）：
//
// TC01 全新安装：按 setup.sh 顺序应用六份 SQL，断言 5 个对象存在，重复应用幂等
// TC02 消费打平：真实 Kafka → Kafka 引擎 → 消费 MV，断言 level 十列 / 限流三列 /
//
//	UTC 墙钟 / UInt64 logid 原值 / Nullable 语义
//
// TC03 明细→聚合口径：聚合 MV 随插入同步触发，GROUP BY + sum() 兜底断言关键指标
// TC04 存量升级 ALTER：明细表加列成功，聚合表/聚合 MV 不自动携带新列（行为记录）
// TC05 重建模式：DROP 聚合 MV + 聚合表 → 按仓库 DDL 重建 → INSERT..SELECT 明细重算，口径不变
package ch01_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"clickhouse-it/common"
)

// 五个被测对象及其引擎（system.tables 中表与 MV 同列）。
var wantObjects = map[string]string{
	"bfe_ai_request_log":   "MergeTree",
	"bfe_ai_log_kafka":     "Kafka",
	"bfe_ai_log_load_mv":   "MaterializedView",
	"bfe_ai_metrics_1m":    "SummingMergeTree",
	"bfe_ai_metrics_1m_mv": "MaterializedView",
}

// setupOrder 为 setup.sh 的六步顺序（建库由 NewTestDB 完成，此处为 Step 2–6 的重复应用顺序）。
var setupOrder = []string{
	"bfe_observability.sql",
	"bfe_ai_request_log.sql",
	"bfe_ai_log_kafka.sql",
	"bfe_ai_log_load_mv.sql",
	"bfe_ai_metrics_1m.sql",
	"bfe_ai_metrics_1m_mv.sql",
}

// TestTC01FreshInstall 全新安装 schema：按序建 5 对象，重复应用幂等（IF NOT EXISTS）。
func TestTC01FreshInstall(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars) // Step 1：建库（bfe_observability.sql）
	common.SetupFreshInstall(t, db, vars, true /*withKafka*/)

	// 1) 5 个对象存在且引擎正确（含建库共六份 SQL，Step 2–6 由 SetupFreshInstall 执行）
	objects := common.Objects(t, db, dbName)
	for name, wantEngine := range wantObjects {
		engine, ok := objects[name]
		if !ok {
			t.Errorf("缺少对象 %s（库 %s 现有对象: %v）", name, dbName, objects)
			continue
		}
		if engine != wantEngine {
			t.Errorf("对象 %s 引擎 = %s，期望 %s", name, engine, wantEngine)
		}
	}
	if len(objects) != len(wantObjects) {
		t.Errorf("库 %s 对象数 = %d，期望 %d（实际: %v）", dbName, len(objects), len(wantObjects), objects)
	}

	// 2) 重复应用全部六份 SQL 幂等（IF NOT EXISTS / CREATE DATABASE IF NOT EXISTS）
	for _, f := range setupOrder {
		common.ExecFileIn(t, db, common.SQLPath(t, f), vars)
	}
	objects = common.Objects(t, db, dbName)
	for name := range wantObjects {
		if _, ok := objects[name]; !ok {
			t.Errorf("重复应用后缺少对象 %s", name)
		}
	}
	if len(objects) != len(wantObjects) {
		t.Errorf("重复应用后对象数 = %d，期望 %d（实际: %v）", len(objects), len(wantObjects), objects)
	}
}

// TestTC02KafkaConsumptionFlatten 消费打平：真实 Kafka 生产三份 demo 样例，
// 经 Kafka 引擎表 + 消费 MV 落入明细表，断言打平列与类型语义。
//
// Kafka 不可达时 Skip（RequireKafka）。降级路径（不自动执行，供无 Kafka 环境手工验证）：
// 按 system.columns 中 bfe_ai_log_kafka 的结构建临时 MergeTree 镜像表（列名带反引号，
// Nested 物理子列为 `req_headers.key` 等带点名字），demo JSON 压缩单行后以
// JSONEachRow + input_format_skip_unknown_fields=1 插入镜像表，再取仓库
// bfe_ai_log_load_mv.sql 的 SELECT 主体（去注释与 CREATE/TO 头、FROM 换成镜像表）执行
// INSERT INTO bfe_ai_request_log SELECT ...——与真实消费路径等价（同样触发消费 MV 的打平逻辑）。
func TestTC02KafkaConsumptionFlatten(t *testing.T) {
	db := common.TestDB(t)
	brokers := common.RequireKafka(t) // Kafka 不可达时 Skip（真实消费链路的前提）

	vars := common.DefaultVars(common.NewDBName(t))
	// 独立临时 topic（KAFKA_TOPIC 为基名，追加纳秒后缀）：全新 topic + 全新消费组，
	// 避免历史位点/残留消息跨运行污染断言
	vars["KAFKA_TOPIC"] = fmt.Sprintf("%s_%d", vars["KAFKA_TOPIC"], time.Now().UnixNano()%1000000)
	topic := vars["KAFKA_TOPIC"]
	common.EnsureTopic(t, brokers, topic)
	// 测试 topic 不做删除：本机 Kafka 数据目录在 /mnt/d（9p），删除 topic 会使 broker
	// log dir failed 宕机（见 common.DeleteTopic 缺失说明）；topic 命名唯一、仅 3 条消息

	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, vars, true /*withKafka*/)

	// 生产三份 demo 样例（值即断言期望来源，兼校验 demo 文件与 DDL 一致性）。
	// 无需等待消费者就绪：实测 ClickHouse Kafka 引擎新消费组从 earliest 起点消费
	//（表/MV 创建前已存在的消息也会被消费），生产与消费就绪的时序不影响送达。
	// demo 的 timestamp 为 2026-08-24，超出明细表 TTL 7 天窗口，直接消费会写入即过期，
	// 故仅将 timestamp 改写为当前时刻（其余字段保持 demo 原值）后生产；
	// log_time 断言以改写后的 timestamp 为准（由 want 行内的 epoch 记录）。
	base := time.Now()
	demoTs := map[string]int64{
		"10602749765076101032": base.Add(-20 * time.Second).Unix(),
		"8877700219231856663":  base.Add(-10 * time.Second).Unix(),
		"12345678901234567890": base.Unix(),
	}
	payloads := make([][]byte, 0, len(demoTs))
	for _, d := range []string{"normal_request.json", "rate_limit.json", "auth_reject.json"} {
		payloads = append(payloads, demoPayload(t, d, demoTs[demoLogid(t, d)]))
	}
	common.ProduceMessages(t, brokers, topic, payloads...)

	// 三个样例的 logid（UInt64 原值，含超出 Int64 上限的取值）
	want := map[string]flattenExpect{
		"10602749765076101032": {
			levels:     [10]string{"dep0", "rd", "dep2", "teama", "dep3", "yyx", "", "", "", ""},
			rateLimit:  [3]string{"", "", ""},
			confIsNull: "0", conf: "0.95",
		},
		"8877700219231856663": {
			levels:     [10]string{"dep0", "rd", "dep2", "teama", "", "", "", "", "", ""},
			rateLimit:  [3]string{"rlp-0002", "tpm", "tpm1"},
			confIsNull: "1", conf: "",
		},
		"12345678901234567890": {
			levels:     [10]string{"", "", "", "", "", "", "", "", "", ""},
			rateLimit:  [3]string{"", "", ""},
			confIsNull: "1", conf: "",
		},
	}
	var logids []string
	for logid := range want {
		logids = append(logids, logid)
	}

	// Kafka 消费有秒级延迟（本机 9p 盘上消费组加入与分区分配可达 1-2 分钟）：
	// 轮询至三个 logid 全部落明细表（deadline 放宽到 240s）
	common.WaitFor(t, "三条样例消息落入明细表", 240*time.Second, time.Second, func() bool {
		var n int
		if err := db.QueryRow(fmt.Sprintf(
			`SELECT count(DISTINCT logid) FROM %s.bfe_ai_request_log WHERE logid IN (%s)`,
			dbName, strings.Join(logids, ","))).Scan(&n); err != nil {
			return false
		}
		return n == 3
	})

	// 独立库 + 独立 topic：明细表应恰好 3 行，无重复消费/无历史残留
	if got := common.RowStrings(t, db, fmt.Sprintf(
		`SELECT toString(count()) FROM %s.bfe_ai_request_log`, dbName))[0]; got != "3" {
		t.Fatalf("明细表行数 = %s，期望 3", got)
	}

	// 逐 logid 断言：level 打平十列、限流打平三列、log_time UTC 墙钟、logid 原值、Nullable 语义
	for logid, exp := range want {
		ts := demoTs[logid]
		row := common.RowStrings(t, db, fmt.Sprintf(`SELECT
    level1Name, level1, level2Name, level2, level3Name, level3, level4Name, level4, level5Name, level5,
    rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
    toString(logid), toString(toUnixTimestamp(log_time)), toString(log_time),
    toString(isNull(ai_intent_confidence)), ifNull(toString(ai_intent_confidence), '')
FROM %s.bfe_ai_request_log WHERE logid = %s`, dbName, logid))

		wantUTC := time.Unix(ts, 0).UTC().Format("2006-01-02 15:04:05")
		wantRow := append(append(append([]string{}, exp.levels[:]...), exp.rateLimit[:]...),
			logid, fmt.Sprintf("%d", ts), wantUTC, exp.confIsNull, exp.conf)
		if strings.Join(row, "|") != strings.Join(wantRow, "|") {
			t.Errorf("logid=%s 明细行不匹配:\n  got: %q\n want: %q", logid, row, wantRow)
		}
	}
}

// flattenExpect 为 TC02 单行断言期望（与 demo/*.json 对应样例一致）。
type flattenExpect struct {
	levels     [10]string // level1Name, level1, ..., level5Name, level5
	rateLimit  [3]string  // rate_limit_policy_id, rate_limit_type, rate_limit_rule_name
	confIsNull string     // "0" = 非 NULL，"1" = NULL
	conf       string
}

// demoLogid 返回指定 demo 样例的 logid（json.Number 保留原文）。
func demoLogid(t *testing.T, name string) string {
	t.Helper()
	return fmt.Sprintf("%v", loadDemo(t, name)["logid"])
}

// demoPayload 读取 demo 样例、将 timestamp 改写为 ts（其余字段原样保留，含数字字面量），
// 压缩为单行 JSONEachRow 消息体返回。
func demoPayload(t *testing.T, name string, ts int64) []byte {
	t.Helper()
	m := loadDemo(t, name)
	m["timestamp"] = json.Number(fmt.Sprintf("%d", ts))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(m); err != nil {
		t.Fatalf("序列化 demo %s 失败: %v", name, err)
	}
	// json.Encoder 输出末尾带换行，JSONEachRow 消息体不需要
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// TestTC03DetailToAggregate 明细→聚合口径：向明细表写入三行样例等价数据
// （对齐 doris-it 直插方式，与导入链路解耦），聚合 MV 随插入同步触发，
// 按 GROUP BY + sum() 兜底范式断言 40 维 + 24 指标的关键子集。
func TestTC03DetailToAggregate(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, vars, false /*withKafka*/)

	// 固定分钟窗口：2 分钟前的整分钟桶（避开跨分钟边界 flaky；TTL 7 天内）
	tsMin := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	insertDemoRows(t, db, dbName, tsMin)

	// 聚合 MV 随明细插入同步触发，tiny sleep 仅为可见性保险
	time.Sleep(time.Second)

	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))

	// 强制后台合并后分布不变：验证排序键全 40 维的 merge 纪律（非排序键维度不被污染）
	assertMergeStable(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))
}

// TestTC04UpgradeAlter 存量升级 ALTER：明细表加列成功；聚合表/聚合 MV 不自动
// 携带新列（对齐 Doris AGGREGATE KEY 重建心智，本例仅做行为记录断言）。
func TestTC04UpgradeAlter(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, vars, false /*withKafka*/)

	// 1) 明细表加列成功（模拟 PB/log-reader 新增字段后的升级 ALTER）
	common.ExecIn(t, db, fmt.Sprintf(
		"ALTER TABLE %s.bfe_ai_request_log ADD COLUMN `it_probe_col` String DEFAULT ''", dbName))
	if got := common.ColumnTypes(t, db, dbName, "bfe_ai_request_log", []string{"it_probe_col"}); got["it_probe_col"] != "String" {
		t.Errorf("明细表加列失败，it_probe_col 类型 = %q", got["it_probe_col"])
	}

	// 2) ClickHouse 支持 ADD COLUMN IF NOT EXISTS：升级脚本可幂等（与 Doris 3.0 差异点，固化为断言）
	common.ExecIn(t, db, fmt.Sprintf(
		"ALTER TABLE %s.bfe_ai_request_log ADD COLUMN IF NOT EXISTS `it_probe_col` String DEFAULT ''", dbName))

	// 3) 行为记录：聚合表与聚合 MV 不自动携带明细新列（system.tables 中 MV 与表同列，
	//    目标表列即 MV 输出列）；明细新列要进聚合口径须按 TC05 流程重建
	if got := common.ColumnTypes(t, db, dbName, "bfe_ai_metrics_1m", []string{"it_probe_col"}); len(got) != 0 {
		t.Errorf("聚合表不应自动携带明细新列，实际存在: %v", got)
	}

	// 4) 明细 ALTER 后既有聚合 MV 不受影响：写入一行（含新列取值）仍正常聚合
	tsMin := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	common.ExecIn(t, db, fmt.Sprintf(`INSERT INTO %s.bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, logid, it_probe_col)
VALUES ('it_host', '%s', 'it_key', 'it_model', 2001, 'probe')`,
		dbName, tsMin.Add(10*time.Second).Format("2006-01-02 15:04:05")))
	time.Sleep(1 * time.Second)
	row := common.RowStrings(t, db, fmt.Sprintf(`SELECT toString(sum(request_count)) FROM %s.bfe_ai_metrics_1m
WHERE ts_min = '%s' GROUP BY ts_min`, dbName, tsMin.Format("2006-01-02 15:04:05")))
	if row[0] != "1" {
		t.Errorf("明细加列后聚合 MV 未正常触发：request_count = %q，期望 1", row[0])
	}
}

// TestTC05MetricsRebuild 重建模式：DROP 聚合 MV + 聚合表 → 按仓库 DDL 重建
// （REPLACE 语义，ClickHouse 无 Doris 原子换名，DROP+CREATE 期间由先删 MV 保证不重写）→
// 按设计稿重算语句（聚合 MV 的 SELECT 主体前加 INSERT INTO）从明细回填 → 口径不变。
func TestTC05MetricsRebuild(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, vars, false /*withKafka*/)

	// 1) 写入三行样例等价数据并聚合
	tsMin := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	insertDemoRows(t, db, dbName, tsMin)
	time.Sleep(1 * time.Second)
	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))

	// 2) 重建：先 DROP 聚合 MV（停止增量写入）再 DROP 聚合表，随后按仓库 DDL 重建
	//    （cleanup.sh 逆序：MV 先于其目标表删除；ClickHouse 中 MV 用 DROP TABLE 删除）
	common.ExecIn(t, db, fmt.Sprintf("DROP TABLE %s.bfe_ai_metrics_1m_mv", dbName))
	common.ExecIn(t, db, fmt.Sprintf("DROP TABLE %s.bfe_ai_metrics_1m", dbName))
	common.ExecFileIn(t, db, common.SQLPath(t, "bfe_ai_metrics_1m.sql"), vars)
	common.ExecFileIn(t, db, common.SQLPath(t, "bfe_ai_metrics_1m_mv.sql"), vars)

	// 重建后（回填前）应为空：证明 REPLACE 语义——旧口径数据不随表名保留
	if got := common.RowStrings(t, db, fmt.Sprintf(
		`SELECT toString(count()) FROM %s.bfe_ai_metrics_1m`, dbName))[0]; got != "0" {
		t.Fatalf("重建后聚合表应为空（待重算），实际 %s 行", got)
	}

	// 3) 按设计稿 §4.3 重算语句回填：INSERT INTO <聚合表> + 聚合 MV 的 SELECT 主体
	//    （GROUP BY 全 40 维，从明细表重算历史窗口）
	common.ExecIn(t, db, extractRecompute(t, dbName, vars))
	time.Sleep(1 * time.Second)

	// 4) 重建后口径不变：与重建前同一套断言
	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))
}

// insertDemoRows 向明细表写入三行样例等价数据（取值与 demo/*.json 一致），
// 落在 base 分钟桶的 10s/20s/30s，聚合后 ts_min = base 单桶。
// 列清单只含非默认值列：空串列（level4/5、backend_info、ai_requested_model 等）由列
// DEFAULT ” 承担、0 指标与空数组同理——既等价 demo 消息缺失字段走默认值的真实消费语义，
// 也规避 26.10 master 构建 VALUES 模板推断对「非空字段后的尾部空串 + 复合字面量」的错列问题。
func insertDemoRows(t *testing.T, db *sql.DB, dbName string, base time.Time) {
	t.Helper()
	fmtT := func(d time.Duration) string {
		return base.Add(d).Format("2006-01-02 15:04:05")
	}
	// 行 A：normal 样例（缓存命中 + 意图分类 + 镜像命中，token 34/173/207）
	common.ExecIn(t, db, fmt.Sprintf(`INSERT INTO %s.bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, ai_target_model,
     product, cluster, sub_cluster, backend_info, method, res_status_code, header_host,
     level1Name, level1, level2Name, level2, level3Name, level3,
     ai_cache_status, mirror_hit, ai_intent_answer, ai_intent_confidence, ai_cluster_key_names,
     ai_input_tokens, ai_output_tokens, ai_total_tokens, ai_ttft_us, ai_tpot_us,
     req_header_len, res_header_len, res_body_len,
     all_time, cluster_serve_time, backend_serve_time, logid)
VALUES
    ('yyxdev_4026531840', '%s', 'TESTKEY_ID', 'test-model', 'gpt-5',
     'example_ai_product', 'ai_cluster_example', 'ai.pool.bj', '172.27.152.27:8081', 'POST', 200, 'ai.example.org',
     'dep0', 'rd', 'dep2', 'teama', 'dep3', 'yyx',
     'hit', 1, 'intent-a-1', 0.95, [('ai_cluster_example', '')],
     34, 173, 207, 6287, 3,
     184, 154, 445,
     10, 6, 6, 10602749765076101032)`, dbName, fmtT(10*time.Second)))

	// 行 B：rate_limit 样例（限流打平 rlp-0002/tpm/tpm1，err_code=AI_RATE_LIMIT，input_tokens=61）
	common.ExecIn(t, db, fmt.Sprintf(`INSERT INTO %s.bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, ai_target_model,
     product, cluster, sub_cluster, method, res_status_code, err_code, header_host,
     level1Name, level1, level2Name, level2,
     rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
     ai_rate_limit_hits, ai_cluster_key_names, ai_input_tokens,
     req_header_len, res_header_len, res_body_len, all_time, logid)
VALUES
    ('yyxdev_4026531840', '%s', 'TESTKEY_ID', 'test-model', 'gpt-5',
     'example_ai_product', 'ai_cluster_example', 'ai.pool.bj', 'POST', 429, 'AI_RATE_LIMIT', 'ai.example.org',
     'dep0', 'rd', 'dep2', 'teama',
     'rlp-0002', 'tpm', 'tpm1',
     [('rlp-0002', 'tpm', ['tpm1'])], [('ai_cluster_example', '')], 61,
     184, 137, 103, 4, 8877700219231856663)`, dbName, fmtT(20*time.Second)))

	// 行 C：auth_reject 样例（认证拒绝，quota 计划 plan_basic/plan_pro，err_code=AI_AUTH_REJECT）
	common.ExecIn(t, db, fmt.Sprintf(`INSERT INTO %s.bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, product, cluster, sub_cluster,
     method, res_status_code, err_code, header_host,
     ai_auth_reject_reason, ai_auth_reject_quota_plans,
     req_header_len, res_header_len, res_body_len, all_time, logid)
VALUES
    ('yyxdev_4026531840', '%s', 'invalid-key', 'example_ai_product', 'ai_cluster_example', 'ai.pool.bj',
     'POST', 401, 'AI_AUTH_REJECT', 'ai.example.org',
     'apikey not found', ['plan_basic', 'plan_pro'],
     150, 100, 50, 2, 12345678901234567890)`, dbName, fmtT(30*time.Second)))
}

// assertAggregation 按 GROUP BY + sum() 兜底范式断言 ts_min 桶的聚合结果
// （SummingMergeTree 纪律：禁止 SELECT * 直读，相同排序键行仅 merge 时合并，
// 查询侧必须 GROUP BY 维度 + sum(指标)）。
// 40 维中断言关键子集：整窗指标、缓存状态、限流三列、镜像命中、意图答案、配额计划槽位。
func assertAggregation(t *testing.T, db *sql.DB, dbName, tsMin string) {
	t.Helper()

	// 1) 整窗 24 指标中的关键子集（GROUP BY ts_min + sum() 兜底）
	totals := aggTotals(t, db, dbName, tsMin)
	for _, k := range []string{"request_count", "error_count", "auth_reject_count", "input_tokens",
		"output_tokens", "total_tokens", "rate_limit_hits", "cache_read_tokens", "cache_write_tokens"} {
		if totals[k] != aggTotalsWant[k] {
			t.Errorf("整窗指标 %s = %q，期望 %q（整窗: %v）", k, totals[k], aggTotalsWant[k], totals)
		}
	}

	// 2) 缓存状态维度：normal 行 'hit'，其余两行 ''（未启用）
	if got := aggBreakdown(t, db, dbName, tsMin, "ai_cache_status"); !eqMap(got, map[string]string{"hit": "1", "": "2"}) {
		t.Errorf("ai_cache_status 维度分布 = %v，期望 map[hit:1 :2]", got)
	}

	// 2b) apikey 标签维度：normal 与 rate_limit 行 level1='rd'，auth_reject 行空
	if got := aggBreakdown(t, db, dbName, tsMin, "level1"); !eqMap(got, map[string]string{"rd": "2", "": "1"}) {
		t.Errorf("level1 维度分布 = %v，期望 map[rd:2 :1]", got)
	}

	// 3) 限流打平三列维度：rate_limit 行 rlp-0002/tpm/tpm1，其余两行空
	if got := aggBreakdown(t, db, dbName, tsMin, "rate_limit_policy_id, rate_limit_type, rate_limit_rule_name"); !eqMap(got, map[string]string{"rlp-0002|tpm|tpm1": "1", "||": "2"}) {
		t.Errorf("限流三列维度分布 = %v，期望 map[rlp-0002|tpm|tpm1:1 ||:2]", got)
	}

	// 4) 镜像命中维度：normal 行 1，其余两行 0
	if got := aggBreakdown(t, db, dbName, tsMin, "mirror_hit"); !eqMap(got, map[string]string{"1": "1", "0": "2"}) {
		t.Errorf("mirror_hit 维度分布 = %v，期望 map[1:1 0:2]", got)
	}

	// 5) 意图答案维度：normal 行 intent-a-1，其余两行 ''（未分类）
	if got := aggBreakdown(t, db, dbName, tsMin, "ai_intent_answer"); !eqMap(got, map[string]string{"intent-a-1": "1", "": "2"}) {
		t.Errorf("ai_intent_answer 维度分布 = %v，期望 map[intent-a-1:1 :2]", got)
	}

	// 6) 认证拒绝行的配额计划槽位（slot1/2 取数组前两个元素，slot3 越界为 ''）
	auth := aggBreakdownWhere(t, db, dbName, tsMin,
		"ai_auth_reject_reason, ai_auth_reject_quota_plans_slot1, ai_auth_reject_quota_plans_slot2, ai_auth_reject_quota_plans_slot3",
		"ai_auth_reject_reason != ''")
	if !eqMap(auth, map[string]string{"apikey not found|plan_basic|plan_pro|": "1"}) {
		t.Errorf("认证拒绝维度分布 = %v，期望 map[apikey not found|plan_basic|plan_pro|:1]", auth)
	}

	// 6b) 配额计划槽位 1 全桶分布：auth_reject 行 'plan_basic'，其余两行 ''（对齐冒烟口径）
	if got := aggBreakdown(t, db, dbName, tsMin, "ai_auth_reject_quota_plans_slot1"); !eqMap(got, map[string]string{"plan_basic": "1", "": "2"}) {
		t.Errorf("quota_plans_slot1 维度分布 = %v，期望 map[plan_basic:1 :2]", got)
	}
}

// assertMergeStable 强制后台合并（OPTIMIZE TABLE ... FINAL）后重查关键分布：
// 排序键为全 40 维，相同排序键行在 merge 时按指标求和折叠、维度值不受影响——
// 若排序键收窄（如仅前 6 列），非排序键维度会被任取一行污染分布，此断言即失效报警。
func assertMergeStable(t *testing.T, db *sql.DB, dbName, tsMin string) {
	t.Helper()
	common.ExecIn(t, db, fmt.Sprintf("OPTIMIZE TABLE %s.bfe_ai_metrics_1m FINAL", dbName))
	if got := aggBreakdown(t, db, dbName, tsMin, "ai_cache_status"); !eqMap(got, map[string]string{"hit": "1", "": "2"}) {
		t.Errorf("OPTIMIZE FINAL 后 ai_cache_status 维度分布 = %v，期望 map[hit:1 :2]（排序键收窄导致 merge 污染？）", got)
	}
	totals := aggTotals(t, db, dbName, tsMin)
	for _, k := range []string{"request_count", "error_count", "auth_reject_count", "input_tokens", "output_tokens"} {
		if totals[k] != aggTotalsWant[k] {
			t.Errorf("OPTIMIZE FINAL 后整窗指标 %s = %q，期望 %q", k, totals[k], aggTotalsWant[k])
		}
	}
}

// aggTotalsWant 为整窗指标期望值（三行样例合计：request=3、error=2（rate_limit+auth_reject）、
// auth_reject=1、input/output/total=95/173/207、rate_limit_hits=1、缓存读写 token 均为 0）。
var aggTotalsWant = map[string]string{
	"request_count":      "3",
	"error_count":        "2",
	"auth_reject_count":  "1",
	"input_tokens":       "95",
	"output_tokens":      "173",
	"total_tokens":       "207",
	"rate_limit_hits":    "1",
	"cache_read_tokens":  "0",
	"cache_write_tokens": "0",
}

// aggTotals 返回 ts_min 桶的整窗 sum() 指标（GROUP BY ts_min 兜底）。
func aggTotals(t *testing.T, db *sql.DB, dbName, tsMin string) map[string]string {
	t.Helper()
	keys := []string{"request_count", "error_count", "auth_reject_count", "input_tokens",
		"output_tokens", "total_tokens", "rate_limit_hits", "cache_read_tokens", "cache_write_tokens"}
	sums := make([]string, len(keys))
	for i, k := range keys {
		sums[i] = "toString(sum(" + k + "))"
	}
	row := common.RowStrings(t, db, fmt.Sprintf(`SELECT %s FROM %s.bfe_ai_metrics_1m
WHERE ts_min = '%s' GROUP BY ts_min`, strings.Join(sums, ", "), dbName, tsMin))
	out := map[string]string{}
	for i, k := range keys {
		out[k] = row[i]
	}
	return out
}

// aggBreakdown 返回 GROUP BY dims 的 request_count 分布（键 = 维度值按 | 拼接）。
func aggBreakdown(t *testing.T, db *sql.DB, dbName, tsMin, dims string) map[string]string {
	t.Helper()
	return aggBreakdownWhere(t, db, dbName, tsMin, dims, "1=1")
}

func aggBreakdownWhere(t *testing.T, db *sql.DB, dbName, tsMin, dims, where string) map[string]string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT %s, toString(sum(request_count)) FROM %s.bfe_ai_metrics_1m
WHERE ts_min = '%s' AND %s GROUP BY %s`, dims, dbName, tsMin, where, dims))
	if err != nil {
		t.Fatalf("聚合维度查询失败: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("读取列名失败: %v", err)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	out := map[string]string{}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("扫描聚合维度行失败: %v", err)
		}
		key := make([]string, len(vals)-1)
		for i := 0; i < len(vals)-1; i++ {
			if vals[i].Valid {
				key[i] = vals[i].String
			}
		}
		var cnt string
		if vals[len(vals)-1].Valid {
			cnt = vals[len(vals)-1].String
		}
		out[strings.Join(key, "|")] = cnt
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历聚合维度结果失败: %v", err)
	}
	return out
}

// extractRecompute 从仓库聚合 MV SQL 提取重算语句：MV 的 SELECT 主体
// （GROUP BY 全 40 维）前加 INSERT INTO <聚合表>（对齐设计稿 §4.3 的历史窗口重算语句）。
func extractRecompute(t *testing.T, dbName string, vars map[string]string) string {
	t.Helper()
	content := common.LoadSQL(t, common.SQLPath(t, "bfe_ai_metrics_1m_mv.sql"), vars)
	idx := strings.Index(content, " AS\nSELECT")
	if idx < 0 {
		t.Fatalf("聚合 MV SQL 中未找到 \" AS\\nSELECT\" 结构:\n%s", content)
	}
	body := strings.TrimRight(strings.TrimSpace(content[idx+len(" AS\n"):]), ";")
	return fmt.Sprintf("INSERT INTO %s.bfe_ai_metrics_1m\n%s", dbName, body)
}

// eqMap 比较两个 string map（断言辅助）。
func eqMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// loadDemo 读取 demo JSON（字段按 json.Number 保留原文）。
func loadDemo(t *testing.T, name string) map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(common.DemoPath(t, name))
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
