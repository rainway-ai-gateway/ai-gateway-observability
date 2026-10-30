// 场景 STARROCKS01：StarRocks 对接（报表存储新引擎，等价 Doris 既有链路）。
//
// 被测对象是本仓库真实 SQL 资产（starrocks/sqls/，仅做 ${VAR} 变量替换），
// 覆盖五个测试例（详见 ../../测试设计文档/scenario-STARROCKS01-starrocks对接/场景说明.md）：
//
// TC01 全新安装：按 setup.sh 顺序应用四份 SQL，断言明细表/MV/Routine Load 存在，
//
//	列数与类型正确；重复应用行为固化（建库幂等，表/MV/RL 非幂等，同 Doris 语义）
//
// TC02 消费打平：真实 Kafka → Routine Load COLUMNS 映射，断言 level 十列 /
//
//	限流三列 / UTC 墙钟 / 布尔两列落 0/1 / NULL 语义（demo 的 UInt64 域 logid 重写到
//	Int64 范围——SR 对超界 BIGINT 直接拒绝（错误行跳过），与 Doris 静默截断、CH UInt64
//	原值均为差异点，固化为文档断言；布尔列断言是 issue #10 的回归守卫：TINYINT 声明
//	会让 JSON 布尔静默落 NULL 且坏行计数为 0）
//
// TC03 明细→聚合口径：异步 MV 周期刷新，断言 24 指标关键子集 + 40 维关键子集，
//
//	且跨刷新周期不重复累计（增量刷新纪律）
//
// TC04 存量升级 ALTER：明细表加列成功；SR 3.5 不支持 ADD COLUMN IF NOT EXISTS
//
//	（负向断言，与 Doris/CH 差异点固化）；MV 不自动携带新列（行为记录）
//
// TC05 重建模式：DROP MV → 按仓库 DDL 重建 → MV 自动从基表全量回填 → 口径不变
package sr01_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"starrocks-it/common"
)

// wantObjects 为 TC01 断言对象（information_schema.tables 中表与 MV 同列）。
var wantObjects = map[string]string{
	"bfe_ai_request_log": "BASE TABLE",
	"bfe_ai_metrics_1m":  "VIEW", // 异步物化视图
}

// TestTC01FreshInstall 全新安装 schema：按序建明细表/MV/Routine Load，
// 重复应用行为固化（建库 IF NOT EXISTS 幂等；表/MV/RL 同名重建报错，与 Doris 资产语义一致）。
func TestTC01FreshInstall(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars) // Step 1：建库（bfe_observability.sql）
	common.SetupFreshInstall(t, db, dbName, vars, true /*withMV*/, true /*withRoutineLoad*/)

	// 1) 对象存在且类型正确
	objects := common.Objects(t, db, dbName)
	for name, wantType := range wantObjects {
		got, ok := objects[name]
		if !ok {
			t.Errorf("缺少对象 %s（库 %s 现有对象: %v）", name, dbName, objects)
			continue
		}
		if got != wantType {
			t.Errorf("对象 %s 类型 = %s，期望 %s", name, got, wantType)
		}
	}
	if len(objects) != len(wantObjects) {
		t.Errorf("库 %s 对象数 = %d，期望 %d（实际: %v）", dbName, len(objects), len(wantObjects), objects)
	}
	if !common.RoutineLoadExists(t, db, dbName) {
		t.Errorf("Routine Load bfe_ai_log_load 不存在（库 %s）", dbName)
	}
	if !common.MVActive(t, db, dbName, "bfe_ai_metrics_1m") {
		t.Errorf("物化视图 bfe_ai_metrics_1m 不存在或 is_active=false")
	}

	// 2) 列数：明细表与 Doris 版 DDL 逐列同名同型（104 列，两版 DDL 列定义数一致）；
	//    MV = 分区列 ts_day + 40 维 + 24 指标 = 65 列
	if got := common.ColumnCount(t, db, dbName, "bfe_ai_request_log"); got != 104 {
		t.Errorf("明细表列数 = %d，期望 104", got)
	}
	if got := common.ColumnCount(t, db, dbName, "bfe_ai_metrics_1m"); got != 65 {
		t.Errorf("MV 列数 = %d，期望 65（ts_day + 40 维 + 24 指标）", got)
	}

	// 3) 关键列类型（SR 方言落库形态固化）。必须走 DESC：information_schema 把 BOOLEAN
	//    呈现为 tinyint(1)，与真正的 TINYINT 无法区分——布尔列一旦被误声明为 TINYINT，
	//    JSON 布尔会静默落 NULL（issue #10 的漏检路径），故布尔列断言以声明类型为准。
	types := common.ColumnTypesDesc(t, db, dbName, "bfe_ai_request_log")
	for col, want := range map[string]string{
		"logid": "bigint", "log_time": "datetime",
		"mirror_hit": "boolean", "ai_intent_cache_hit": "boolean",
		"ai_stream": "boolean", "is_trust_src_ip": "boolean",
		"ai_intent_confidence": "double", "level1Name": "varchar(128)", "ai_input_tokens": "bigint",
		// 缺价信号（bfe#1479）：ai_price_miss 同样吃 JSON 布尔，必须是 BOOLEAN（issue #10 教训）
		"ai_price_miss": "boolean", "ai_price_warn": "varchar(32)",
	} {
		if types[col] != want {
			t.Errorf("明细表列 %s 类型 = %q，期望 %q（DESC 口径）", col, types[col], want)
		}
	}

	// 4) 重复应用行为：建库幂等；表/MV/RL 非幂等（同 doris 版资产，setup.sh 为
	//    一次性脚本，重建须先 cleanup）——此处把差异固化为断言，防误改 SQL 引入
	//    IF NOT EXISTS 造成"重复 setup 静默跳过"的运维错觉
	if err := common.ExecE(db, "", common.LoadSQL(t, common.SQLPath(t, "bfe_observability.sql"), vars)); err != nil {
		t.Errorf("建库 SQL 重复应用应幂等，实际报错: %v", err)
	}
	for _, f := range []string{"bfe_ai_request_log.sql", "bfe_ai_metrics_1m.sql", "bfe_ai_log_load_routine.sql"} {
		if err := common.ExecE(db, dbName, common.LoadSQL(t, common.SQLPath(t, f), vars)); err == nil {
			t.Errorf("重复应用 %s 应报已存在错误，实际成功", f)
		} else {
			msg := strings.ToLower(err.Error())
			// 表/MV 报 "already exists"；SR Routine Load 报 "Name ... already used in db ..."
			if !strings.Contains(msg, "exist") && !strings.Contains(msg, "already used") {
				t.Errorf("重复应用 %s 的错误信息不符合预期: %v", f, err)
			}
		}
	}
}

// demoCases 为 TC02/TC03 共用的三样例清单。
// logid 为消息生产/直插使用的 Int64 域值：demo 原文中 normal（10602749765076101032）
// 与 auth_reject（12345678901234567890）超出 Int64 上限——SR 对超界 BIGINT 值直接
// 拒绝（INSERT 报错 / Routine Load 计错误行跳过），Doris 静默截断、CH UInt64 原值
// 保留，三引擎差异点在测试设计文档固化；此处按"保持数字形态"原则收敛到 Int64 范围。
var demoCases = []struct {
	file  string
	logid string
}{
	{"normal_request.json", "1060274976507610103"},
	{"rate_limit.json", "8877700219231856663"},
	{"auth_reject.json", "1234567890123456789"},
}

// TestTC02KafkaConsumptionFlatten 消费打平：真实 Kafka 生产三份 demo 样例，
// 经 Routine Load 的 COLUMNS 映射（JSON 打平 + UTC 墙钟换算）落入明细表。
//
// Kafka 不可达时 Skip（RequireKafka）。降级路径（不自动执行，供无 Kafka 环境手工验证）：
// 跳过 Kafka/Routine Load，直接向明细表 INSERT 时手工复现 COLUMNS 表达式
// （json_unquote(json_extract(...)) 打平列 + DATE_SUB(FROM_UNIXTIME(...), INTERVAL ...) UTC 式），
// 等价 RL 列映射求值结果。
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
	common.SetupFreshInstall(t, db, dbName, vars, true /*withMV*/, true /*withRoutineLoad*/)
	// 本机 9p 盘上 Kafka 元数据获取偶发失败会使 job 创建即 PAUSED，自愈一次
	common.ResumeRoutineLoadIfPaused(t, db, dbName)

	// 生产三份 demo 样例 + 一份"流式"变体（值即断言期望来源，兼校验 demo 文件与 DDL 一致性）。
	// 消息层改写（不改 demo 文件）：
	//  1. timestamp 重写为当前时刻——demo 时间戳 2026-08-24 早于动态分区 start=-7
	//     的既有分区范围，消费落库会因无对应分区成为错误行；
	//  2. logid 重写到 Int64 域（见 demoCases 注释）——SR 对超界 BIGINT 直接拒绝；
	//  3. 第 4 条把 normal 样例的 ai_stream / is_trust_src_ip 改写为 JSON true——三份 demo
	//     都是 false，只覆盖 0 分支；布尔列落库口径（issue #10）要求 1/0 都被断言。
	const streamLogid = "1060274976507610104"
	base := time.Now()
	demoTs := map[string]int64{}
	payloads := make([][]byte, 0, len(demoCases)+1)
	for i, c := range demoCases {
		ts := base.Add(time.Duration(i-2) * 10 * time.Second).Unix()
		demoTs[c.logid] = ts
		payloads = append(payloads, demoPayload(t, c.file, c.logid, ts))
	}
	streamTs := base.Add(20 * time.Second).Unix()
	demoTs[streamLogid] = streamTs
	payloads = append(payloads, demoPayloadBool(t, demoCases[0].file, streamLogid, streamTs, true))
	common.ProduceMessages(t, brokers, topic, payloads...)

	// 断言期望：level 十列 / 限流三列 / 布尔两列（JSON 布尔必须落 0/1，绝不能 NULL）/ NULL 语义
	want := map[string]flattenExpect{
		demoCases[0].logid: {
			levels:     [10]string{"dep0", "rd", "dep2", "teama", "dep3", "yyx", "", "", "", ""},
			rateLimit:  [3]string{"", "", ""},
			confIsNull: "0", conf: "0.95",
			stream: "0", trustSrcIP: "0",
			priceMissIsNull: "0", priceMiss: "1",
			priceWarnIsNull: "0", priceWarn: "subitem_missing",
		},
		demoCases[1].logid: {
			levels:     [10]string{"dep0", "rd", "dep2", "teama", "", "", "", "", "", ""},
			rateLimit:  [3]string{"rlp-0002", "tpm", "tpm1"},
			confIsNull: "1", conf: "",
			stream: "0", trustSrcIP: "0",
			priceMissIsNull: "1", priceMiss: "",
			priceWarnIsNull: "1", priceWarn: "",
		},
		demoCases[2].logid: {
			levels:     [10]string{"", "", "", "", "", "", "", "", "", ""},
			rateLimit:  [3]string{"", "", ""},
			confIsNull: "1", conf: "",
			stream: "0", trustSrcIP: "0",
			priceMissIsNull: "1", priceMiss: "",
			priceWarnIsNull: "1", priceWarn: "",
		},
		// normal 样例 + ai_stream/is_trust_src_ip 改写为 JSON true
		streamLogid: {
			levels:     [10]string{"dep0", "rd", "dep2", "teama", "dep3", "yyx", "", "", "", ""},
			rateLimit:  [3]string{"", "", ""},
			confIsNull: "0", conf: "0.95",
			stream: "1", trustSrcIP: "1",
			// 该条由 normal 样例派生，缺价两列随之带上
			priceMissIsNull: "0", priceMiss: "1",
			priceWarnIsNull: "0", priceWarn: "subitem_missing",
		},
	}

	// Routine Load 消费有秒级到分钟级延迟（本机 9p 盘上消费组加入与分区分配
	// 可达 1-2 分钟）：轮询至全部 logid 落明细表，期间 PAUSED 自愈
	logids := make([]string, 0, len(want))
	for logid := range want {
		logids = append(logids, logid)
	}
	common.WaitFor(t, "样例消息落入明细表", 300*time.Second, 5*time.Second, func() bool {
		var n int
		if err := db.QueryRow(fmt.Sprintf(
			`SELECT COUNT(DISTINCT logid) FROM %s.bfe_ai_request_log WHERE logid IN (%s)`,
			dbName, strings.Join(logids, ","))).Scan(&n); err != nil {
			return false
		}
		if n == len(want) {
			return true
		}
		common.ResumeRoutineLoadIfPaused(t, db, dbName)
		return false
	})

	// 独立库 + 独立 topic：明细表应恰好 len(want) 行，无重复消费/无历史残留
	if got := common.RowStrings(t, db, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s.bfe_ai_request_log`, dbName))[0]; got != fmt.Sprintf("%d", len(want)) {
		t.Fatalf("明细表行数 = %s，期望 %d", got, len(want))
	}

	// 逐 logid 断言：level 打平十列、限流打平三列、log_time UTC 墙钟、布尔两列 0/1、NULL 语义
	for logid, exp := range want {
		ts := demoTs[logid]
		row := common.RowStrings(t, db, fmt.Sprintf(`SELECT
    level1Name, level1, level2Name, level2, level3Name, level3, level4Name, level4, level5Name, level5,
    rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
    CAST(logid AS CHAR), TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', log_time), CAST(log_time AS CHAR),
    IF(ai_intent_confidence IS NULL, '1', '0'), IFNULL(CAST(ai_intent_confidence AS CHAR), ''),
    IF(ai_stream IS NULL, 'NULL', IF(ai_stream, '1', '0')),
    IF(is_trust_src_ip IS NULL, 'NULL', IF(is_trust_src_ip, '1', '0')),
    IF(ai_price_miss IS NULL, '1', '0'), IF(ai_price_miss IS NULL, '', IF(ai_price_miss, '1', '0')),
    IF(ai_price_warn IS NULL, '1', '0'), IFNULL(ai_price_warn, '')
FROM %s.bfe_ai_request_log WHERE logid = %s`, dbName, logid))

		wantUTC := time.Unix(ts, 0).UTC().Format("2006-01-02 15:04:05")
		wantRow := append(append(append([]string{}, exp.levels[:]...), exp.rateLimit[:]...),
			logid, fmt.Sprintf("%d", ts), wantUTC, exp.confIsNull, exp.conf, exp.stream, exp.trustSrcIP,
			exp.priceMissIsNull, exp.priceMiss, exp.priceWarnIsNull, exp.priceWarn)
		if strings.Join(row, "|") != strings.Join(wantRow, "|") {
			t.Errorf("logid=%s 明细行不匹配:\n  got: %q\n want: %q", logid, row, wantRow)
		}
	}

	// 布尔列非空守卫（issue #10）：payload 里两字段均为 JSON 布尔，若 DDL 把它们声明成
	// TINYINT，SR 会静默写 NULL 且 NumberFilteredRows=0——只有列级非空统计能发现
	guard := common.RowStrings(t, db, fmt.Sprintf(`SELECT
    CAST(COUNT(*) - COUNT(ai_stream) AS CHAR),
    CAST(COUNT(*) - COUNT(is_trust_src_ip) AS CHAR)
FROM %s.bfe_ai_request_log`, dbName))
	if guard[0] != "0" || guard[1] != "0" {
		t.Errorf("布尔列存在 NULL 行：ai_stream 缺失 %s 行、is_trust_src_ip 缺失 %s 行（均应为 0）", guard[0], guard[1])
	}
}

// flattenExpect 为 TC02 单行断言期望（与 demo/*.json 对应样例一致）。
type flattenExpect struct {
	levels     [10]string // level1Name, level1, ..., level5Name, level5
	rateLimit  [3]string  // rate_limit_policy_id, rate_limit_type, rate_limit_rule_name
	confIsNull string     // "0" = 非 NULL，"1" = NULL
	conf       string
	stream     string // ai_stream：'1' 流式 / '0' 非流式（断言查询把 NULL 渲染成 "NULL"）
	trustSrcIP string // is_trust_src_ip：'1' 可信 / '0' 不可信
	// 缺价信号两列（bfe#1479）：未携带字段的消息必须为 NULL（≠ 0/空串）
	priceMissIsNull string
	priceMiss       string
	priceWarnIsNull string
	priceWarn       string
}

// demoLogid 返回指定 demo 样例的原始 logid（json.Number 保留原文，超 Int64 域）。
func demoLogid(t *testing.T, name string) string {
	t.Helper()
	return fmt.Sprintf("%v", loadDemo(t, name)["logid"])
}

// demoPayload 读取 demo 样例、将 logid 重写到 Int64 域取值 logid、timestamp 改写为 ts
// （其余字段原样保留，含数字字面量），压缩为单行 JSON 消息体返回。
func demoPayload(t *testing.T, name string, logid string, ts int64) []byte {
	t.Helper()
	m := loadDemo(t, name)
	m["logid"] = json.Number(logid)
	m["timestamp"] = json.Number(fmt.Sprintf("%d", ts))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(m); err != nil {
		t.Fatalf("序列化 demo %s 失败: %v", name, err)
	}
	// json.Encoder 输出末尾带换行，消息体不需要
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// demoPayloadBool 同 demoPayload，另把两个布尔字段改写为 JSON true/false。
// 用于断言 Routine Load 把 JSON 布尔落成 0/1（三份 demo 的取值都是 false，只覆盖 0 分支）。
func demoPayloadBool(t *testing.T, name, logid string, ts int64, v bool) []byte {
	t.Helper()
	m := loadDemo(t, name)
	m["logid"] = json.Number(logid)
	m["timestamp"] = json.Number(fmt.Sprintf("%d", ts))
	m["ai_stream"] = v
	m["is_trust_src_ip"] = v
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatalf("序列化 demo %s 失败: %v", name, err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// TestTC03DetailToAggregate 明细→聚合口径：向明细表写入三行样例等价数据
// （对齐 doris-it/clickhouse-it 直插方式，与导入链路解耦），异步 MV 周期刷新后
// 断言 40 维 + 24 指标的关键子集；再跨一个刷新周期重查，断言增量刷新不重复累计。
func TestTC03DetailToAggregate(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, true /*withMV*/, false /*withRoutineLoad*/)

	// 固定分钟窗口：2 分钟前的整分钟桶（避开跨分钟边界 flaky；动态分区 7 天窗口内）
	tsMin := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	insertDemoRows(t, db, dbName, tsMin)

	waitMVRequestCount(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"), 3)
	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))

	// 增量刷新纪律：再跨一个刷新周期（70s），聚合值不得重复累计
	time.Sleep(70 * time.Second)
	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))
}

// TestTC04UpgradeAlter 存量升级 ALTER：明细表加列成功；SR 3.5 不支持
// ADD COLUMN IF NOT EXISTS（负向断言，升级脚本的幂等须走 information_schema 判断
// 或容忍报错，与 Doris 3.0 / ClickHouse 差异点固化）；行为记录：MV 不自动携带
// 明细新列（对齐 Doris AGGREGATE KEY 重建心智）；明细 ALTER 后 MV 刷新不受影响。
func TestTC04UpgradeAlter(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, true /*withMV*/, false /*withRoutineLoad*/)

	// 1) 明细表加列成功（模拟 PB/log-reader 新增字段后的升级 ALTER；SR Schema
	//    Change 异步生效，轮询至列可见）
	common.ExecIn(t, db, dbName, fmt.Sprintf(
		"ALTER TABLE bfe_ai_request_log ADD COLUMN it_probe_col VARCHAR(64) DEFAULT ''"))
	waitColumn(t, db, dbName, "bfe_ai_request_log", "it_probe_col")

	// 2) 负向断言：SR 3.5.21 不支持 ADD COLUMN IF NOT EXISTS（1064 "No viable
	//    statement for input 'ADD COLUMN IF'"）——升级脚本幂等手段与 Doris/CH 不同
	err := common.ExecE(db, dbName,
		"ALTER TABLE bfe_ai_request_log ADD COLUMN IF NOT EXISTS it_probe_col2 VARCHAR(64) DEFAULT ''")
	if err == nil || !strings.Contains(err.Error(), "ADD COLUMN IF") {
		t.Errorf("SR 3.5 预期不支持 ADD COLUMN IF NOT EXISTS，实际: %v", err)
	}

	// 3) 行为记录：MV 不自动携带明细新列（MV 输出列在创建时固化）；
	//    明细新列要进聚合口径须按 TC05 流程重建 MV
	if got := common.ColumnTypes(t, db, dbName, "bfe_ai_metrics_1m", []string{"it_probe_col"}); len(got) != 0 {
		t.Errorf("MV 不应自动携带明细新列，实际存在: %v", got)
	}

	// 4) 明细 ALTER 后既有 MV 不受影响：写入一行（含新列取值）刷新后正常聚合
	tsMin := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	common.ExecIn(t, db, dbName, fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, logid, it_probe_col)
VALUES ('it_host', '%s', 'it_key', 'it_model', 2001, 'probe')`,
		tsMin.Add(10*time.Second).Format("2006-01-02 15:04:05")))
	waitMVRequestCount(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"), 1)
	if got := common.RowStrings(t, db, fmt.Sprintf(`SELECT SUM(request_count) FROM %s.bfe_ai_metrics_1m
WHERE ts_min = '%s' GROUP BY ts_min`, dbName, tsMin.Format("2006-01-02 15:04:05")))[0]; got != "1" {
		t.Errorf("明细加列后 MV 未正常聚合：request_count = %q，期望 1", got)
	}
}

// TestTC06PriceMissUpgrade 缺价信号存量升级（bfe#1479）：DROP 两列模拟旧表 → 按设计稿
// §4 的升级语句 ADD COLUMN → 类型（DESC 口径）与明细 NULL 语义正确。SR 的 Schema Change
// 异步生效，列的出现/消失均需轮询。
func TestTC06PriceMissUpgrade(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, true /*withMV*/, false /*withRoutineLoad*/)

	// 1) 旧表模拟：DROP 两列，轮询至不可见
	common.ExecIn(t, db, dbName,
		"ALTER TABLE bfe_ai_request_log DROP COLUMN ai_price_miss, DROP COLUMN ai_price_warn")
	common.WaitFor(t, "两列已从明细表移除", 60*time.Second, time.Second, func() bool {
		return len(common.ColumnTypes(t, db, dbName, "bfe_ai_request_log",
			[]string{"ai_price_miss", "ai_price_warn"})) == 0
	})

	// 2) 按设计稿 §4 的升级语句加列（SR 3.5 不支持 ADD COLUMN IF NOT EXISTS，每环境一次）
	common.ExecIn(t, db, dbName, `ALTER TABLE bfe_ai_request_log
    ADD COLUMN ai_price_miss BOOLEAN     COMMENT '是否发生缺价回退（NULL=未上报，1/0）' AFTER ai_intent_questions_version,
    ADD COLUMN ai_price_warn VARCHAR(32) COMMENT '缺价形态：fallback/subitem_missing/no_entry/no_baseline/no_model_table/reject（NULL=未上报）' AFTER ai_price_miss`)
	waitColumn(t, db, dbName, "bfe_ai_request_log", "ai_price_miss")
	waitColumn(t, db, dbName, "bfe_ai_request_log", "ai_price_warn")

	// 3) 类型（DESC 口径）：ai_price_miss 必须是 BOOLEAN——JSON 布尔落 TINYINT 会静默写 NULL
	types := common.ColumnTypesDesc(t, db, dbName, "bfe_ai_request_log")
	if types["ai_price_miss"] != "boolean" || types["ai_price_warn"] != "varchar(32)" {
		t.Errorf("升级后列类型 = ai_price_miss:%q / ai_price_warn:%q，期望 boolean / varchar(32)",
			types["ai_price_miss"], types["ai_price_warn"])
	}

	// 4) 明细 NULL 语义：带信号行 1/'subitem_missing'；旧消息行两列 NULL（≠ 0/空串）
	ts := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	common.ExecIn(t, db, dbName, fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, logid, ai_price_miss, ai_price_warn)
VALUES
    ('it_host', '%s', 'it_key', 'it_model', 6001, 1, 'subitem_missing'),
    ('it_host', '%s', 'it_key', 'it_model', 6002, NULL, NULL)`,
		ts.Add(10*time.Second).Format("2006-01-02 15:04:05"),
		ts.Add(20*time.Second).Format("2006-01-02 15:04:05")))

	for _, c := range []struct {
		logid           int64
		wantMiss, wantW string
	}{
		{6001, "1", "subitem_missing"},
		{6002, "NULL", "NULL"},
	} {
		row := common.RowStrings(t, db, fmt.Sprintf(`SELECT
    IF(ai_price_miss IS NULL, 'NULL', IF(ai_price_miss, '1', '0')),
    IFNULL(ai_price_warn, 'NULL')
FROM %s.bfe_ai_request_log WHERE logid = %d`, dbName, c.logid))
		if row[0] != c.wantMiss || row[1] != c.wantW {
			t.Errorf("logid=%d 缺价两列 = %q/%q，期望 %q/%q", c.logid, row[0], row[1], c.wantMiss, c.wantW)
		}
	}
}

// TestTC05MetricsRebuild 重建模式：DROP MV → 按仓库 DDL 重建（REPLACE 语义，
// SR 无 Doris 原子换名）→ 与 CH 不同，SR 异步 MV 创建后自动从基表全量回填
// （无需 CH 侧手工 INSERT..SELECT 重算）→ 口径与重建前逐项一致。
func TestTC05MetricsRebuild(t *testing.T) {
	db := common.TestDB(t)
	vars := common.DefaultVars(common.NewDBName(t))
	dbName := common.NewTestDB(t, db, vars)
	common.SetupFreshInstall(t, db, dbName, vars, true /*withMV*/, false /*withRoutineLoad*/)

	// 1) 写入三行样例等价数据并等首次刷新聚合
	tsMin := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	insertDemoRows(t, db, dbName, tsMin)
	waitMVRequestCount(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"), 3)
	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))

	// 2) 重建：DROP MV 后按仓库 DDL 重建（cleanup.sh 逆序：MV 先于明细表删除）
	common.ExecIn(t, db, dbName, "DROP MATERIALIZED VIEW bfe_ai_metrics_1m")
	if common.MVActive(t, db, dbName, "bfe_ai_metrics_1m") {
		t.Fatal("DROP MATERIALIZED VIEW 后 MV 仍可见")
	}
	common.ExecFileIn(t, db, dbName, common.SQLPath(t, "bfe_ai_metrics_1m.sql"), vars)
	if !common.MVActive(t, db, dbName, "bfe_ai_metrics_1m") {
		t.Fatal("重建后 MV 不存在或 is_active=false")
	}

	// 3) MV 创建后自动全量回填基表数据：轮询至聚合行重现（即 SR 侧"重算"）
	waitMVRequestCount(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"), 3)

	// 4) 重建后口径不变：与重建前同一套断言
	assertAggregation(t, db, dbName, tsMin.Format("2006-01-02 15:04:05"))
}

// insertDemoRows 向明细表写入三行样例等价数据（取值与 demo/*.json 一致），
// 落在 base 分钟桶的 10s/20s/30s，聚合后 ts_min = base 单桶。
// 列清单以标量列为主：ai_rate_limit_hits 在 SR 侧为 VARCHAR(JSON 文本)，
// 直插时写 JSON 文本字面量——rate_limit_hits 指标由 MV 内 parse_json 还原数组后
// 取长度推导，必须真实写入；其余嵌套列不参与聚合口径断言，留给 TC02 真实消费路径。
// logid 取 demoCases 的 Int64 域值（SR 对超界 BIGINT 字面量直接报错，见 demoCases 注释）。
// 行 A：normal 样例（缓存命中 + 意图分类 + 镜像命中，token 34/173/207）
// 行 B：rate_limit 样例（限流打平 rlp-0002/tpm/tpm1，err_code=AI_RATE_LIMIT，input_tokens=61）
// 行 C：auth_reject 样例（认证拒绝，quota 计划 plan_basic/plan_pro，err_code=AI_AUTH_REJECT）
func insertDemoRows(t *testing.T, db *sql.DB, dbName string, base time.Time) {
	t.Helper()
	fmtT := func(d time.Duration) string {
		return base.Add(d).Format("2006-01-02 15:04:05")
	}
	common.ExecIn(t, db, dbName, fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, ai_target_model,
     product, cluster, sub_cluster, backend_info, method, res_status_code, err_code, header_host,
     level1Name, level1, level2Name, level2, level3Name, level3,
     ai_cache_status, mirror_hit, ai_intent_answer, ai_intent_confidence,
     ai_input_tokens, ai_output_tokens, ai_total_tokens, ai_ttft_us, ai_tpot_us,
     req_header_len, res_header_len, res_body_len,
     all_time, cluster_serve_time, backend_serve_time, logid)
VALUES
    ('yyxdev_4026531840', '%s', 'TESTKEY_ID', 'test-model', 'gpt-5',
     'example_ai_product', 'ai_cluster_example', 'ai.pool.bj', '172.27.152.27:8081', 'POST', 200, '', 'ai.example.org',
     'dep0', 'rd', 'dep2', 'teama', 'dep3', 'yyx',
     'hit', 1, 'intent-a-1', 0.95,
     34, 173, 207, 6287, 3,
     184, 154, 445,
     10, 6, 6, %s)`,
		fmtT(10*time.Second), demoCases[0].logid))

	common.ExecIn(t, db, dbName, fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, ai_requested_model, ai_target_model,
     product, cluster, sub_cluster, backend_info, method, res_status_code, err_code, header_host,
     level1Name, level1, level2Name, level2,
     rate_limit_policy_id, rate_limit_type, rate_limit_rule_name, ai_rate_limit_hits,
     ai_input_tokens, req_header_len, res_header_len, res_body_len, all_time, logid)
VALUES
    ('yyxdev_4026531840', '%s', 'TESTKEY_ID', 'test-model', 'gpt-5',
     'example_ai_product', 'ai_cluster_example', 'ai.pool.bj', '', 'POST', 429, 'AI_RATE_LIMIT', 'ai.example.org',
     'dep0', 'rd', 'dep2', 'teama',
     'rlp-0002', 'tpm', 'tpm1',
     '[{"rate_limit_policy_id":"rlp-0002","rate_limit_type":"tpm","rule_names":["tpm1"]}]',
     61, 184, 137, 103, 4, %s)`,
		fmtT(20*time.Second), demoCases[1].logid))

	common.ExecIn(t, db, dbName, fmt.Sprintf(`INSERT INTO bfe_ai_request_log
    (hostid, log_time, ai_apikey_id, product, cluster, sub_cluster,
     method, res_status_code, err_code, header_host,
     ai_auth_reject_reason, ai_auth_reject_quota_plans,
     req_header_len, res_header_len, res_body_len, all_time, logid)
VALUES
    ('yyxdev_4026531840', '%s', 'invalid-key', 'example_ai_product', 'ai_cluster_example', 'ai.pool.bj',
     'POST', 401, 'AI_AUTH_REJECT', 'ai.example.org',
     'apikey not found', ['plan_basic', 'plan_pro'],
     150, 100, 50, 2, %s)`,
		fmtT(30*time.Second), demoCases[2].logid))
}

// waitMVRequestCount 轮询至 MV 中 ts_min 桶的 SUM(request_count) 达到 want
// （异步 MV 每分钟触发一次刷新，且增量刷新可能分批可见——必须等到全量行数到位，
// 不能以"出现聚合行"为信号；deadline 放宽到 240s）。
func waitMVRequestCount(t *testing.T, db *sql.DB, dbName, tsMin string, want int) {
	t.Helper()
	common.WaitFor(t, fmt.Sprintf("MV %s 桶聚合到 %d 行", tsMin, want), 240*time.Second, 5*time.Second, func() bool {
		var n int
		if err := db.QueryRow(fmt.Sprintf(
			`SELECT CAST(SUM(request_count) AS SIGNED) FROM %s.bfe_ai_metrics_1m WHERE ts_min = '%s'`,
			dbName, tsMin)).Scan(&n); err != nil {
			return false
		}
		return n >= want
	})
}

// waitColumn 轮询至 information_schema 中列可见（SR Schema Change 异步生效）。
func waitColumn(t *testing.T, db *sql.DB, dbName, table, column string) {
	t.Helper()
	common.WaitFor(t, fmt.Sprintf("列 %s.%s 可见", table, column), 60*time.Second, time.Second, func() bool {
		return len(common.ColumnTypes(t, db, dbName, table, []string{column})) == 1
	})
}

// assertAggregation 断言 ts_min 桶的聚合结果（MV 行已是最终聚合值，
// GROUP BY ts_min + SUM() 仅为聚合查询范式，无需 CH 侧 merge 兜底）。
// 40 维中断言关键子集：整窗指标、缓存状态、限流三列、镜像命中、意图答案、配额计划槽位。
func assertAggregation(t *testing.T, db *sql.DB, dbName, tsMin string) {
	t.Helper()

	// 1) 整窗 24 指标中的关键子集
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

	// 4b) 流式维度：直插行未写 ai_stream（NULL），MV 的 COALESCE(ai_stream,0) 归到 0 维度——
	//     维度列在 BOOLEAN 基列之上仍为 tinyint，报表 stream 过滤口径不受布尔类型影响
	if got := aggBreakdown(t, db, dbName, tsMin, "ai_stream"); !eqMap(got, map[string]string{"0": "3"}) {
		t.Errorf("ai_stream 维度分布 = %v，期望 map[0:3]", got)
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

// aggTotals 返回 ts_min 桶的整窗 SUM(指标)（GROUP BY ts_min）。
func aggTotals(t *testing.T, db *sql.DB, dbName, tsMin string) map[string]string {
	t.Helper()
	keys := []string{"request_count", "error_count", "auth_reject_count", "input_tokens",
		"output_tokens", "total_tokens", "rate_limit_hits", "cache_read_tokens", "cache_write_tokens"}
	sums := make([]string, len(keys))
	for i, k := range keys {
		sums[i] = "CAST(SUM(" + k + ") AS CHAR)"
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
	rows, err := db.Query(fmt.Sprintf(`SELECT %s, CAST(SUM(request_count) AS CHAR) FROM %s.bfe_ai_metrics_1m
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
