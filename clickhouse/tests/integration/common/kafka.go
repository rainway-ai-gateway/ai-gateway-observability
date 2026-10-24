package common

// Kafka 生产辅助（TC02 真实消费链路使用）：
//
//	brokers := RequireKafka(t)                 // Kafka 不可达时 Skip（不视为失败）
//	EnsureTopic(t, brokers, topic)             // 建独立临时 topic（4 分区，对齐 kafka_num_consumers=3）
//	ProduceFiles(t, brokers, topic, files...)  // 生产 demo 样例消息
//
// 与 doris-it 的差异：doris-it 不生产 Kafka 消息（其 TC02 仅校验 Routine Load
// 任务创建）；ClickHouse 侧的打平发生在 MV 消费 Kafka 的路径上，必须走真实
// 生产 → Kafka 引擎 → 消费 MV 链路才能验证。
// 注意：测试 topic 只做创建不做删除（原因见文件尾注释），命名唯一、体积极小。

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaBrokers 返回 Kafka broker 列表（KAFKA_BROKER_LIST 环境变量，逗号分隔）。
func KafkaBrokers() []string {
	out := []string{}
	for _, b := range strings.Split(envOr("KAFKA_BROKER_LIST", "127.0.0.1:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// RequireKafka 检查 Kafka 可达；不可达时 Skip（提示启动方式）。
func RequireKafka(t *testing.T) []string {
	t.Helper()
	brokers := KafkaBrokers()
	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", brokers[0])
	if err != nil {
		t.Skipf("Kafka 不可达 (%v)，跳过真实消费链路用例；请先执行: wsl bash /mnt/d/kafka/bin/start-kafka.sh", err)
	}
	_ = conn.Close()
	return brokers
}

func dialKafka(t *testing.T, brokers []string) *kafka.Conn {
	t.Helper()
	dialer := &kafka.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("连接 Kafka 失败 %s: %v", brokers[0], err)
	}
	return conn
}

// EnsureTopic 创建 topic（已存在时忽略）。分区数取 4：对齐 SQL 资产中
// kafka_num_consumers = 3——测试 topic 若分区数小于消费者数，多出的消费者空转，
// ClickHouse 侧会持续 "Got empty assignment" 且消费不稳定。
func EnsureTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn := dialKafka(t, brokers)
	defer conn.Close()
	err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 4, ReplicationFactor: 1})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		t.Fatalf("创建 Kafka topic %s 失败: %v", topic, err)
	}
}

// DeleteTopic 故意不提供：本机测试 Kafka 的数据目录位于 /mnt/d（WSL 9p 文件系统），
// 删除 topic 时 broker 对分区目录的 rename（标记 *.stray）会 AccessDenied，
// 导致 log dir failed 并使整个 broker 宕机。测试 topic 命名唯一（纳秒后缀）、
// 仅 3 条消息，直接保留不做清理。
// ProduceFiles 将一个或多个 JSON 文件生产到 topic（生产前按 JSON 原文压缩空白，
// 去掉多行缩进——JSONEachRow 要求单行一条消息，等价真实 log-reader 的产出格式；
// json.Compact 保留数字字面量不变）。
func ProduceFiles(t *testing.T, brokers []string, topic string, files ...string) {
	t.Helper()
	payloads := make([][]byte, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("读取消息文件失败 %s: %v", f, err)
		}
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, raw); err != nil {
			t.Fatalf("压缩 demo JSON 失败 %s（JSONEachRow 需要单行消息）: %v", f, err)
		}
		payloads = append(payloads, compacted.Bytes())
	}
	ProduceMessages(t, brokers, topic, payloads...)
}

// ProduceMessages 将准备好的消息体逐条生产到 topic（纯传输，不做内容改写）。
func ProduceMessages(t *testing.T, brokers []string, topic string, payloads ...[]byte) {
	t.Helper()
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireOne,
	}
	defer w.Close()
	msgs := make([]kafka.Message, 0, len(payloads))
	for _, p := range payloads {
		cp := make([]byte, len(p))
		copy(cp, p)
		msgs = append(msgs, kafka.Message{Value: cp})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("生产 Kafka 消息失败 topic=%s: %v", topic, err)
	}
}

// DeleteTopic 故意不提供：本机测试 Kafka 的数据目录位于 /mnt/d（WSL 9p 文件系统），
// 删除 topic 时 broker 对分区目录的 rename（标记 *.stray）会 AccessDenied，
// 导致 log dir failed 并使整个 broker 宕机。测试 topic 命名唯一（纳秒后缀）、
// 仅 3 条消息，直接保留不做清理。
