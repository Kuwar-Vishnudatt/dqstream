package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/dqstream/internal/alert"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/event"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/kafkaio"
	"github.com/Kuwar-Vishnudatt/dqstream/internal/monitor"
	kafka "github.com/segmentio/kafka-go"
)

func publish(t *testing.T, writer *kafka.Writer, eventTime time.Time, fields map[string]any) {
	t.Helper()
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := event.Envelope{EventTime: eventTime, Payload: payload}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func drainInto(t *testing.T, reader *kafka.Reader, m *monitor.Monitor, want int, timeout time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	got := 0
	for got < want {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("expected %d messages, read %d before error: %v", want, got, err)
		}
		rec, err := event.Decode(msg.Value)
		if err != nil {
			t.Fatalf("decode message: %v", err)
		}
		m.Observe(rec)
		got++
	}
	return got
}

// TestRealKafkaProduceConsumeDetectsSchemaDrift proves the full pipeline —
// real producer, real broker, real consumer-group reader, decode, monitor —
// correctly detects a new field appearing mid-stream, not just the
// in-process unit tests.
func TestRealKafkaProduceConsumeDetectsSchemaDrift(t *testing.T) {
	broker := StartBroker(t)
	topic := UniqueTopic("orders")

	if err := kafkaio.EnsureTopic(context.Background(), []string{broker.Addr}, topic, 1); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	writer := kafkaio.NewWriter([]string{broker.Addr}, topic)
	defer writer.Close()
	reader := kafkaio.NewReader([]string{broker.Addr}, topic, "dqstream-test")
	defer reader.Close()

	mem := alert.NewMemorySink()
	m := monitor.New(monitor.Config{Topic: topic, SchemaWarmupEvents: 10, VolumeMinWarmup: 100, NullRateMinWarmup: 100}, mem)

	now := time.Now()
	for i := 0; i < 10; i++ {
		publish(t, writer, now, map[string]any{"id": "1", "status": "ok", "amount": float64(5)})
	}
	drainInto(t, reader, m, 10, 20*time.Second)
	m.Flush()
	if len(mem.Alerts()) != 0 {
		t.Fatalf("expected no alerts after clean warmup, got %v", mem.Alerts())
	}

	publish(t, writer, now, map[string]any{"id": "2", "status": "ok", "amount": float64(5), "coupon": "SAVE5"})
	drainInto(t, reader, m, 1, 20*time.Second)
	m.Flush()

	found := false
	for _, a := range mem.Alerts() {
		if a.Check == "schema" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a schema drift alert for the new 'coupon' field via real Kafka, got %v", mem.Alerts())
	}
}

// TestConsumerGroupResumesFromCommittedOffsetAfterRestart proves the
// consumer-group offset mechanics actually work against a real broker: a
// reader that reads some messages, is closed (simulating a restart), and a
// NEW reader in the same group picks up from where the first left off
// rather than re-reading from the start or skipping ahead.
func TestConsumerGroupResumesFromCommittedOffsetAfterRestart(t *testing.T) {
	broker := StartBroker(t)
	topic := UniqueTopic("resume")
	group := "dqstream-resume-test"

	if err := kafkaio.EnsureTopic(context.Background(), []string{broker.Addr}, topic, 1); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	writer := kafkaio.NewWriter([]string{broker.Addr}, topic)
	defer writer.Close()

	now := time.Now()
	for i := 0; i < 20; i++ {
		publish(t, writer, now, map[string]any{"n": float64(i)})
	}

	reader1 := kafkaio.NewReader([]string{broker.Addr}, topic, group)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	readFirstHalf := 0
	for readFirstHalf < 10 {
		if _, err := reader1.ReadMessage(ctx); err != nil {
			cancel()
			t.Fatalf("reader1 read failed at %d: %v", readFirstHalf, err)
		}
		readFirstHalf++
	}
	cancel()
	// CommitInterval is async (1s); give the in-flight commit a moment to
	// land before closing, otherwise this test would be asserting on
	// coincidence rather than on offset commit actually having happened.
	time.Sleep(1500 * time.Millisecond)
	if err := reader1.Close(); err != nil {
		t.Fatalf("close reader1: %v", err)
	}

	reader2 := kafkaio.NewReader([]string{broker.Addr}, topic, group)
	defer reader2.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	msg, err := reader2.ReadMessage(ctx2)
	if err != nil {
		t.Fatalf("reader2 read failed: %v", err)
	}
	var env event.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var payload map[string]float64
	json.Unmarshal(env.Payload, &payload)
	if payload["n"] != 10 {
		t.Fatalf("expected reader2 to resume at n=10 (offset 10), got n=%v", payload["n"])
	}
}
