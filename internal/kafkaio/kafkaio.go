// Package kafkaio wraps segmentio/kafka-go with the specific reader/writer
// configuration this project needs, so callers (the CLI and the test
// harness) don't have to repeat broker/topic/group wiring.
package kafkaio

import (
	"context"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// NewReader returns a consumer-group reader for one topic. StartOffset is
// irrelevant here because CommitInterval+group membership drive offsets
// once a group has committed at least once; FirstOffset only matters for a
// brand-new group with no committed offset, which is the common case for
// this monitor (it's meant to run continuously, not backfill history).
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
}

// NewWriter returns a writer for one topic, used by the demo/test event
// generator. RequiredAcks=1 (leader ack only) is a deliberate choice for a
// synthetic load generator — durability past leader failure isn't the point
// here, producing a reproducible rate of events is.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 50 * time.Millisecond,
	}
}

// EnsureTopic creates a topic if it doesn't already exist. Kafka's own
// auto.create.topics.enable would normally handle this, but relying on it
// means the first few produced messages can race topic creation; creating
// explicitly up front avoids that flakiness in tests and in the demo.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", controller.Host+":"+strconv.Itoa(controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}
