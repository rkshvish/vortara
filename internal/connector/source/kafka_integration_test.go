//go:build integration

package source

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

func TestKafkaSource_IntegrationConnect(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("KAFKA_BROKER"))
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if broker == "" || topic == "" {
		t.Skip("set KAFKA_BROKER and KAFKA_TOPIC to run Kafka integration tests")
	}

	src := NewKafkaSource()
	if err := src.Connect(context.Background(), config.StreamingConfig{
		Endpoint: broker,
		Topic:    topic,
		Options: map[string]string{
			"group_id": "vortara-integration",
		},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	_ = src.Close()
}

func TestKafkaSource_IntegrationAckFlow(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("KAFKA_BROKER"))
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if broker == "" || topic == "" {
		t.Skip("set KAFKA_BROKER and KAFKA_TOPIC to run Kafka integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafka.Message{Value: []byte(`{"id":1,"name":"foo","updated_at":"2026-01-01T10:00:00Z"}`)}); err != nil {
		t.Fatalf("WriteMessages() error = %v", err)
	}

	src := NewKafkaSource()
	if err := src.Connect(ctx, config.StreamingConfig{
		Endpoint: broker,
		Topic:    topic,
		Options: map[string]string{
			"group_id": "vortara-integration",
		},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer src.Close()

	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Subscribe(ctx, out)
	}()

	select {
	case got := <-out:
		if got.PrimaryKey != "id=1" {
			t.Fatalf("unexpected primary key %q", got.PrimaryKey)
		}
		if err := src.Ack(ctx, got.ID); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	cancel()
	_ = <-done
}
