package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

type kafkaReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

// KafkaSource implements StreamingSource for Kafka consumers.
type KafkaSource struct {
	cfg     config.StreamingConfig
	reader  kafkaReader
	pending map[string]kafka.Message
	mu      sync.Mutex
}

var _ StreamingSource = (*KafkaSource)(nil)

func init() {
	registry.RegisterStreamingSource("kafka", func() any {
		return NewKafkaSource()
	})
}

// NewKafkaSource returns a new KafkaSource.
func NewKafkaSource() *KafkaSource {
	return &KafkaSource{pending: make(map[string]kafka.Message)}
}

// Connect opens a Kafka reader using the provided streaming config.
func (k *KafkaSource) Connect(ctx context.Context, cfg config.StreamingConfig) error {
	brokerSpec := strings.TrimSpace(cfg.Broker)
	if brokerSpec == "" {
		brokerSpec = strings.TrimSpace(cfg.Endpoint)
	}
	if brokerSpec == "" {
		return errors.New("kafka source: broker is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return errors.New("kafka source: topic is required")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		if cfg.Options != nil {
			cfg.GroupID = strings.TrimSpace(cfg.Options["group_id"])
		}
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return errors.New("kafka source: group_id is required")
	}

	brokers, err := parseBrokers(brokerSpec)
	if err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.LastOffset,
	})

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		_ = reader.Close()
		return err
	}
	_ = conn.Close()

	k.cfg = cfg
	k.reader = reader
	return nil
}

// Subscribe starts consuming Kafka messages and sends them to out.
// Subscribe does not close out; the caller owns it.
func (k *KafkaSource) Subscribe(ctx context.Context, out chan<- row.Row) error {
	if k.reader == nil {
		return errors.New("kafka source: not connected")
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return err
		}

		payload := parseKafkaPayload(msg.Value)
		wm := kafkaWatermark(payload)
		result := row.NewRow(
			k.sourceName(),
			k.pipelineName(),
			fmt.Sprintf("partition=%d:offset=%d", msg.Partition, msg.Offset),
			payload,
			wm,
		)

		k.mu.Lock()
		k.pending[result.ID] = msg
		k.mu.Unlock()

		select {
		case out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Ack commits a Kafka message after successful delivery.
func (k *KafkaSource) Ack(ctx context.Context, rowID string) error {
	k.mu.Lock()
	msg, ok := k.pending[rowID]
	if !ok {
		k.mu.Unlock()
		return errors.New("unknown rowID")
	}
	if err := k.reader.CommitMessages(ctx, msg); err != nil {
		k.mu.Unlock()
		return err
	}
	delete(k.pending, rowID)
	k.mu.Unlock()
	return nil
}

// Nack discards the pending message without committing it.
func (k *KafkaSource) Nack(ctx context.Context, rowID string) error {
	k.mu.Lock()
	delete(k.pending, rowID)
	k.mu.Unlock()
	return nil
}

// Close shuts down the Kafka reader gracefully.
func (k *KafkaSource) Close() error {
	if k.reader == nil {
		return nil
	}
	return k.reader.Close()
}

func (k *KafkaSource) sourceName() string {
	if k.cfg.Topic == "" {
		return "kafka"
	}
	return "kafka." + k.cfg.Topic
}

func (k *KafkaSource) pipelineName() string {
	if name := strings.TrimSpace(k.cfg.GroupID); name != "" {
		return name
	}
	return ""
}

func parseBrokers(endpoint string) ([]string, error) {
	parts := strings.Split(endpoint, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			continue
		}
		host, port, err := net.SplitHostPort(broker)
		if err != nil || host == "" || port == "" {
			return nil, fmt.Errorf("kafka source: invalid broker %q", broker)
		}
		brokers = append(brokers, broker)
	}
	if len(brokers) == 0 {
		return nil, errors.New("kafka source: no brokers configured")
	}
	return brokers, nil
}

func parseKafkaPayload(value []byte) map[string]interface{} {
	dec := json.NewDecoder(strings.NewReader(string(value)))
	dec.UseNumber()

	var payload map[string]interface{}
	if err := dec.Decode(&payload); err != nil {
		return map[string]interface{}{"raw": string(value)}
	}
	return payload
}

func kafkaWatermark(payload map[string]interface{}) time.Time {
	if v, ok := payload["updated_at"]; ok {
		if ts, err := parseRESTTime(v); err == nil {
			return ts
		}
	}
	return time.Now().UTC()
}
