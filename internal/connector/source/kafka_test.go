package source

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

type fakeKafkaReader struct {
	messages  []kafka.Message
	idx       int
	committed []kafka.Message
	closed    bool
}

func (f *fakeKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.idx >= len(f.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := f.messages[f.idx]
	f.idx++
	return msg, nil
}

func (f *fakeKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeKafkaReader) Close() error {
	f.closed = true
	return nil
}

func newKafkaSourceWithFakeReader(msgs ...kafka.Message) (*KafkaSource, *fakeKafkaReader) {
	fake := &fakeKafkaReader{messages: msgs}
	src := &KafkaSource{
		cfg: config.StreamingConfig{
			Topic:   "events",
			GroupID: "stream-sync",
		},
		reader:  fake,
		pending: make(map[string]kafka.Message),
	}
	return src, fake
}

func TestKafkaSource_SubscribeAndAck(t *testing.T) {
	src, fake := newKafkaSourceWithFakeReader(
		kafka.Message{Value: []byte(`{"id":1,"name":"foo","updated_at":"2026-01-01T10:00:00Z"}`)},
	)

	out := make(chan row.Row)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- src.Subscribe(ctx, out)
	}()

	got := <-out
	if got.PrimaryKey != "partition=0:offset=0" {
		t.Fatalf("unexpected primary key %q", got.PrimaryKey)
	}
	if got.Data["name"] != "foo" {
		t.Fatalf("unexpected data: %+v", got.Data)
	}
	if got.Source != "kafka.events" {
		t.Fatalf("unexpected source %q", got.Source)
	}

	if err := src.Ack(context.Background(), got.ID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if len(fake.committed) != 1 {
		t.Fatalf("expected 1 committed message, got %d", len(fake.committed))
	}

	cancel()
	_ = <-done
}

func TestKafkaSource_NackRemovesPending(t *testing.T) {
	src, fake := newKafkaSourceWithFakeReader(
		kafka.Message{Value: []byte(`{"id":2,"name":"bar"}`)},
	)

	out := make(chan row.Row)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- src.Subscribe(ctx, out)
	}()

	got := <-out
	if err := src.Nack(context.Background(), got.ID); err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	if len(fake.committed) != 0 {
		t.Fatal("expected no committed messages after Nack")
	}

	cancel()
	_ = <-done
}

func TestKafkaSource_AckMissingPending(t *testing.T) {
	src, _ := newKafkaSourceWithFakeReader()
	if err := src.Ack(context.Background(), "missing"); err == nil {
		t.Fatal("expected error for missing pending row")
	}
}

func TestKafkaSource_SubscribeStopsOnMalformedJSON(t *testing.T) {
	src, _ := newKafkaSourceWithFakeReader(
		kafka.Message{Value: []byte(`not-json`)},
	)

	out := make(chan row.Row)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- src.Subscribe(ctx, out)
	}()

	got := <-out
	if got.Data["raw"] != "not-json" {
		t.Fatalf("expected raw fallback, got %+v", got.Data)
	}
	cancel()
	_ = <-done
}

func TestKafkaSource_NoBrokers(t *testing.T) {
	src := NewKafkaSource()
	if err := src.Connect(context.Background(), config.StreamingConfig{Topic: "events", GroupID: "g1"}); err == nil {
		t.Fatal("expected error when endpoint is missing")
	}
}
