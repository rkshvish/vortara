package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	destpkg "github.com/rkshvish/vortaraos/internal/connector/destination"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/router"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/internal/steps"
	conncfg "github.com/rkshvish/vortaraos/pkg/config"
	v2 "github.com/rkshvish/vortaraos/pkg/config/v2"
	"github.com/rkshvish/vortaraos/pkg/row"
)

var registerV2MocksOnce sync.Once

func registerV2Mocks() {
	registerV2MocksOnce.Do(func() {
		registry.RegisterBatchSource("v2test-batch", func() any { return currentV2BatchSource })
		registry.RegisterStreamingSource("v2test-stream", func() any { return currentV2StreamSource })
		registry.RegisterDestination("v2test-dest", func() any {
			v2DestMu.Lock()
			defer v2DestMu.Unlock()
			if v2DestIndex >= len(currentV2Dests) {
				return &mockV2Destination{}
			}
			d := currentV2Dests[v2DestIndex]
			v2DestIndex++
			return d
		})
	})
}

var (
	currentV2BatchSource  *mockV2BatchSource
	currentV2StreamSource *mockV2StreamSource
	currentV2Dests        []*mockV2Destination
	v2DestMu              sync.Mutex
	v2DestIndex           int
)

type mockV2BatchSource struct {
	rows []row.Row
	mu   sync.Mutex
}

func (m *mockV2BatchSource) Connect(ctx context.Context, cfg conncfg.SourceConfig) error { return nil }

func (m *mockV2BatchSource) Extract(ctx context.Context, wm time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	defer close(out)
	for _, r := range m.rows {
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *mockV2BatchSource) GetWatermarkColumn() string { return "updated_at" }
func (m *mockV2BatchSource) Close() error               { return nil }

type mockV2StreamSource struct {
	rows  []row.Row
	mu    sync.Mutex
	acks  []string
	nacks []string
}

func (m *mockV2StreamSource) Connect(ctx context.Context, cfg conncfg.StreamingConfig) error {
	return nil
}

func (m *mockV2StreamSource) Subscribe(ctx context.Context, out chan<- row.Row) error {
	for _, r := range m.rows {
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockV2StreamSource) Ack(ctx context.Context, rowID string) error {
	m.mu.Lock()
	m.acks = append(m.acks, rowID)
	m.mu.Unlock()
	return nil
}

func (m *mockV2StreamSource) Nack(ctx context.Context, rowID string) error {
	m.mu.Lock()
	m.nacks = append(m.nacks, rowID)
	m.mu.Unlock()
	return nil
}

func (m *mockV2StreamSource) Close() error { return nil }

type mockV2Destination struct {
	mu     sync.Mutex
	rows   []row.Row
	calls  int
	target int
	done   chan struct{}
}

func (m *mockV2Destination) Connect(ctx context.Context, cfg conncfg.DestinationConfig) error {
	return nil
}

func (m *mockV2Destination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (destpkg.LoadResult, error) {
	m.mu.Lock()
	m.calls++
	m.rows = append(m.rows, rows...)
	if m.done != nil && m.target > 0 && len(m.rows) >= m.target {
		select {
		case <-m.done:
		default:
			close(m.done)
		}
	}
	m.mu.Unlock()
	return destpkg.LoadResult{Loaded: len(rows)}, nil
}

func (m *mockV2Destination) Close() error { return nil }

func TestRouter_V2_FanOut(t *testing.T) {
	registerV2Mocks()
	dest0 := &mockV2Destination{target: 2, done: make(chan struct{})}
	dest1 := &mockV2Destination{target: 1, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest0, dest1}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"status": "won"}, time.Now().UTC()),
			row.NewRow("src", "pipe", "pk2", map[string]interface{}{"status": "lost"}, time.Now().UTC()),
		},
	}

	cfg := &v2.PipelineConfig{
		Name: "v2-fanout",
		Source: v2.SourceConfig{
			Type:      "v2test-batch",
			Table:     "deals",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []v2.DestinationConfig{
			{Type: "v2test-dest"},
			{Type: "v2test-dest", When: "status == 'won'"},
		},
		Settings: v2.SettingsConfig{Concurrency: v2.ConcurrencySettings{Workers: 2, BatchSize: 10}},
	}

	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest0, dest1}, "v2test-batch"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := len(dest0.rows); got != 2 {
		t.Fatalf("dest0 rows = %d, want 2", got)
	}
	if got := len(dest1.rows); got != 1 {
		t.Fatalf("dest1 rows = %d, want 1", got)
	}
}

func TestEngine_RunV2_BatchOnly(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 2, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"status": "won"}, time.Now().UTC()),
			row.NewRow("src", "pipe", "pk2", map[string]interface{}{"status": "lost"}, time.Now().UTC()),
		},
	}

	cfg := &v2.PipelineConfig{
		Name: "batch-only",
		Source: v2.SourceConfig{
			Type:      "v2test-batch",
			Table:     "deals",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Destinations: []v2.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     v2.SettingsConfig{Concurrency: v2.ConcurrencySettings{Workers: 2, BatchSize: 10}},
	}

	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	if err := eng.runBatchOnce(context.Background(), cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := len(dest.rows); got != 2 {
		t.Fatalf("dest rows = %d, want 2", got)
	}
}

func TestEngine_RunV2_StreamingOnly(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 1, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2StreamSource = &mockV2StreamSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"status": "won"}, time.Now().UTC()),
		},
	}

	cfg := &v2.PipelineConfig{
		Name: "stream-only",
		Source: v2.SourceConfig{
			Type:    "v2test-stream",
			Topic:   "events",
			GroupID: "group",
		},
		Destinations: []v2.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     v2.SettingsConfig{Concurrency: v2.ConcurrencySettings{Workers: 1, BatchSize: 1}},
	}

	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- eng.runStreaming(ctx, cfg, currentV2StreamSource, proc, rt, []destpkg.Destination{dest}, "v2test-stream")
	}()
	select {
	case <-dest.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for streaming delivery")
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestEngine_RunV2_BatchAndStreaming(t *testing.T) {
	registerV2Mocks()
	dest := &mockV2Destination{target: 2, done: make(chan struct{})}
	currentV2Dests = []*mockV2Destination{dest}
	v2DestIndex = 0
	currentV2BatchSource = &mockV2BatchSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk1", map[string]interface{}{"status": "won"}, time.Now().UTC()),
		},
	}
	currentV2StreamSource = &mockV2StreamSource{
		rows: []row.Row{
			row.NewRow("src", "pipe", "pk2", map[string]interface{}{"status": "lost"}, time.Now().UTC()),
		},
	}

	cfg := &v2.PipelineConfig{
		Name: "both",
		Source: v2.SourceConfig{
			Type:      "v2test-batch",
			Table:     "deals",
			Watermark: "updated_at",
			BatchSize: 10,
		},
		Also:         &v2.AlsoConfig{Type: "v2test-stream", Topic: "events", GroupID: "group"},
		Destinations: []v2.DestinationConfig{{Type: "v2test-dest"}},
		Settings:     v2.SettingsConfig{Concurrency: v2.ConcurrencySettings{Workers: 1, BatchSize: 1}},
	}

	eng := NewEngine(state.NewMemoryStore())
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		t.Fatalf("router.New() error = %v", err)
	}
	proc, err := steps.New(cfg.Transform)
	if err != nil {
		t.Fatalf("steps.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 2)
	go func() {
		errCh <- eng.runBatchOnce(ctx, cfg, currentV2BatchSource, proc, rt, []destpkg.Destination{dest}, "v2test-batch")
	}()
	go func() {
		errCh <- eng.runStreaming(ctx, cfg, currentV2StreamSource, proc, rt, []destpkg.Destination{dest}, "v2test-stream")
	}()
	select {
	case <-dest.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for both pipelines")
	}
	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("batch/stream error = %v", err)
	}
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("batch/stream error = %v", err)
	}
}
