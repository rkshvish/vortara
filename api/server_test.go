package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rkshvish/vortara/internal/engine"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

type stubEngine struct {
	stats engine.SyncStats
}

func (s *stubEngine) Stats(_ context.Context, _ *synccfg.SyncSpec) engine.SyncStats {
	return s.stats
}

func TestServer_Health(t *testing.T) {
	srv := newServer(&stubEngine{stats: engine.SyncStats{Name: "deals-sync", Status: "running"}}, nil, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	srv.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("unexpected body: %+v", body)
	}
	if _, ok := body["syncs"]; !ok {
		t.Fatalf("expected syncs field: %+v", body)
	}
	syncs, ok := body["syncs"].([]any)
	if !ok || len(syncs) != 1 {
		t.Fatalf("expected one sync entry, got %+v", body["syncs"])
	}
	entry, ok := syncs[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map sync entry, got %#v", syncs[0])
	}
	if d, ok := entry["duration_seconds"].(float64); !ok || d < 0 {
		t.Fatalf("expected non-negative duration_seconds, got %#v", entry["duration_seconds"])
	}
}

func TestServer_Metrics(t *testing.T) {
	srv := newServer(&stubEngine{stats: engine.SyncStats{
		Name:            "deals-sync",
		RowsLoaded:      1234,
		RowsSkipped:     89,
		RowsErrored:     5,
		LastStatus:      "success",
		LastRunDuration: 12400 * time.Millisecond,
	}}, nil, 0)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	srv.handleMetrics(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Fatalf("unexpected content type %q", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "vortara_rows_loaded_total") {
		t.Fatalf("missing rows_loaded metric: %s", body)
	}
	if !strings.Contains(body, `pipeline="deals-sync"`) {
		t.Fatalf("missing pipeline label: %s", body)
	}
}

func TestServer_Health_SyncStatus(t *testing.T) {
	srv := newServer(&stubEngine{stats: engine.SyncStats{
		Name:       "deals-sync",
		Status:     "idle",
		LastStatus: "success",
	}}, nil, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	srv.handleHealth(rec, req)

	var body struct {
		Syncs []struct {
			LastStatus string `json:"last_status"`
		} `json:"syncs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(body.Syncs) != 1 || body.Syncs[0].LastStatus != "success" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestServer_Stop(t *testing.T) {
	srv := newServer(&stubEngine{stats: engine.SyncStats{Name: "pipe"}}, nil, 0)
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
}

func TestServer_Concurrent(t *testing.T) {
	srv := newServer(&stubEngine{stats: engine.SyncStats{Name: "pipe", Status: "idle"}}, nil, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			srv.handleHealth(rec, req.Clone(context.Background()))
			if rec.Code != http.StatusOK {
				errCh <- io.EOF
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal("expected all requests to succeed")
		}
	}
}
