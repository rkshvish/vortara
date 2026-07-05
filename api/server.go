// Package api exposes health and metrics endpoints for a running Vortara process.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rkshvish/vortara/internal/engine"
)

// Version is stamped by the CLI at startup so /health and /version report
// the same build as vortara --version.
var Version = "dev"

type statsProvider interface {
	Stats(ctx context.Context) engine.PipelineStats
}

// Server serves health and metrics endpoints for one engine.
type Server struct {
	engine  statsProvider
	startAt time.Time
	port    int
	srv     *http.Server
	addr    string
	mu      sync.RWMutex
}

// NewServer creates a new API server for an engine.
func NewServer(eng *engine.Engine, port int) *Server {
	return newServer(eng, port)
}

func newServer(provider statsProvider, port int) *Server {
	if port == 0 {
		port = 9090
	}
	return &Server{
		engine:  provider,
		startAt: time.Now(),
		port:    port,
	}
}

// Start begins serving HTTP endpoints in the background.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", s.handlePing)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/version", s.handleVersion)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)

	addr := fmt.Sprintf("127.0.0.1:%d", s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.addr = ln.Addr().String()
	s.srv = &http.Server{Addr: s.addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = s.Stop()
	}()
	go func() {
		_ = s.srv.Serve(ln)
	}()
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.srv.Shutdown(shutdownCtx)
	s.srv = nil
	return err
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := s.stats(r.Context())
	payload := map[string]any{
		"status":         "ok",
		"version":        Version,
		"uptime_seconds": int64(time.Since(s.startAt).Seconds()),
		"pipelines": []map[string]any{
			{
				"name":             stats.Name,
				"mode":             stats.Mode,
				"status":           stats.Status,
				"last_run_at":      formatTime(stats.LastRunAt),
				"last_status":      stats.LastStatus,
				"duration_seconds": stats.LastRunDuration.Seconds(),
				"rows_loaded":      stats.RowsLoaded,
				"rows_errored":     stats.RowsErrored,
				"next_run_at":      formatTime(stats.NextRunAt),
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.stats(r.Context())
	lastDuration := stats.LastRunDuration.Seconds()
	pipelineUp := 0
	if stats.LastStatus == "success" {
		pipelineUp = 1
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP vortara_rows_loaded_total Total rows loaded to all destinations\n")
	fmt.Fprintf(w, "# TYPE vortara_rows_loaded_total counter\n")
	fmt.Fprintf(w, "vortara_rows_loaded_total{pipeline=%q} %d\n\n", stats.Name, stats.RowsLoaded)
	fmt.Fprintf(w, "# HELP vortara_rows_errored_total Total rows that failed to load\n")
	fmt.Fprintf(w, "# TYPE vortara_rows_errored_total counter\n")
	fmt.Fprintf(w, "vortara_rows_errored_total{pipeline=%q} %d\n\n", stats.Name, stats.RowsErrored)
	fmt.Fprintf(w, "# HELP vortara_rows_skipped_total Total rows skipped (already delivered)\n")
	fmt.Fprintf(w, "# TYPE vortara_rows_skipped_total counter\n")
	fmt.Fprintf(w, "vortara_rows_skipped_total{pipeline=%q} %d\n\n", stats.Name, stats.RowsSkipped)
	fmt.Fprintf(w, "# HELP vortara_last_run_duration_seconds Duration of last pipeline run\n")
	fmt.Fprintf(w, "# TYPE vortara_last_run_duration_seconds gauge\n")
	fmt.Fprintf(w, "vortara_last_run_duration_seconds{pipeline=%q} %.1f\n\n", stats.Name, lastDuration)
	fmt.Fprintf(w, "# HELP vortara_pipeline_up 1 if pipeline last run succeeded\n")
	fmt.Fprintf(w, "# TYPE vortara_pipeline_up gauge\n")
	fmt.Fprintf(w, "vortara_pipeline_up{pipeline=%q} %d\n", stats.Name, pipelineUp)
}

// handlePing is the liveness probe: the process is up.
func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("pong"))
}

// handleReady is the readiness probe: 200 once an engine is attached and its
// last run did not fail; 503 otherwise.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		http.Error(w, "no engine", http.StatusServiceUnavailable)
		return
	}
	stats := s.stats(r.Context())
	if stats.LastStatus == "failed" {
		http.Error(w, "last run failed", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"version": Version})
}

func (s *Server) stats(ctx context.Context) engine.PipelineStats {
	if s.engine == nil {
		return engine.PipelineStats{}
	}
	return s.engine.Stats(ctx)
}

func (s *Server) address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

func formatTime(t time.Time) any {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
