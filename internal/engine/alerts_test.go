package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	pipeline "github.com/rkshvish/vortara/pkg/config/pipeline"
)

func TestSendFailureAlert(t *testing.T) {
	var mu sync.Mutex
	var payloads []failureAlert
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p failureAlert
		_ = json.Unmarshal(body, &p)
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &pipeline.PipelineConfig{
		Name:   "alert-test",
		Alerts: &pipeline.AlertsConfig{OnFailure: &pipeline.AlertTarget{WebhookURL: srv.URL}},
	}
	sendFailureAlert(context.Background(), cfg, errors.New("boom"))

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 1 {
		t.Fatalf("payloads = %d, want 1", len(payloads))
	}
	p := payloads[0]
	if p.Pipeline != "alert-test" || p.Status != "failed" || p.Error != "boom" || p.FailedAt.IsZero() {
		t.Fatalf("payload = %+v", p)
	}
}

func TestSendFailureAlert_NoConfig(t *testing.T) {
	// Must not panic and must not send anything when alerts are absent.
	sendFailureAlert(context.Background(), &pipeline.PipelineConfig{Name: "x"}, errors.New("boom"))
	sendFailureAlert(context.Background(), nil, errors.New("boom"))
	sendFailureAlert(context.Background(), &pipeline.PipelineConfig{
		Name:   "x",
		Alerts: &pipeline.AlertsConfig{OnFailure: &pipeline.AlertTarget{WebhookURL: "http://127.0.0.1:1"}},
	}, nil)
}
