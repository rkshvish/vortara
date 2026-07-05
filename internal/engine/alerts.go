package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	vlogger "github.com/rkshvish/vortaraos/internal/logger"
	v2cfg "github.com/rkshvish/vortaraos/pkg/config/v2"
)

var alertClient = &http.Client{Timeout: 10 * time.Second}

// failureAlert is the JSON payload POSTed to the on_failure webhook.
type failureAlert struct {
	Pipeline string    `json:"pipeline"`
	Status   string    `json:"status"`
	Error    string    `json:"error"`
	FailedAt time.Time `json:"failed_at"`
}

// sendFailureAlert POSTs a failure notification if alerts.on_failure is configured.
// Alert delivery is best-effort: failures are logged, never propagated.
func sendFailureAlert(ctx context.Context, cfg *v2cfg.PipelineConfig, runErr error) {
	if cfg == nil || runErr == nil || cfg.Alerts == nil || cfg.Alerts.OnFailure == nil {
		return
	}
	url := cfg.Alerts.OnFailure.WebhookURL
	if url == "" {
		return
	}
	l := vlogger.FromContext(ctx)
	payload, err := json.Marshal(failureAlert{
		Pipeline: cfg.Name,
		Status:   "failed",
		Error:    runErr.Error(),
		FailedAt: time.Now().UTC(),
	})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		l.Warn("failure alert: bad webhook request", slog.String("error", err.Error()))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := alertClient.Do(req)
	if err != nil {
		l.Warn("failure alert: webhook unreachable",
			slog.String("pipeline", cfg.Name),
			slog.String("error", err.Error()),
		)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		l.Warn("failure alert: webhook rejected",
			slog.String("pipeline", cfg.Name),
			slog.Int("status", resp.StatusCode),
		)
		return
	}
	l.Info("failure alert sent",
		slog.String("pipeline", cfg.Name),
	)
}
