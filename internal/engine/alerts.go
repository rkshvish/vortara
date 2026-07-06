package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	vlogger "github.com/rkshvish/vortara/internal/logger"
)

var alertClient = &http.Client{Timeout: 10 * time.Second}

type failureAlert struct {
	Sync     string    `json:"sync"`
	Status   string    `json:"status"`
	Error    string    `json:"error"`
	FailedAt time.Time `json:"failed_at"`
}

// sendFailureAlert POSTs a failure notification to a webhook URL.
// Best-effort: failures are logged, never propagated.
func sendFailureAlert(ctx context.Context, syncName, webhookURL string, runErr error) {
	if webhookURL == "" || runErr == nil {
		return
	}
	l := vlogger.FromContext(ctx)
	payload, err := json.Marshal(failureAlert{
		Sync: syncName, Status: "failed",
		Error: runErr.Error(), FailedAt: time.Now().UTC(),
	})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		l.Warn("failure alert: bad request", slog.String("error", err.Error()))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := alertClient.Do(req)
	if err != nil {
		l.Warn("failure alert: unreachable", slog.String("error", err.Error()))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		l.Warn("failure alert: rejected", slog.Int("status", resp.StatusCode))
	}
}
