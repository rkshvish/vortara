package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	httpauth "github.com/rkshvish/vortara/internal/connector/http"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

const (
	mixpanelEngageURL = "https://api.mixpanel.com/engage"
	mixpanelBatch     = 2000
)

// MixpanelDestination syncs rows to Mixpanel user profiles via the Engage API.
// Auth: basic (service account username + secret) OR bearer (project token only — no SA).
// Required options: project_token, distinct_id (field name to use as $distinct_id).
// Optional options: operation ($set | $set_once | $union | $add — default $set).
//
//	type: mixpanel
//	auth: { type: basic, username: ${SA_USER}, password: ${SA_SECRET} }
//	options:
//	  project_token: ${MIXPANEL_TOKEN}
//	  distinct_id: user_id
//	  operation: $set
type MixpanelDestination struct {
	cfg          config.DestinationConfig
	client       *http.Client
	rateLimiter  *httpauth.RateLimiter
	breaker      *httpauth.CircuitBreaker
	projectToken string
	distinctID   string
	operation    string
	auth         httpauth.Authenticator
}

var _ Destination = (*MixpanelDestination)(nil)

func init() {
	registry.RegisterDestination("mixpanel", func() any {
		return &MixpanelDestination{}
	})
}

func (m *MixpanelDestination) Connect(_ context.Context, cfg config.DestinationConfig) error {
	projectToken := strings.TrimSpace(cfg.Options["project_token"])
	if projectToken == "" {
		return errors.New("mixpanel destination: options.project_token is required")
	}
	distinctID := strings.TrimSpace(cfg.Options["distinct_id"])
	if distinctID == "" {
		distinctID = "distinct_id"
	}
	operation := strings.TrimSpace(cfg.Options["operation"])
	if operation == "" {
		operation = "$set"
	}
	switch operation {
	case "$set", "$set_once", "$union", "$add", "$append", "$remove", "$unset":
	default:
		return fmt.Errorf("mixpanel destination: unknown operation %q (valid: $set, $set_once, $union, $add)", operation)
	}

	auth, err := httpauth.NewAuthenticator(cfg.Auth)
	if err != nil {
		return err
	}
	rl, err := httpauth.NewRateLimiter(cfg.RateLimit)
	if err != nil {
		return err
	}
	m.cfg = cfg
	m.projectToken = projectToken
	m.distinctID = distinctID
	m.operation = operation
	m.auth = auth
	m.rateLimiter = rl
	m.breaker = httpauth.NewCircuitBreaker(cfg.CircuitBreaker)
	m.client = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return nil
}

func (m *MixpanelDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, nil
	}

	pending := make([]row.Row, 0, len(rows))
	for _, rw := range rows {
		delivered, err := store.IsDelivered(rw.ID, pipeline, destName)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}
		if _, ok := rw.Data[m.distinctID]; !ok {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw,
				Err: fmt.Errorf("mixpanel destination: missing distinct_id field %q", m.distinctID)})
			continue
		}
		pending = append(pending, rw)
	}

	for start := 0; start < len(pending); start += mixpanelBatch {
		end := start + mixpanelBatch
		if end > len(pending) {
			end = len(pending)
		}
		chunk := pending[start:end]
		if err := m.sendBatch(ctx, chunk); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return result, ctx.Err()
			}
			for _, rw := range chunk {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			}
			continue
		}
		for _, rw := range chunk {
			if err := store.MarkDelivered(rw.ID, pipeline, destName); err != nil {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				continue
			}
			result.Loaded++
		}
	}
	return result, nil
}

func (m *MixpanelDestination) Close() error {
	if m.rateLimiter != nil {
		m.rateLimiter.Stop()
	}
	return nil
}

func (m *MixpanelDestination) sendBatch(ctx context.Context, rows []row.Row) error {
	records := make([]map[string]any, 0, len(rows))
	for _, rw := range rows {
		props := make(map[string]any, len(rw.Data))
		for k, v := range rw.Data {
			if k == m.distinctID {
				continue
			}
			props[k] = v
		}
		records = append(records, map[string]any{
			"$token":       m.projectToken,
			"$distinct_id": fmt.Sprintf("%v", rw.Data[m.distinctID]),
			m.operation:   props,
		})
	}
	payload, err := json.Marshal(records)
	if err != nil {
		return err
	}

	return httpauth.DoWithRetry(ctx, m.cfg.Retry, func() (int, error) {
		if m.rateLimiter != nil {
			if err := m.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if m.breaker != nil {
			if err := m.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, mixpanelEngageURL, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/plain")
		if m.auth != nil {
			if err := m.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := m.client.Do(req)
		if err != nil {
			if m.breaker != nil {
				m.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if m.breaker != nil {
			if resp.StatusCode >= 500 {
				m.breaker.RecordFailure()
			} else {
				m.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode != http.StatusOK {
			return resp.StatusCode, fmt.Errorf("mixpanel: status %d: %s", resp.StatusCode, bodySnip(body))
		}
		// Mixpanel returns {"status": 1, "error": null} on success.
		var result struct {
			Status int    `json:"status"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(body, &result); err == nil && result.Status != 1 {
			msg := result.Error
			if msg == "" {
				msg = "unknown error"
			}
			return resp.StatusCode, fmt.Errorf("mixpanel: %s", msg)
		}
		return resp.StatusCode, nil
	})
}
