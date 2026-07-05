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

	httpauth "github.com/rkshvish/vortaraos/internal/connector/http"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

const (
	klaviyoBaseURL  = "https://a.klaviyo.com"
	klaviyoRevision = "2024-02-15"
	klaviyoBatch    = 1000
)

// klaviyoIdentifiers are top-level Klaviyo profile fields (not nested under properties).
var klaviyoIdentifiers = map[string]bool{
	"email":        true,
	"phone_number": true,
	"external_id":  true,
	"first_name":   true,
	"last_name":    true,
}

// KlaviyoDestination syncs rows to Klaviyo profiles via the bulk-upsert API.
// Auth: bearer or api_key — value is used as the Klaviyo-API-Key header.
//
//	type: klaviyo
//	auth: { type: bearer, token: ${KLAVIYO_API_KEY} }
//	match_on: [email]   # email | phone_number | external_id
type KlaviyoDestination struct {
	cfg         config.DestinationConfig
	client      *http.Client
	rateLimiter *httpauth.RateLimiter
	breaker     *httpauth.CircuitBreaker
	apiKey      string
	matchField  string
}

var _ Destination = (*KlaviyoDestination)(nil)

func init() {
	registry.RegisterDestination("klaviyo", func() any {
		return &KlaviyoDestination{}
	})
}

func (k *KlaviyoDestination) Connect(_ context.Context, cfg config.DestinationConfig) error {
	if strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("klaviyo destination: match_on is required (email, phone_number, or external_id)")
	}
	apiKey := ""
	switch cfg.Auth.Type {
	case "bearer":
		apiKey = strings.TrimSpace(cfg.Auth.Token)
	case "api_key":
		apiKey = strings.TrimSpace(cfg.Auth.Value)
	}
	if apiKey == "" {
		return errors.New("klaviyo destination: auth token is required (bearer or api_key)")
	}
	rl, err := httpauth.NewRateLimiter(cfg.RateLimit)
	if err != nil {
		return err
	}
	k.cfg = cfg
	k.apiKey = apiKey
	k.matchField = strings.TrimSpace(strings.Split(cfg.MatchOn, ",")[0])
	k.rateLimiter = rl
	k.breaker = httpauth.NewCircuitBreaker(cfg.CircuitBreaker)
	k.client = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return nil
}

func (k *KlaviyoDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
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
		pending = append(pending, rw)
	}

	for start := 0; start < len(pending); start += klaviyoBatch {
		end := start + klaviyoBatch
		if end > len(pending) {
			end = len(pending)
		}
		chunk := pending[start:end]
		if err := k.upsertChunk(ctx, chunk); err != nil {
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

func (k *KlaviyoDestination) Close() error {
	if k.rateLimiter != nil {
		k.rateLimiter.Stop()
	}
	return nil
}

func (k *KlaviyoDestination) upsertChunk(ctx context.Context, rows []row.Row) error {
	payload, err := k.buildPayload(rows)
	if err != nil {
		return err
	}
	url := klaviyoBaseURL + "/api/profile-bulk-upsert-jobs/"
	return httpauth.DoWithRetry(ctx, k.cfg.Retry, func() (int, error) {
		if k.rateLimiter != nil {
			if err := k.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if k.breaker != nil {
			if err := k.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Authorization", "Klaviyo-API-Key "+k.apiKey)
		req.Header.Set("revision", klaviyoRevision)
		req.Header.Set("Content-Type", "application/vnd.api+json")
		resp, err := k.client.Do(req)
		if err != nil {
			if k.breaker != nil {
				k.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if k.breaker != nil {
			if resp.StatusCode >= 500 {
				k.breaker.RecordFailure()
			} else {
				k.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, fmt.Errorf("klaviyo: status %d: %s", resp.StatusCode, bodySnip(body))
	})
}

func (k *KlaviyoDestination) buildPayload(rows []row.Row) ([]byte, error) {
	type profileData struct {
		Type       string         `json:"type"`
		Attributes map[string]any `json:"attributes"`
	}
	profiles := make([]profileData, 0, len(rows))
	for _, rw := range rows {
		if _, ok := rw.Data[k.matchField]; !ok {
			continue
		}
		attrs := make(map[string]any, len(rw.Data))
		props := make(map[string]any)
		for col, val := range rw.Data {
			if klaviyoIdentifiers[col] {
				attrs[col] = val
			} else {
				props[col] = val
			}
		}
		if len(props) > 0 {
			attrs["properties"] = props
		}
		profiles = append(profiles, profileData{Type: "profile", Attributes: attrs})
	}
	return json.Marshal(map[string]any{
		"data": map[string]any{
			"type": "profile-bulk-upsert-job",
			"attributes": map[string]any{
				"profiles": map[string]any{"data": profiles},
			},
		},
	})
}

// bodySnip returns up to 200 bytes of body as a string.
func bodySnip(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}
