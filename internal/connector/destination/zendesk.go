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
	"sync"
	"time"

	httpauth "github.com/rkshvish/vortara/internal/connector/http"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

const zendeskParallelism = 10

// ZendeskDestination upserts rows into Zendesk users or tickets.
// Auth: basic with email/token format (username="${EMAIL}/token", password=${API_TOKEN}).
//
//	type: zendesk
//	url: https://yourcompany.zendesk.com
//	auth: { type: basic, username: ${ZENDESK_EMAIL}/token, password: ${ZENDESK_TOKEN} }
//	options:
//	  object: users    # users (default) | tickets
//	match_on: [external_id]
type ZendeskDestination struct {
	cfg         config.DestinationConfig
	client      *http.Client
	auth        httpauth.Authenticator
	rateLimiter *httpauth.RateLimiter
	breaker     *httpauth.CircuitBreaker
	baseURL     string
	object      string
	matchField  string
}

var _ Destination = (*ZendeskDestination)(nil)

func init() {
	registry.RegisterDestination("zendesk", func() any {
		return &ZendeskDestination{}
	})
}

func (z *ZendeskDestination) Connect(_ context.Context, cfg config.DestinationConfig) error {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		return errors.New("zendesk destination: url is required (https://yourcompany.zendesk.com)")
	}
	object := strings.ToLower(strings.TrimSpace(cfg.Options["object"]))
	if object == "" {
		object = "users"
	}
	if object != "users" && object != "tickets" {
		return fmt.Errorf("zendesk destination: object %q unknown, valid: users, tickets", object)
	}
	if strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("zendesk destination: match_on is required")
	}
	auth, err := httpauth.NewAuthenticator(cfg.Auth)
	if err != nil {
		return err
	}
	rl, err := httpauth.NewRateLimiter(cfg.RateLimit)
	if err != nil {
		return err
	}
	parallelism := cfg.WriteParallelism
	if parallelism <= 0 {
		parallelism = zendeskParallelism
	}
	z.cfg = cfg
	z.auth = auth
	z.rateLimiter = rl
	z.breaker = httpauth.NewCircuitBreaker(cfg.CircuitBreaker)
	z.baseURL = baseURL
	z.object = object
	z.matchField = strings.TrimSpace(strings.Split(cfg.MatchOn, ",")[0])
	z.client = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: parallelism,
			MaxConnsPerHost:     parallelism,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return nil
}

func (z *ZendeskDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, nil
	}

	pending := make([]row.Row, 0, len(rows))
	for _, rw := range rows {
		delivered, err := store.IsDelivered(ctx, rw.ID, pipeline, destName)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}
		if _, ok := rw.Data[z.matchField]; !ok {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw,
				Err: fmt.Errorf("zendesk destination: missing match field %q", z.matchField)})
			continue
		}
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	parallelism := z.cfg.WriteParallelism
	if parallelism <= 0 {
		parallelism = zendeskParallelism
	}
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, rw := range pending {
		rw := rw
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := z.upsertOne(ctx, rw); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					mu.Lock()
					result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
					mu.Unlock()
					return
				}
				mu.Lock()
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				mu.Unlock()
				return
			}
			if err := store.MarkDelivered(ctx, rw.ID, pipeline, destName); err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				mu.Unlock()
				return
			}
			mu.Lock()
			result.Loaded++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func (z *ZendeskDestination) Close() error {
	if z.rateLimiter != nil {
		z.rateLimiter.Stop()
	}
	return nil
}

func (z *ZendeskDestination) upsertOne(ctx context.Context, rw row.Row) error {
	obj := make(map[string]any, len(rw.Data))
	for k, v := range rw.Data {
		obj[k] = v
	}
	// Zendesk uses external_id as the upsert key; rename match_on field if different.
	if z.matchField != "external_id" {
		if v, ok := obj[z.matchField]; ok {
			obj["external_id"] = fmt.Sprintf("%v", v)
		}
	}

	var endpoint string
	switch z.object {
	case "tickets":
		payload, err := json.Marshal(map[string]any{"ticket": obj})
		if err != nil {
			return err
		}
		endpoint = z.baseURL + "/api/v2/tickets/create_or_update"
		return z.doRequest(ctx, endpoint, payload)
	default: // users
		payload, err := json.Marshal(map[string]any{"user": obj})
		if err != nil {
			return err
		}
		endpoint = z.baseURL + "/api/v2/users/create_or_update"
		return z.doRequest(ctx, endpoint, payload)
	}
}

func (z *ZendeskDestination) doRequest(ctx context.Context, url string, payload []byte) error {
	return httpauth.DoWithRetry(ctx, z.cfg.Retry, func() (int, error) {
		if z.rateLimiter != nil {
			if err := z.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if z.breaker != nil {
			if err := z.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if z.auth != nil {
			if err := z.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := z.client.Do(req)
		if err != nil {
			if z.breaker != nil {
				z.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if z.breaker != nil {
			if resp.StatusCode >= 500 {
				z.breaker.RecordFailure()
			} else {
				z.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, fmt.Errorf("zendesk: status %d: %s", resp.StatusCode, bodySnip(body))
	})
}
