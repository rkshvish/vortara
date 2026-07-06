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

const (
	intercomBaseURL     = "https://api.intercom.io"
	intercomAPIVersion  = "2.10"
	intercomParallelism = 8
)

// intercomTopFields are Intercom contact top-level fields (not custom attributes).
var intercomTopFields = map[string]bool{
	"email":                    true,
	"external_id":              true,
	"role":                     true,
	"name":                     true,
	"phone":                    true,
	"avatar":                   true,
	"signed_up_at":             true,
	"last_seen_at":             true,
	"unsubscribed_from_emails": true,
}

// IntercomDestination upserts rows into Intercom contacts or companies.
// Auth: bearer (access token).
//
//	type: intercom
//	auth: { type: bearer, token: ${INTERCOM_TOKEN} }
//	options:
//	  object: contacts   # contacts (default) | companies
//	match_on: [external_id]  # or email
type IntercomDestination struct {
	cfg         config.DestinationConfig
	client      *http.Client
	auth        httpauth.Authenticator
	rateLimiter *httpauth.RateLimiter
	breaker     *httpauth.CircuitBreaker
	baseURL     string
	object      string // contacts | companies
	matchField  string
}

var _ Destination = (*IntercomDestination)(nil)

func init() {
	registry.RegisterDestination("intercom", func() any {
		return &IntercomDestination{}
	})
}

func (ic *IntercomDestination) Connect(_ context.Context, cfg config.DestinationConfig) error {
	object := strings.ToLower(strings.TrimSpace(cfg.Options["object"]))
	if object == "" {
		object = "contacts"
	}
	if object != "contacts" && object != "companies" {
		return fmt.Errorf("intercom destination: object %q unknown, valid: contacts, companies", object)
	}
	if strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("intercom destination: match_on is required (external_id or email)")
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
		parallelism = intercomParallelism
	}
	ic.cfg = cfg
	ic.auth = auth
	ic.rateLimiter = rl
	ic.breaker = httpauth.NewCircuitBreaker(cfg.CircuitBreaker)
	ic.baseURL = intercomBaseURL
	ic.object = object
	ic.matchField = strings.TrimSpace(strings.Split(cfg.MatchOn, ",")[0])
	ic.client = &http.Client{
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

func (ic *IntercomDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
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
		if _, ok := rw.Data[ic.matchField]; !ok {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw,
				Err: fmt.Errorf("intercom destination: missing match field %q", ic.matchField)})
			continue
		}
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	parallelism := ic.cfg.WriteParallelism
	if parallelism <= 0 {
		parallelism = intercomParallelism
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
			if err := ic.upsertOne(ctx, rw); err != nil {
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

func (ic *IntercomDestination) Close() error {
	if ic.rateLimiter != nil {
		ic.rateLimiter.Stop()
	}
	return nil
}

func (ic *IntercomDestination) upsertOne(ctx context.Context, rw row.Row) error {
	matchVal := fmt.Sprintf("%v", rw.Data[ic.matchField])

	// Search for existing record by match field.
	existingID, err := ic.searchContact(ctx, matchVal)
	if err != nil {
		return err
	}

	// Split top-level fields vs custom_attributes.
	body := make(map[string]any, len(rw.Data))
	customAttrs := make(map[string]any)
	for k, v := range rw.Data {
		if ic.object == "contacts" && !intercomTopFields[k] {
			customAttrs[k] = v
		} else {
			body[k] = v
		}
	}
	if len(customAttrs) > 0 {
		body["custom_attributes"] = customAttrs
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	if existingID != "" {
		// Update existing.
		endpoint := fmt.Sprintf("%s/%s/%s", ic.baseURL, ic.object, existingID)
		return ic.doRequest(ctx, http.MethodPut, endpoint, payload)
	}
	// Create new.
	endpoint := ic.baseURL + "/" + ic.object
	return ic.doRequest(ctx, http.MethodPost, endpoint, payload)
}

func (ic *IntercomDestination) searchContact(ctx context.Context, matchVal string) (string, error) {
	searchURL := fmt.Sprintf("%s/%s/search", ic.baseURL, ic.object)

	queryField := ic.matchField
	if ic.object == "companies" {
		queryField = "company_id"
	}

	searchBody, err := json.Marshal(map[string]any{
		"query": map[string]any{
			"field":    queryField,
			"operator": "=",
			"value":    matchVal,
		},
		"pagination": map[string]any{"per_page": 1},
	})
	if err != nil {
		return "", err
	}

	var existingID string
	err = httpauth.DoWithRetry(ctx, ic.cfg.Retry, func() (int, error) {
		if ic.rateLimiter != nil {
			if err := ic.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if ic.breaker != nil {
			if err := ic.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, searchURL, bytes.NewReader(searchBody))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Intercom-Version", intercomAPIVersion)
		if ic.auth != nil {
			if err := ic.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := ic.client.Do(req)
		if err != nil {
			if ic.breaker != nil {
				ic.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if ic.breaker != nil {
			if resp.StatusCode >= 500 {
				ic.breaker.RecordFailure()
			} else {
				ic.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode != http.StatusOK {
			return resp.StatusCode, fmt.Errorf("intercom: search status %d: %s", resp.StatusCode, bodySnip(body))
		}
		var result struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return resp.StatusCode, nil
		}
		if len(result.Data) > 0 {
			existingID = result.Data[0].ID
		}
		return resp.StatusCode, nil
	})
	return existingID, err
}

func (ic *IntercomDestination) doRequest(ctx context.Context, method, endpoint string, payload []byte) error {
	return httpauth.DoWithRetry(ctx, ic.cfg.Retry, func() (int, error) {
		if ic.rateLimiter != nil {
			if err := ic.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if ic.breaker != nil {
			if err := ic.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Intercom-Version", intercomAPIVersion)
		if ic.auth != nil {
			if err := ic.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := ic.client.Do(req)
		if err != nil {
			if ic.breaker != nil {
				ic.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if ic.breaker != nil {
			if resp.StatusCode >= 500 {
				ic.breaker.RecordFailure()
			} else {
				ic.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, fmt.Errorf("intercom: status %d: %s", resp.StatusCode, bodySnip(body))
	})
}
