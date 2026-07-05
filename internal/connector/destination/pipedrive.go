package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	httpauth "github.com/rkshvish/vortaraos/internal/connector/http"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

const pipedriveDefaultBase = "https://api.pipedrive.com"

// pipedriveObjects maps object names to their v2 API paths.
var pipedriveObjects = map[string]string{
	"persons":       "/api/v2/persons",
	"deals":         "/api/v2/deals",
	"organizations": "/api/v2/organizations",
	"leads":         "/api/v2/leads",
}

// PipedriveDestination upserts rows into Pipedrive CRM via search → create/update.
// Auth: bearer (personal API token or OAuth2 access token).
//
//	type: pipedrive
//	url: https://yourcompany.pipedrive.com   # optional, defaults to api.pipedrive.com
//	auth: { type: bearer, token: ${PIPEDRIVE_TOKEN} }
//	options:
//	  object: persons   # persons | deals | organizations | leads
//	  search_field: email  # field to search by (e.g. email, name)
//	match_on: [email]
type PipedriveDestination struct {
	cfg         config.DestinationConfig
	client      *http.Client
	auth        httpauth.Authenticator
	rateLimiter *httpauth.RateLimiter
	breaker     *httpauth.CircuitBreaker
	baseURL     string
	objectPath  string
	matchField  string
	searchField string
}

var _ Destination = (*PipedriveDestination)(nil)

func init() {
	registry.RegisterDestination("pipedrive", func() any {
		return &PipedriveDestination{}
	})
}

func (p *PipedriveDestination) Connect(_ context.Context, cfg config.DestinationConfig) error {
	object := strings.ToLower(strings.TrimSpace(cfg.Options["object"]))
	if object == "" {
		object = "persons"
	}
	objectPath, ok := pipedriveObjects[object]
	if !ok {
		return fmt.Errorf("pipedrive destination: object %q unknown, valid: persons, deals, organizations, leads", object)
	}
	if strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("pipedrive destination: match_on is required")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		baseURL = pipedriveDefaultBase
	}
	searchField := strings.TrimSpace(cfg.Options["search_field"])
	if searchField == "" {
		searchField = strings.TrimSpace(strings.Split(cfg.MatchOn, ",")[0])
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
		parallelism = 5
	}
	p.cfg = cfg
	p.auth = auth
	p.rateLimiter = rl
	p.breaker = httpauth.NewCircuitBreaker(cfg.CircuitBreaker)
	p.baseURL = baseURL
	p.objectPath = objectPath
	p.matchField = strings.TrimSpace(strings.Split(cfg.MatchOn, ",")[0])
	p.searchField = searchField
	p.client = &http.Client{
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

func (p *PipedriveDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
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
		if _, ok := rw.Data[p.matchField]; !ok {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw,
				Err: fmt.Errorf("pipedrive destination: missing match field %q", p.matchField)})
			continue
		}
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	parallelism := p.cfg.WriteParallelism
	if parallelism <= 0 {
		parallelism = 5
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
			if err := p.upsertOne(ctx, rw); err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				mu.Unlock()
				return
			}
			if err := store.MarkDelivered(rw.ID, pipeline, destName); err != nil {
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

func (p *PipedriveDestination) Close() error {
	if p.rateLimiter != nil {
		p.rateLimiter.Stop()
	}
	return nil
}

func (p *PipedriveDestination) upsertOne(ctx context.Context, rw row.Row) error {
	matchVal := fmt.Sprintf("%v", rw.Data[p.matchField])

	// Search for existing record.
	existingID, err := p.searchOne(ctx, matchVal)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(rw.Data)
	if err != nil {
		return err
	}

	var method, endpoint string
	if existingID != "" {
		method = http.MethodPatch
		endpoint = p.baseURL + p.objectPath + "/" + existingID
	} else {
		method = http.MethodPost
		endpoint = p.baseURL + p.objectPath
	}

	return p.doRequest(ctx, method, endpoint, payload)
}

func (p *PipedriveDestination) searchOne(ctx context.Context, term string) (string, error) {
	searchURL := p.baseURL + p.objectPath + "/search"
	params := url.Values{
		"term":   {term},
		"fields": {p.searchField},
		"limit":  {"1"},
	}
	reqURL := searchURL + "?" + params.Encode()

	var existingID string
	err := httpauth.DoWithRetry(ctx, p.cfg.Retry, func() (int, error) {
		if p.rateLimiter != nil {
			if err := p.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if p.breaker != nil {
			if err := p.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return 0, err
		}
		if p.auth != nil {
			if err := p.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := p.client.Do(req)
		if err != nil {
			if p.breaker != nil {
				p.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if p.breaker != nil {
			if resp.StatusCode >= 500 {
				p.breaker.RecordFailure()
			} else {
				p.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode != http.StatusOK {
			return resp.StatusCode, fmt.Errorf("pipedrive: search status %d: %s", resp.StatusCode, bodySnip(body))
		}
		var result struct {
			Data struct {
				Items []struct {
					Item struct {
						ID json.Number `json:"id"`
					} `json:"item"`
				} `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return resp.StatusCode, nil // non-fatal: treat as not found
		}
		if len(result.Data.Items) > 0 {
			existingID = result.Data.Items[0].Item.ID.String()
		}
		return resp.StatusCode, nil
	})
	return existingID, err
}

func (p *PipedriveDestination) doRequest(ctx context.Context, method, endpoint string, payload []byte) error {
	return httpauth.DoWithRetry(ctx, p.cfg.Retry, func() (int, error) {
		if p.rateLimiter != nil {
			if err := p.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if p.breaker != nil {
			if err := p.breaker.Allow(); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if p.auth != nil {
			if err := p.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := p.client.Do(req)
		if err != nil {
			if p.breaker != nil {
				p.breaker.RecordFailure()
			}
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if p.breaker != nil {
			if resp.StatusCode >= 500 {
				p.breaker.RecordFailure()
			} else {
				p.breaker.RecordSuccess()
			}
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, fmt.Errorf("pipedrive: status %d: %s", resp.StatusCode, bodySnip(body))
	})
}
