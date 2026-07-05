package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	httpauth "github.com/rkshvish/vortaraos/internal/connector/http"
	vlogger "github.com/rkshvish/vortaraos/internal/logger"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

const hubspotDefaultBaseURL = "https://api.hubapi.com"

// HubSpotDestination writes rows to HubSpot CRM using the batch upsert API.
type HubSpotDestination struct {
	cfg              config.DestinationConfig
	auth             httpauth.Authenticator
	rateLimiter      *httpauth.RateLimiter
	breaker          *httpauth.CircuitBreaker
	client           *http.Client
	baseURL          string
	object           string
	matchOn          string
	writeParallelism int
}

var _ Destination = (*HubSpotDestination)(nil)

func init() {
	registry.RegisterDestination("hubspot", func() any {
		return NewHubSpotDestination()
	})
}

// NewHubSpotDestination returns a new HubSpotDestination.
func NewHubSpotDestination() *HubSpotDestination {
	return &HubSpotDestination{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Connect validates HubSpot settings and initializes shared HTTP helpers.
func (h *HubSpotDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	if strings.TrimSpace(cfg.Options["object"]) == "" {
		return errors.New("hubspot destination: options.object is required")
	}
	if strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("hubspot destination: match_on is required")
	}

	auth, err := httpauth.NewAuthenticator(cfg.Auth)
	if err != nil {
		return err
	}
	rl, err := httpauth.NewRateLimiter(cfg.RateLimit)
	if err != nil {
		return err
	}
	cb := httpauth.NewCircuitBreaker(cfg.CircuitBreaker)
	if cfg.WriteParallelism <= 0 {
		cfg.WriteParallelism = 3
	}
	transport := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        cfg.WriteParallelism * 2,
		MaxIdleConnsPerHost: cfg.WriteParallelism,
		MaxConnsPerHost:     cfg.WriteParallelism,
		IdleConnTimeout:     90 * time.Second,
	}

	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		baseURL = hubspotDefaultBaseURL
	}

	h.cfg = cfg
	h.auth = auth
	h.rateLimiter = rl
	h.breaker = cb
	h.writeParallelism = cfg.WriteParallelism
	h.client = &http.Client{Timeout: 30 * time.Second, Transport: transport}
	h.baseURL = baseURL
	h.object = cfg.Options["object"]
	h.matchOn = cfg.MatchOn
	return nil
}

// Load writes rows to HubSpot in batches of up to 100 using batch upsert.
func (h *HubSpotDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, nil
	}
	if h.client == nil {
		h.client = &http.Client{Timeout: 30 * time.Second}
	}

	batches := make([][]row.Row, 0, (len(rows)+99)/100)
	for start := 0; start < len(rows); start += 100 {
		end := start + 100
		if end > len(rows) {
			end = len(rows)
		}
		batches = append(batches, rows[start:end])
	}

	if len(batches) == 0 {
		return result, nil
	}

	if len(batches) > 2 && h.writeParallelism > 1 {
		return h.loadBatchesParallel(ctx, batches, store, pipeline, destName)
	}

	for _, batch := range batches {
		pending := make([]row.Row, 0, len(batch))
		for _, rw := range batch {
			delivered, err := store.IsDelivered(rw.ID, pipeline, destName)
			if err != nil {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				continue
			}
			if delivered {
				result.Skipped++
				continue
			}
			if _, ok := rw.Data[h.matchOn]; !ok {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: fmt.Errorf("hubspot destination: missing %s", h.matchOn)})
				continue
			}
			pending = append(pending, rw)
		}
		batchResult, err := h.processPendingBatch(ctx, pending, store, pipeline, destName)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return result, ctx.Err()
			}
			for _, rw := range pending {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			}
			continue
		}
		result.Loaded += batchResult.Loaded
		result.Skipped += batchResult.Skipped
		result.Errors = append(result.Errors, batchResult.Errors...)
	}

	return result, nil
}

// Close releases HubSpot connector resources.
func (h *HubSpotDestination) Close() error {
	if h.rateLimiter != nil {
		h.rateLimiter.Stop()
	}
	return nil
}

func (h *HubSpotDestination) loadBatchesParallel(ctx context.Context, batches [][]row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	parallelism := h.writeParallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, batch := range batches {
		batch := batch
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			pending := make([]row.Row, 0, len(batch))
			var skipped int
			var batchErrors []RowError
			for _, rw := range batch {
				delivered, err := store.IsDelivered(rw.ID, pipeline, destName)
				if err != nil {
					batchErrors = append(batchErrors, RowError{RowID: rw.ID, Row: rw, Err: err})
					continue
				}
				if delivered {
					skipped++
					continue
				}
				if _, ok := rw.Data[h.matchOn]; !ok {
					batchErrors = append(batchErrors, RowError{RowID: rw.ID, Row: rw, Err: fmt.Errorf("hubspot destination: missing %s", h.matchOn)})
					continue
				}
				pending = append(pending, rw)
			}
			if len(pending) == 0 {
				mu.Lock()
				result.Skipped += skipped
				result.Errors = append(result.Errors, batchErrors...)
				mu.Unlock()
				return
			}

			successIDs, failed, err := h.upsertBatch(ctx, destName, pending)
			mu.Lock()
			defer mu.Unlock()
			result.Skipped += skipped
			result.Errors = append(result.Errors, batchErrors...)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					result.Errors = append(result.Errors, RowError{Err: err})
					return
				}
				for _, rw := range pending {
					result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				}
				return
			}

			for _, rw := range pending {
				matchID := fmt.Sprintf("%v", rw.Data[h.matchOn])
				if rowErr, ok := failed[matchID]; ok {
					result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: rowErr})
					continue
				}
				if successIDs != nil {
					if _, ok := successIDs[matchID]; !ok {
						result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: errors.New("hubspot destination: row missing from successful results")})
						continue
					}
				}
				if err := store.MarkDelivered(rw.ID, pipeline, destName); err != nil {
					result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
					continue
				}
				result.Loaded++
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func (h *HubSpotDestination) processPendingBatch(ctx context.Context, pending []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	if len(pending) == 0 {
		return result, nil
	}

	successIDs, failed, err := h.upsertBatch(ctx, destName, pending)
	if err != nil {
		return result, err
	}

	for _, rw := range pending {
		matchID := fmt.Sprintf("%v", rw.Data[h.matchOn])
		if rowErr, ok := failed[matchID]; ok {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: rowErr})
			continue
		}
		if successIDs != nil {
			if _, ok := successIDs[matchID]; !ok {
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: errors.New("hubspot destination: row missing from successful results")})
				continue
			}
		}
		if err := store.MarkDelivered(rw.ID, pipeline, destName); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		result.Loaded++
	}
	return result, nil
}

func (h *HubSpotDestination) upsertBatch(ctx context.Context, destName string, rows []row.Row) (map[string]struct{}, map[string]error, error) {
	payload, err := h.buildBatchPayload(rows)
	if err != nil {
		return nil, nil, err
	}

	endpoint := fmt.Sprintf("%s/crm/v3/objects/%s/batch/upsert", h.baseURL, h.object)
	var successIDs map[string]struct{}
	var failed map[string]error

	err = httpauth.DoWithRetry(ctx, h.cfg.Retry, func() (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := h.allowRequest(ctx, destName); err != nil {
			return 0, err
		}
		if h.rateLimiter != nil {
			if err := h.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if h.auth != nil {
			if err := h.auth.Apply(req); err != nil {
				return 0, err
			}
		}

		resp, err := h.client.Do(req)
		if err != nil {
			h.recordCircuitFailure(ctx, destName)
			return 0, err
		}
		defer resp.Body.Close()
		h.recordCircuitResponse(ctx, destName, resp.StatusCode)

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusMultiStatus {
			successIDs, failed, err = parseHubSpotResponse(resp.Body)
			if err != nil {
				return resp.StatusCode, err
			}
		}
		return resp.StatusCode, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return successIDs, failed, nil
}

func (h *HubSpotDestination) allowRequest(ctx context.Context, destination string) error {
	if h.breaker == nil {
		return nil
	}
	before := h.breaker.State()
	if err := h.breaker.Allow(); err != nil {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Warn("circuit breaker open, skipping request",
			slog.String("destination", destination),
		)
		return err
	}
	if before == "open" && h.breaker.State() == "half_open" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Info("circuit breaker half-open, testing",
			slog.String("destination", destination),
		)
	}
	return nil
}

func (h *HubSpotDestination) recordCircuitFailure(ctx context.Context, destination string) {
	if h.breaker == nil {
		return
	}
	before := h.breaker.State()
	h.breaker.RecordFailure()
	if before != "open" && h.breaker.State() == "open" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Error("circuit breaker opened",
			slog.String("destination", destination),
			slog.Int("failures", h.cfg.CircuitBreaker.Threshold),
		)
	}
}

func (h *HubSpotDestination) recordCircuitResponse(ctx context.Context, destination string, statusCode int) {
	if h.breaker == nil {
		return
	}
	before := h.breaker.State()
	if statusCode >= http.StatusInternalServerError {
		h.recordCircuitFailure(ctx, destination)
		return
	}
	h.breaker.RecordSuccess()
	if before == "half_open" && h.breaker.State() == "closed" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Info("circuit breaker closed, destination recovered",
			slog.String("destination", destination),
		)
	}
}

func (h *HubSpotDestination) buildBatchPayload(rows []row.Row) ([]byte, error) {
	type hubspotInput struct {
		IDProperty string            `json:"idProperty"`
		ID         string            `json:"id"`
		Properties map[string]string `json:"properties"`
	}
	inputs := make([]hubspotInput, 0, len(rows))
	for _, rw := range rows {
		props := make(map[string]string, len(rw.Data))
		for k, v := range rw.Data {
			if k == h.matchOn {
				continue
			}
			props[k] = fmt.Sprintf("%v", v)
		}
		inputs = append(inputs, hubspotInput{
			IDProperty: h.matchOn,
			ID:         fmt.Sprintf("%v", rw.Data[h.matchOn]),
			Properties: props,
		})
	}
	return json.Marshal(map[string]any{"inputs": inputs})
}

func parseHubSpotResponse(body io.Reader) (map[string]struct{}, map[string]error, error) {
	var payload struct {
		Results []struct {
			ID string `json:"id"`
		} `json:"results"`
		Errors []struct {
			ID      string `json:"id"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		return nil, nil, err
	}

	var successIDs map[string]struct{}
	if len(payload.Results) > 0 {
		successIDs = make(map[string]struct{}, len(payload.Results))
		for _, res := range payload.Results {
			if strings.TrimSpace(res.ID) == "" {
				continue
			}
			successIDs[res.ID] = struct{}{}
		}
	}

	failed := make(map[string]error, len(payload.Errors))
	for _, rowErr := range payload.Errors {
		if strings.TrimSpace(rowErr.ID) == "" {
			continue
		}
		msg := strings.TrimSpace(rowErr.Message)
		if msg == "" {
			msg = "hubspot destination: upsert failed"
		}
		failed[rowErr.ID] = errors.New(msg)
	}
	return successIDs, failed, nil
}
