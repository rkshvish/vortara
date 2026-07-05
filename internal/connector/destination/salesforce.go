package destination

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	httpauth "github.com/rkshvish/vortara/internal/connector/http"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

const salesforceAPIVersion = "v58.0"

// SalesforceDestination writes rows to Salesforce using REST upsert or Bulk API pipeline.
type SalesforceDestination struct {
	cfg              config.DestinationConfig
	auth             httpauth.Authenticator
	rateLimiter      *httpauth.RateLimiter
	breaker          *httpauth.CircuitBreaker
	client           *http.Client
	instanceURL      string
	object           string
	matchOn          string
	writeParallelism int
}

var _ Destination = (*SalesforceDestination)(nil)

func init() {
	registry.RegisterDestination("salesforce", func() any {
		return NewSalesforceDestination()
	})
}

// NewSalesforceDestination returns a new SalesforceDestination.
func NewSalesforceDestination() *SalesforceDestination {
	return &SalesforceDestination{client: &http.Client{Timeout: 30 * time.Second}}
}

// Connect validates Salesforce settings and initializes shared HTTP helpers.
func (s *SalesforceDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	if strings.TrimSpace(cfg.URL) == "" {
		return errors.New("salesforce destination: url is required")
	}
	if strings.TrimSpace(cfg.MatchOn) == "" {
		return errors.New("salesforce destination: match_on is required")
	}
	if strings.TrimSpace(cfg.Options["object"]) == "" {
		return errors.New("salesforce destination: options.object is required")
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

	s.cfg = cfg
	s.auth = auth
	s.rateLimiter = rl
	s.breaker = cb
	s.writeParallelism = cfg.WriteParallelism
	s.client = &http.Client{Timeout: 30 * time.Second, Transport: transport}
	s.instanceURL = strings.TrimRight(cfg.URL, "/")
	s.object = cfg.Options["object"]
	s.matchOn = cfg.MatchOn
	return nil
}

// Load writes rows to Salesforce using REST upsert or Bulk API pipeline.
func (s *SalesforceDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, nil
	}
	if s.client == nil {
		s.client = &http.Client{Timeout: 30 * time.Second}
	}

	threshold := s.bulkThreshold()
	if len(rows) > threshold {
		if len(rows) > threshold*2 && s.writeParallelism > 1 {
			return s.loadBulkParallel(ctx, rows, store, pipeline, destName)
		}
		return s.loadBulk(ctx, rows, store, pipeline, destName)
	}
	return s.loadREST(ctx, rows, store, pipeline, destName)
}

// Close releases Salesforce connector resources.
func (s *SalesforceDestination) Close() error {
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
	return nil
}

func (s *SalesforceDestination) loadREST(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	pending := make([]row.Row, 0, len(rows))
	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		delivered, err := store.IsDelivered(ctx, rw.ID, pipeline, destName)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}
		if _, ok := rw.Data[s.matchOn]; !ok {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: fmt.Errorf("salesforce destination: missing %s", s.matchOn)})
			continue
		}
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	parallelism := s.writeParallelism
	if parallelism <= 0 {
		parallelism = 1
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

			if err := s.upsertREST(ctx, destName, rw); err != nil {
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

func (s *SalesforceDestination) loadBulkParallel(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	threshold := s.bulkThreshold()
	if threshold <= 0 {
		threshold = 200
	}
	if len(rows) <= threshold*2 {
		return s.loadBulk(ctx, rows, store, pipeline, destName)
	}

	batches := make([][]row.Row, 0, (len(rows)+threshold-1)/threshold)
	for start := 0; start < len(rows); start += threshold {
		end := start + threshold
		if end > len(rows) {
			end = len(rows)
		}
		batches = append(batches, rows[start:end])
	}

	parallelism := s.writeParallelism
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

			batchResult, err := s.loadBulk(ctx, batch, store, pipeline, destName)
			mu.Lock()
			defer mu.Unlock()
			result.Loaded += batchResult.Loaded
			result.Skipped += batchResult.Skipped
			result.Errors = append(result.Errors, batchResult.Errors...)
			if err != nil {
				result.Errors = append(result.Errors, RowError{Err: err})
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func (s *SalesforceDestination) upsertREST(ctx context.Context, destName string, rw row.Row) error {
	matchValue := url.PathEscape(fmt.Sprintf("%v", rw.Data[s.matchOn]))
	body := filterKey(rw.Data, s.matchOn)
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", s.instanceURL, salesforceAPIVersion, url.PathEscape(s.object), url.PathEscape(s.matchOn), matchValue)

	return httpauth.DoWithRetry(ctx, s.cfg.Retry, func() (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := s.allowRequest(ctx, destName); err != nil {
			return 0, err
		}
		if s.rateLimiter != nil {
			if err := s.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, endpoint, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if s.auth != nil {
			if err := s.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := s.client.Do(req)
		if err != nil {
			s.recordCircuitFailure(ctx, destName)
			return 0, err
		}
		defer resp.Body.Close()
		s.recordCircuitResponse(ctx, destName, resp.StatusCode)
		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, nil
	})
}

func (s *SalesforceDestination) loadBulk(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
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
		pending = append(pending, rw)
	}
	if len(pending) == 0 {
		return result, nil
	}

	jobID, err := s.createBulkJob(ctx, destName)
	if err != nil {
		return result, err
	}
	csvBytes, headers := rowsToCSV(pending)
	if err := s.uploadBulkCSV(ctx, destName, jobID, csvBytes); err != nil {
		return result, err
	}
	if err := s.closeBulkJob(ctx, destName, jobID); err != nil {
		return result, err
	}
	if err := s.waitForBulkJob(ctx, destName, jobID); err != nil {
		return result, err
	}
	loaded, errs := s.readBulkResults(ctx, destName, jobID, len(pending), pending)
	for _, rw := range pending {
		if err := store.MarkDelivered(ctx, rw.ID, pipeline, destName); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
	}
	result.Loaded = loaded
	result.Errors = append(result.Errors, errs...)
	_ = headers
	return result, nil
}

func (s *SalesforceDestination) createBulkJob(ctx context.Context, destName string) (string, error) {
	body := map[string]any{
		"object":              s.object,
		"operation":           "upsert",
		"externalIdFieldName": s.matchOn,
		"contentType":         "CSV",
		"lineEnding":          "LF",
	}
	payload, _ := json.Marshal(body)
	var jobID string
	err := httpauth.DoWithRetry(ctx, s.cfg.Retry, func() (int, error) {
		if err := s.allowRequest(ctx, destName); err != nil {
			return 0, err
		}
		if s.rateLimiter != nil {
			if err := s.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.instanceURL+"/services/data/"+salesforceAPIVersion+"/jobs/ingest", bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if s.auth != nil {
			if err := s.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := s.client.Do(req)
		if err != nil {
			s.recordCircuitFailure(ctx, destName)
			return 0, err
		}
		defer resp.Body.Close()
		s.recordCircuitResponse(ctx, destName, resp.StatusCode)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return resp.StatusCode, nil
		}
		var out struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return resp.StatusCode, err
		}
		jobID = out.ID
		return resp.StatusCode, nil
	})
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(jobID) == "" {
		return "", errors.New("salesforce bulk job id missing")
	}
	return jobID, nil
}

func (s *SalesforceDestination) uploadBulkCSV(ctx context.Context, destName, jobID string, csvBytes []byte) error {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", s.instanceURL, salesforceAPIVersion, url.PathEscape(jobID))
	return httpauth.DoWithRetry(ctx, s.cfg.Retry, func() (int, error) {
		if err := s.allowRequest(ctx, destName); err != nil {
			return 0, err
		}
		if s.rateLimiter != nil {
			if err := s.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(csvBytes))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "text/csv")
		if s.auth != nil {
			if err := s.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := s.client.Do(req)
		if err != nil {
			s.recordCircuitFailure(ctx, destName)
			return 0, err
		}
		defer resp.Body.Close()
		s.recordCircuitResponse(ctx, destName, resp.StatusCode)
		return resp.StatusCode, nil
	})
}

func (s *SalesforceDestination) closeBulkJob(ctx context.Context, destName, jobID string) error {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", s.instanceURL, salesforceAPIVersion, url.PathEscape(jobID))
	payload := []byte(`{"state":"UploadComplete"}`)
	return httpauth.DoWithRetry(ctx, s.cfg.Retry, func() (int, error) {
		if err := s.allowRequest(ctx, destName); err != nil {
			return 0, err
		}
		if s.rateLimiter != nil {
			if err := s.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, endpoint, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if s.auth != nil {
			if err := s.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := s.client.Do(req)
		if err != nil {
			s.recordCircuitFailure(ctx, destName)
			return 0, err
		}
		defer resp.Body.Close()
		s.recordCircuitResponse(ctx, destName, resp.StatusCode)
		return resp.StatusCode, nil
	})
}

func (s *SalesforceDestination) waitForBulkJob(ctx context.Context, destName, jobID string) error {
	deadline := time.NewTimer(10 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", s.instanceURL, salesforceAPIVersion, url.PathEscape(jobID))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("salesforce bulk job timed out")
		case <-ticker.C:
		}

		var state string
		err := httpauth.DoWithRetry(ctx, s.cfg.Retry, func() (int, error) {
			if err := s.allowRequest(ctx, destName); err != nil {
				return 0, err
			}
			if s.rateLimiter != nil {
				if err := s.rateLimiter.Wait(ctx); err != nil {
					return 0, err
				}
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
			if err != nil {
				return 0, err
			}
			if s.auth != nil {
				if err := s.auth.Apply(req); err != nil {
					return 0, err
				}
			}
			resp, err := s.client.Do(req)
			if err != nil {
				s.recordCircuitFailure(ctx, destName)
				return 0, err
			}
			defer resp.Body.Close()
			s.recordCircuitResponse(ctx, destName, resp.StatusCode)
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return resp.StatusCode, nil
			}
			var out struct {
				State string `json:"state"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				return resp.StatusCode, err
			}
			state = out.State
			return resp.StatusCode, nil
		})
		if err != nil {
			return err
		}
		switch strings.ToLower(strings.TrimSpace(state)) {
		case "jobcomplete", "completed":
			return nil
		case "failed", "aborted":
			return fmt.Errorf("salesforce bulk job failed: %s", state)
		case "":
			continue
		}
	}
}

func (s *SalesforceDestination) readBulkResults(ctx context.Context, destName, jobID string, fallback int, rows []row.Row) (int, []RowError) {
	endpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/successfulResults", s.instanceURL, salesforceAPIVersion, url.PathEscape(jobID))
	var successBody []byte
	if err := s.httpGet(ctx, destName, endpoint, &successBody); err != nil {
		return fallback, nil
	}
	count := countCSVRecords(successBody)
	if count <= 0 {
		count = fallback
	}

	failedEndpoint := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/failedResults", s.instanceURL, salesforceAPIVersion, url.PathEscape(jobID))
	var failedBody []byte
	_ = s.httpGet(ctx, destName, failedEndpoint, &failedBody)
	if len(bytes.TrimSpace(failedBody)) == 0 {
		return count, nil
	}
	return count, []RowError{{Err: fmt.Errorf("salesforce bulk job reported failures")}}
}

func (s *SalesforceDestination) httpGet(ctx context.Context, destName, endpoint string, dst *[]byte) error {
	return httpauth.DoWithRetry(ctx, s.cfg.Retry, func() (int, error) {
		if err := s.allowRequest(ctx, destName); err != nil {
			return 0, err
		}
		if s.rateLimiter != nil {
			if err := s.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return 0, err
		}
		if s.auth != nil {
			if err := s.auth.Apply(req); err != nil {
				return 0, err
			}
		}
		resp, err := s.client.Do(req)
		if err != nil {
			s.recordCircuitFailure(ctx, destName)
			return 0, err
		}
		defer resp.Body.Close()
		s.recordCircuitResponse(ctx, destName, resp.StatusCode)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return resp.StatusCode, nil
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp.StatusCode, err
		}
		*dst = body
		return resp.StatusCode, nil
	})
}

func (s *SalesforceDestination) allowRequest(ctx context.Context, destination string) error {
	if s.breaker == nil {
		return nil
	}
	before := s.breaker.State()
	if err := s.breaker.Allow(); err != nil {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Warn("circuit breaker open, skipping request",
			slog.String("destination", destination),
		)
		return err
	}
	if before == "open" && s.breaker.State() == "half_open" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Info("circuit breaker half-open, testing",
			slog.String("destination", destination),
		)
	}
	return nil
}

func (s *SalesforceDestination) recordCircuitFailure(ctx context.Context, destination string) {
	if s.breaker == nil {
		return
	}
	before := s.breaker.State()
	s.breaker.RecordFailure()
	if before != "open" && s.breaker.State() == "open" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Error("circuit breaker opened",
			slog.String("destination", destination),
			slog.Int("failures", s.cfg.CircuitBreaker.Threshold),
		)
	}
}

func (s *SalesforceDestination) recordCircuitResponse(ctx context.Context, destination string, statusCode int) {
	if s.breaker == nil {
		return
	}
	before := s.breaker.State()
	if statusCode >= http.StatusInternalServerError {
		s.recordCircuitFailure(ctx, destination)
		return
	}
	s.breaker.RecordSuccess()
	if before == "half_open" && s.breaker.State() == "closed" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Info("circuit breaker closed, destination recovered",
			slog.String("destination", destination),
		)
	}
}

func (s *SalesforceDestination) bulkThreshold() int {
	if raw := strings.TrimSpace(s.cfg.Options["bulk_threshold"]); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return 200
}

func rowsToCSV(rows []row.Row) ([]byte, []string) {
	if len(rows) == 0 {
		return nil, nil
	}
	headers := sortedKeys(rows[0].Data)
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(headers)
	for _, rw := range rows {
		record := make([]string, len(headers))
		for i, key := range headers {
			if val, ok := rw.Data[key]; ok && val != nil {
				record[i] = fmt.Sprintf("%v", val)
			}
		}
		_ = w.Write(record)
	}
	w.Flush()
	return buf.Bytes(), headers
}

func filterKey(data map[string]interface{}, drop string) map[string]interface{} {
	out := make(map[string]interface{}, len(data))
	for k, v := range data {
		if k == drop {
			continue
		}
		out[k] = v
	}
	return out
}

func sortedKeys(data map[string]interface{}) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func countCSVRecords(data []byte) int {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return 0
	}
	return bytes.Count(data, []byte{'\n'}) + 1
}
