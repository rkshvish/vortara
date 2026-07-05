package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
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

// RESTAPIDestination delivers rows to a REST API over HTTP.
type RESTAPIDestination struct {
	cfg              config.DestinationConfig
	client           *http.Client
	auth             httpauth.Authenticator
	rateLimiter      *httpauth.RateLimiter
	breaker          *httpauth.CircuitBreaker
	writeParallelism int
}

var _ Destination = (*RESTAPIDestination)(nil)

func init() {
	registry.RegisterDestination("restapi", func() any {
		return NewRESTAPIDestination()
	})
}

// NewRESTAPIDestination returns a new RESTAPIDestination with a 30 second timeout.
func NewRESTAPIDestination() *RESTAPIDestination {
	return &RESTAPIDestination{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Connect validates the destination URL and stores the configuration.
func (r *RESTAPIDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	if strings.TrimSpace(cfg.URL) == "" {
		return errors.New("rest api destination: url is required")
	}
	if _, err := url.ParseRequestURI(cfg.URL); err != nil {
		return err
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
	if strings.TrimSpace(cfg.Method) == "" {
		cfg.Method = http.MethodPost
	}
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
	r.cfg = cfg
	r.auth = auth
	r.rateLimiter = rl
	r.breaker = cb
	r.writeParallelism = cfg.WriteParallelism
	r.client = &http.Client{Timeout: 30 * time.Second, Transport: transport}
	return nil
}

// Load writes rows to the REST API destination with per-row idempotency.
func (r *RESTAPIDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if r.client == nil {
		r.client = &http.Client{Timeout: 30 * time.Second}
	}

	pending := make([]row.Row, 0, len(rows))
	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		delivered, err := store.IsDelivered(rw.ID, pipeline, destination)
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

	parallelism := r.writeParallelism
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

			body, err := marshalRowData(rw.Data)
			if err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
				mu.Unlock()
				return
			}

			statusErr := r.sendWithRetry(ctx, destination, rw.ID, body)
			if statusErr != nil {
				if errors.Is(statusErr, context.Canceled) || errors.Is(statusErr, context.DeadlineExceeded) {
					return
				}
				mu.Lock()
				result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: statusErr})
				mu.Unlock()
				return
			}

			if err := store.MarkDelivered(rw.ID, pipeline, destination); err != nil {
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

// Close releases all resources.
func (r *RESTAPIDestination) Close() error {
	if r.rateLimiter != nil {
		r.rateLimiter.Stop()
	}
	return nil
}

func (r *RESTAPIDestination) sendWithRetry(ctx context.Context, destination, rowID string, body []byte) error {
	return httpauth.DoWithRetry(ctx, r.cfg.Retry, func() (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := r.allowRequest(ctx, destination); err != nil {
			return 0, err
		}
		if r.rateLimiter != nil {
			if err := r.rateLimiter.Wait(ctx); err != nil {
				return 0, err
			}
		}

		req, err := http.NewRequestWithContext(ctx, r.cfg.Method, r.cfg.URL, bytes.NewReader(body))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Idempotency-Key", rowID)
		for k, v := range r.cfg.Headers {
			req.Header.Set(k, v)
		}
		if r.auth != nil {
			if err := r.auth.Apply(req); err != nil {
				return 0, err
			}
		}

		resp, err := r.client.Do(req)
		if err != nil {
			r.recordCircuitFailure(ctx, destination)
			return 0, err
		}
		defer resp.Body.Close()
		r.recordCircuitResponse(ctx, destination, resp.StatusCode)
		return resp.StatusCode, nil
	})
}

func (r *RESTAPIDestination) allowRequest(ctx context.Context, destination string) error {
	if r.breaker == nil {
		return nil
	}
	before := r.breaker.State()
	if err := r.breaker.Allow(); err != nil {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Warn("circuit breaker open, skipping request",
			slog.String("destination", destination),
		)
		return err
	}
	if before == "open" && r.breaker.State() == "half_open" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Info("circuit breaker half-open, testing",
			slog.String("destination", destination),
		)
	}
	return nil
}

func (r *RESTAPIDestination) recordCircuitFailure(ctx context.Context, destination string) {
	if r.breaker == nil {
		return
	}
	before := r.breaker.State()
	r.breaker.RecordFailure()
	if before != "open" && r.breaker.State() == "open" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Error("circuit breaker opened",
			slog.String("destination", destination),
			slog.Int("failures", r.cfg.CircuitBreaker.Threshold),
		)
	}
}

func (r *RESTAPIDestination) recordCircuitResponse(ctx context.Context, destination string, statusCode int) {
	if r.breaker == nil {
		return
	}
	before := r.breaker.State()
	if statusCode >= http.StatusInternalServerError {
		r.recordCircuitFailure(ctx, destination)
		return
	}
	r.breaker.RecordSuccess()
	if before == "half_open" && r.breaker.State() == "closed" {
		vlogger.WithDestination(vlogger.FromContext(ctx), destination).Info("circuit breaker closed, destination recovered",
			slog.String("destination", destination),
		)
	}
}

func marshalRowData(data map[string]interface{}) ([]byte, error) {
	if data == nil {
		return []byte("{}"), nil
	}
	return jsonMarshal(data)
}

var jsonMarshal = func(v any) ([]byte, error) {
	return json.Marshal(v)
}
