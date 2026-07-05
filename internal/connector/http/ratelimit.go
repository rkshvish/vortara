// Package http provides shared HTTP authentication and retry helpers for connectors.
package http

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rkshvish/vortaraos/pkg/config"
)

// CommonRateLimits lists common API rate limits for reference.
var CommonRateLimits = map[string]config.RateLimitConfig{
	"salesforce": {Requests: 100, Period: "10s"},
	"hubspot":    {Requests: 100, Period: "10s"},
	"slack":      {Requests: 1, Period: "1s"},
	"sheets":     {Requests: 60, Period: "60s"},
}

// RateLimiter provides token-bucket rate limiting for outbound HTTP requests.
type RateLimiter struct {
	tokens chan struct{}
	ticker *time.Ticker
	stopCh chan struct{}
	once   sync.Once
}

// NewRateLimiter creates a token-bucket rate limiter.
// It returns nil when requests is zero or period is empty.
func NewRateLimiter(cfg config.RateLimitConfig) (*RateLimiter, error) {
	if cfg.Requests < 0 {
		return nil, fmt.Errorf("rate limit requests must be >= 0")
	}
	if cfg.Requests == 0 || strings.TrimSpace(cfg.Period) == "" {
		return nil, nil
	}

	period, err := time.ParseDuration(cfg.Period)
	if err != nil {
		return nil, err
	}

	interval := period / time.Duration(cfg.Requests)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	rl := &RateLimiter{
		tokens: make(chan struct{}, cfg.Requests),
		stopCh: make(chan struct{}),
	}
	for i := 0; i < cfg.Requests; i++ {
		rl.tokens <- struct{}{}
	}
	rl.ticker = time.NewTicker(interval)

	go rl.refill()
	return rl, nil
}

func (r *RateLimiter) refill() {
	for {
		select {
		case <-r.ticker.C:
			select {
			case r.tokens <- struct{}{}:
			default:
			}
		case <-r.stopCh:
			return
		}
	}
}

// Wait blocks until a token is available or the context is cancelled.
func (r *RateLimiter) Wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	select {
	case <-r.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop shuts down the refill goroutine.
func (r *RateLimiter) Stop() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		close(r.stopCh)
		if r.ticker != nil {
			r.ticker.Stop()
		}
	})
}
