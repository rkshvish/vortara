// Package http provides shared HTTP authentication and retry helpers for connectors.
package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rkshvish/vortaraos/pkg/config"
)

// DoWithRetry executes fn up to cfg.Attempts times with exponential backoff.
// It retries on status codes in cfg.BackoffOn, drops immediately on cfg.DropOn,
// and always respects ctx cancellation during backoff.
func DoWithRetry(ctx context.Context, cfg config.RetryConfig, fn func() (statusCode int, err error)) error {
	cfg = defaultRetryConfig(cfg)
	backoffSet := make(map[int]struct{}, len(cfg.BackoffOn))
	for _, code := range cfg.BackoffOn {
		backoffSet[code] = struct{}{}
	}
	dropSet := make(map[int]struct{}, len(cfg.DropOn))
	for _, code := range cfg.DropOn {
		dropSet[code] = struct{}{}
	}

	var lastCode int
	for attempt := 0; attempt < cfg.Attempts; attempt++ {
		code, err := fn()
		lastCode = code

		if err == nil && code >= http.StatusOK && code < http.StatusMultipleChoices {
			return nil
		}
		if errors.Is(err, ErrCircuitOpen) {
			return err
		}
		if _, ok := dropSet[code]; ok {
			return fmt.Errorf("permanent failure: HTTP %d", code)
		}
		if attempt == cfg.Attempts-1 {
			return fmt.Errorf("exhausted %d attempts, last status: %d", cfg.Attempts, code)
		}
		if _, ok := backoffSet[code]; ok || err != nil {
			backoff := time.Duration(cfg.BackoffMs) * time.Millisecond * time.Duration(1<<attempt)
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}

	return fmt.Errorf("exhausted retries, last status: %d", lastCode)
}

func defaultRetryConfig(cfg config.RetryConfig) config.RetryConfig {
	if cfg.Attempts == 0 {
		cfg.Attempts = 3
	}
	if cfg.BackoffMs == 0 {
		cfg.BackoffMs = 1000
	}
	if len(cfg.BackoffOn) == 0 {
		cfg.BackoffOn = []int{429, 500, 502, 503, 504}
	}
	if len(cfg.DropOn) == 0 {
		cfg.DropOn = []int{400, 401, 403, 404, 409}
	}
	return cfg
}
