package http

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

func TestDoWithRetry_Success_FirstAttempt(t *testing.T) {
	var calls int32
	err := DoWithRetry(context.Background(), config.RetryConfig{Attempts: 3, BackoffMs: 1}, func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 200, nil
	})
	if err != nil {
		t.Fatalf("DoWithRetry() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoWithRetry_RetryOn500(t *testing.T) {
	var calls int32
	err := DoWithRetry(context.Background(), config.RetryConfig{Attempts: 3, BackoffMs: 1, BackoffOn: []int{500}}, func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return 500, nil
		}
		return 200, nil
	})
	if err != nil {
		t.Fatalf("DoWithRetry() error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestDoWithRetry_DropOn400(t *testing.T) {
	var calls int32
	err := DoWithRetry(context.Background(), config.RetryConfig{Attempts: 3, BackoffMs: 1, DropOn: []int{400}}, func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 400, nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoWithRetry_ExhaustRetries(t *testing.T) {
	var calls int32
	err := DoWithRetry(context.Background(), config.RetryConfig{Attempts: 3, BackoffMs: 1, BackoffOn: []int{503}}, func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 503, nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestDoWithRetry_ExponentialBackoff(t *testing.T) {
	var calls int32
	start := time.Now()
	err := DoWithRetry(context.Background(), config.RetryConfig{Attempts: 2, BackoffMs: 10, BackoffOn: []int{500}}, func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return 500, nil
		}
		return 200, nil
	})
	if err != nil {
		t.Fatalf("DoWithRetry() error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("backoff too short: %s", elapsed)
	}
}

func TestDoWithRetry_CtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	err := DoWithRetry(ctx, config.RetryConfig{Attempts: 3, BackoffMs: 100, BackoffOn: []int{500}}, func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 500, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoWithRetry_DefaultConfig(t *testing.T) {
	var calls int32
	err := DoWithRetry(context.Background(), config.RetryConfig{}, func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return 500, nil
		}
		return 200, nil
	})
	if err != nil {
		t.Fatalf("DoWithRetry() error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}
