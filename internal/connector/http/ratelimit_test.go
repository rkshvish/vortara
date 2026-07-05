package http

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/pkg/config"
)

func TestNewRateLimiter_NoConfig(t *testing.T) {
	rl, err := NewRateLimiter(config.RateLimitConfig{})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	if rl != nil {
		t.Fatal("expected nil limiter")
	}

	rl, err = NewRateLimiter(config.RateLimitConfig{Period: ""})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	if rl != nil {
		t.Fatal("expected nil limiter for empty period")
	}
}

func TestNewRateLimiter_InvalidPeriod(t *testing.T) {
	if _, err := NewRateLimiter(config.RateLimitConfig{Requests: 1, Period: "notaduration"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRateLimiter_AllowsBurst(t *testing.T) {
	rl, err := NewRateLimiter(config.RateLimitConfig{Requests: 10, Period: "1s"})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	defer rl.Stop()

	for i := 0; i < 10; i++ {
		if err := rl.Wait(context.Background()); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- rl.Wait(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("expected 11th wait to block, got %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRateLimiter_Throttles(t *testing.T) {
	rl, err := NewRateLimiter(config.RateLimitConfig{Requests: 2, Period: "100ms"})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	defer rl.Stop()

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	start := time.Now()
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("expected throttling, elapsed=%s", elapsed)
	}
}

func TestRateLimiter_CtxCancel(t *testing.T) {
	rl, err := NewRateLimiter(config.RateLimitConfig{Requests: 1, Period: "1s"})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	defer rl.Stop()

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rl.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRateLimiter_Stop(t *testing.T) {
	rl, err := NewRateLimiter(config.RateLimitConfig{Requests: 1, Period: "1s"})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	rl.Stop()
	rl.Stop()
}

func TestRateLimiter_Concurrent(t *testing.T) {
	rl, err := NewRateLimiter(config.RateLimitConfig{Requests: 10, Period: "100ms"})
	if err != nil {
		t.Fatalf("NewRateLimiter() error = %v", err)
	}
	defer rl.Stop()

	var calls int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rl.Wait(context.Background()); err != nil {
				t.Errorf("Wait() error = %v", err)
				return
			}
			atomic.AddInt32(&calls, 1)
		}()
	}
	wg.Wait()
	if calls != 5 {
		t.Fatalf("calls = %d, want 5", calls)
	}
}
