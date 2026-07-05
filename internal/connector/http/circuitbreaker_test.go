package http

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/pkg/config"
)

func TestCircuitBreaker_InitiallyClosed(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 5, CooldownMs: 10})
	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 3, CooldownMs: 10})
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if !errors.Is(cb.Allow(), ErrCircuitOpen) {
		t.Fatal("expected open circuit")
	}
}

func TestCircuitBreaker_HalfOpenAfterCooldown(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 2, CooldownMs: 10})
	cb.RecordFailure()
	cb.RecordFailure()
	cb.openedAt = time.Now().Add(-time.Second)

	if err := cb.Allow(); err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	if state := cb.State(); state != "half_open" {
		t.Fatalf("state = %q, want half_open", state)
	}
	if !errors.Is(cb.Allow(), ErrCircuitOpen) {
		t.Fatal("expected second allow to be rejected in half-open")
	}
}

func TestCircuitBreaker_ClosesOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 2, CooldownMs: 10})
	cb.RecordFailure()
	cb.RecordFailure()
	cb.openedAt = time.Now().Add(-time.Second)

	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	cb.RecordSuccess()

	if state := cb.State(); state != "closed" {
		t.Fatalf("state = %q, want closed", state)
	}
	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow() after close error = %v", err)
	}
}

func TestCircuitBreaker_ReOpensOnHalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 2, CooldownMs: 10})
	cb.RecordFailure()
	cb.RecordFailure()
	cb.openedAt = time.Now().Add(-time.Second)

	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	cb.RecordFailure()

	if state := cb.State(); state != "open" {
		t.Fatalf("state = %q, want open", state)
	}
	if !errors.Is(cb.Allow(), ErrCircuitOpen) {
		t.Fatal("expected circuit to be open again")
	}
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 5, CooldownMs: 10})
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()

	if errors.Is(cb.Allow(), ErrCircuitOpen) {
		t.Fatal("expected circuit to remain closed")
	}
	cb.RecordFailure()
	if !errors.Is(cb.Allow(), ErrCircuitOpen) {
		t.Fatal("expected circuit to open after five failures")
	}
}

func TestCircuitBreaker_Disabled(t *testing.T) {
	if cb := NewCircuitBreaker(config.CircuitBreakerConfig{}); cb != nil {
		t.Fatalf("expected nil circuit breaker, got %#v", cb)
	}
}

func TestCircuitBreaker_Concurrent(t *testing.T) {
	cb := NewCircuitBreaker(config.CircuitBreakerConfig{Threshold: 5, CooldownMs: 10})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := cb.Allow(); err == nil {
				if i%2 == 0 {
					cb.RecordFailure()
				} else {
					cb.RecordSuccess()
				}
			}
		}(i)
	}
	wg.Wait()
}
