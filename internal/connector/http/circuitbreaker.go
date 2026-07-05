// Package http provides shared HTTP authentication, retry, and resilience helpers for connectors.
package http

import (
	"errors"
	"sync"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

// ErrCircuitOpen is returned when a circuit breaker rejects a request.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreaker guards outbound HTTP requests after repeated failures.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            breakerState
	failures         int
	threshold        int
	cooldown         time.Duration
	openedAt         time.Time
	halfOpenLock     sync.Mutex
	halfOpenInFlight bool
}

// NewCircuitBreaker creates a new CircuitBreaker.
func NewCircuitBreaker(cfg config.CircuitBreakerConfig) *CircuitBreaker {
	if cfg.Threshold == 0 {
		return nil
	}
	cooldown := time.Duration(cfg.CooldownMs) * time.Millisecond
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &CircuitBreaker{
		state:     stateClosed,
		threshold: cfg.Threshold,
		cooldown:  cooldown,
	}
}

// Allow reports whether the next request may proceed.
func (cb *CircuitBreaker) Allow() error {
	if cb == nil {
		return nil
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil
	case stateOpen:
		if time.Since(cb.openedAt) < cb.cooldown {
			return ErrCircuitOpen
		}
		cb.state = stateHalfOpen
		cb.failures = 0
		fallthrough
	case stateHalfOpen:
		if cb.halfOpenInFlight {
			return ErrCircuitOpen
		}
		if !cb.halfOpenLock.TryLock() {
			return ErrCircuitOpen
		}
		cb.halfOpenInFlight = true
		return nil
	default:
		return nil
	}
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == stateHalfOpen {
		cb.state = stateClosed
		cb.failures = 0
		cb.openedAt = time.Time{}
		cb.releaseHalfOpen()
		return
	}
	cb.failures = 0
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateHalfOpen:
		cb.state = stateOpen
		cb.failures = cb.threshold
		cb.openedAt = time.Now()
		cb.releaseHalfOpen()
	case stateClosed:
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.state = stateOpen
			cb.openedAt = time.Now()
		}
	case stateOpen:
		cb.openedAt = time.Now()
	}
}

// State returns the breaker state name.
func (cb *CircuitBreaker) State() string {
	if cb == nil {
		return "disabled"
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

func (cb *CircuitBreaker) releaseHalfOpen() {
	if cb.halfOpenInFlight {
		cb.halfOpenInFlight = false
		cb.halfOpenLock.Unlock()
	}
}
