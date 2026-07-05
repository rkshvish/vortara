package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newMockClient(fn roundTripperFunc) *http.Client {
	return &http.Client{Transport: fn}
}

func newResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newRESTAPIDestinationForTest(t *testing.T, url string, headers map[string]string) *RESTAPIDestination {
	t.Helper()
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{
		URL:     url,
		Headers: headers,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return dst
}

func mustReadBody(t *testing.T, r *http.Request) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	_ = r.Body.Close()
	return body
}

func testRow() row.Row {
	return row.Row{ID: "row-1", Data: map[string]interface{}{"id": 1, "name": "foo"}}
}

func restRows(n int) []row.Row {
	rows := make([]row.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = row.Row{
			ID: fmt.Sprintf("row-%d", i),
			Data: map[string]interface{}{
				"id":   i,
				"name": fmt.Sprintf("name-%d", i),
			},
		}
	}
	return rows
}

// TestRESTAPIDestination_Connect_EmptyURL verifies empty URLs are rejected.
func TestRESTAPIDestination_Connect_EmptyURL(t *testing.T) {
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{}); err == nil {
		t.Fatal("expected error for empty url")
	}
}

func TestRESTAPIDestination_TransportConfigured(t *testing.T) {
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{
		URL:              "http://example.com",
		WriteParallelism: 5,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	transport, ok := dst.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", dst.client.Transport)
	}
	if transport.MaxConnsPerHost != 5 {
		t.Fatalf("MaxConnsPerHost = %d, want 5", transport.MaxConnsPerHost)
	}
}

// TestRESTAPIDestination_Load_Success verifies a row is posted successfully.
func TestRESTAPIDestination_Load_Success(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{
		URL: "http://example.com",
		Auth: config.AuthConfig{
			Type:  "bearer",
			Token: "tok",
		},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Fatalf("unexpected auth header %q", got)
		}
		if got := r.Header.Get("X-Idempotency-Key"); got != "row-1" {
			t.Fatalf("unexpected idempotency key %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("unexpected content type %q", got)
		}
		body := mustReadBody(t, r)
		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		if payload["name"] != "foo" {
			t.Fatalf("unexpected payload: %+v", payload)
		}
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 1 || result.Skipped != 0 || len(result.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", calls)
	}
}

func TestRESTAPIDestination_WriteParallelism(t *testing.T) {
	var calls int32
	var inflight int32
	var maxInflight int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{
		URL:              "http://example.com",
		WriteParallelism: 3,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		cur := atomic.AddInt32(&inflight, 1)
		for {
			max := atomic.LoadInt32(&maxInflight)
			if cur <= max || atomic.CompareAndSwapInt32(&maxInflight, max, cur) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		atomic.AddInt32(&calls, 1)
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), restRows(9), store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 9 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 9 {
		t.Fatalf("expected 9 HTTP calls, got %d", calls)
	}
	if atomic.LoadInt32(&maxInflight) != 3 {
		t.Fatalf("maxInflight = %d, want 3", maxInflight)
	}
}

func TestRESTAPIDestination_AuthAPIKey(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{
		URL: "http://example.com",
		Auth: config.AuthConfig{
			Type:     "api_key",
			Key:      "X-API-Key",
			Value:    "secret",
			InHeader: true,
		},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("X-API-Key"); got != "secret" {
			t.Fatalf("unexpected auth header %q", got)
		}
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()
	if _, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest"); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", calls)
	}
}

// TestRESTAPIDestination_Load_AlreadyDelivered verifies delivered rows are skipped.
func TestRESTAPIDestination_Load_AlreadyDelivered(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()
	if err := store.MarkDelivered("row-1", "pipe", "dest"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Skipped != 1 || result.Loaded != 0 || len(result.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("expected no HTTP calls, got %d", calls)
	}
}

// TestRESTAPIDestination_Load_Retry verifies retryable failures are retried.
func TestRESTAPIDestination_Load_Retry(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return newResponse(http.StatusInternalServerError, ""), nil
		}
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 1 || len(result.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 HTTP calls, got %d", calls)
	}
}

// TestRESTAPIDestination_Load_NonRetryable verifies 400 errors are not retried.
func TestRESTAPIDestination_Load_NonRetryable(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return newResponse(http.StatusBadRequest, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(result.Errors) != 1 || result.Loaded != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", calls)
	}
}

// TestRESTAPIDestination_Load_ExhaustedRetries verifies repeated retryable failures surface as row errors.
func TestRESTAPIDestination_Load_ExhaustedRetries(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return newResponse(http.StatusServiceUnavailable, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(result.Errors) != 1 || result.Loaded != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 HTTP calls, got %d", calls)
	}
}

// TestRESTAPIDestination_Load_MultipleRows verifies multiple rows are written.
func TestRESTAPIDestination_Load_MultipleRows(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()
	rows := []row.Row{
		{ID: "row-1", Data: map[string]interface{}{"name": "a"}},
		{ID: "row-2", Data: map[string]interface{}{"name": "b"}},
		{ID: "row-3", Data: map[string]interface{}{"name": "c"}},
	}

	result, err := dst.Load(context.Background(), rows, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 3 || len(result.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 HTTP calls, got %d", calls)
	}
}

// TestRESTAPIDestination_Load_CtxCancel verifies cancellation is returned as a top-level error.
func TestRESTAPIDestination_Load_CtxCancel(t *testing.T) {
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		t.Fatal("request should not be sent after context cancellation")
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := dst.Load(ctx, []row.Row{testRow()}, store, "pipe", "dest")
	if err == nil {
		t.Fatal("expected context error")
	}
}

// TestRESTAPIDestination_Headers verifies custom headers are sent on every request.
func TestRESTAPIDestination_Headers(t *testing.T) {
	var calls int32
	var mu sync.Mutex
	headers := make([]string, 0, 3)
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com", Headers: map[string]string{"X-Api-Key": "secret"}}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		mu.Lock()
		headers = append(headers, r.Header.Get("X-Api-Key"))
		mu.Unlock()
		n := atomic.LoadInt32(&calls)
		if n < 3 {
			return newResponse(http.StatusInternalServerError, ""), nil
		}
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 HTTP calls, got %d", calls)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, got := range headers {
		if got != "secret" {
			t.Fatalf("request %d header mismatch: %q", i, got)
		}
	}
}

// TestRESTAPIDestination_Load_Retry_then_MarkDelivered ensures delivery is recorded after success.
func TestRESTAPIDestination_Load_Retry_then_MarkDelivered(t *testing.T) {
	var calls int32
	dst := NewRESTAPIDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{URL: "http://example.com"}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		if atomic.LoadInt32(&calls) < 2 {
			return newResponse(http.StatusBadGateway, ""), nil
		}
		return newResponse(http.StatusOK, ""), nil
	})
	store := state.NewMemoryStore()

	result, err := dst.Load(context.Background(), []row.Row{testRow()}, store, "pipe", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	ok, err := store.IsDelivered("row-1", "pipe", "dest")
	if err != nil {
		t.Fatalf("IsDelivered() error = %v", err)
	}
	if !ok {
		t.Fatal("expected row to be marked delivered")
	}
}
