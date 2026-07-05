package source

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	httpauth "github.com/rkshvish/vortara/internal/connector/http"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newMockSourceClient(fn roundTripperFunc) *http.Client {
	return &http.Client{Transport: fn}
}

func newRESTSource(serverURL string, opts map[string]string) *RESTAPISource {
	return &RESTAPISource{
		cfg: config.SourceConfig{
			Type:       "restapi",
			Connection: serverURL,
			Options:    opts,
		},
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func readRows(t *testing.T, ch <-chan row.Row) []row.Row {
	t.Helper()
	var rows []row.Row
	for r := range ch {
		rows = append(rows, r)
	}
	return rows
}

// TestRESTAPISource_Connect_InvalidURL verifies invalid URLs are rejected.
func TestRESTAPISource_Connect_InvalidURL(t *testing.T) {
	src := NewRESTAPISource()
	if err := src.Connect(context.Background(), config.SourceConfig{Connection: "://bad"}); err == nil {
		t.Fatal("expected invalid URL error")
	}
}

// TestRESTAPISource_Extract_ArrayResponse verifies array payloads are parsed.
func TestRESTAPISource_Extract_ArrayResponse(t *testing.T) {
	src := newRESTSource("http://example.com", map[string]string{"pipeline": "rest-sync"})
	src.auth = &httpauth.BearerAuth{Token: "tok"}
	src.client = newMockSourceClient(func(r *http.Request) (*http.Response, error) {
		if got := r.URL.Query().Get("since"); got == "" {
			t.Fatal("expected since query param")
		}
		if got := r.URL.Query().Get("until"); got == "" {
			t.Fatal("expected until query param")
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Fatalf("unexpected auth header %q", got)
		}
		resp := newResponse(http.StatusOK, `[{"id":1,"name":"foo","updated_at":"2026-01-01T10:00:00Z"},{"id":2,"name":"bar","updated_at":"2026-01-01T11:00:00Z"}]`)
		resp.Header.Set("Content-Type", "application/json")
		return resp, nil
	})
	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), out)
	}()

	rows := readRows(t, out)
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].PrimaryKey != "id=1" || rows[0].Data["name"] != "foo" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[0].Watermark.UTC().Format(time.RFC3339) != "2026-01-01T10:00:00Z" {
		t.Fatalf("unexpected watermark: %v", rows[0].Watermark)
	}
}

// TestRESTAPISource_Extract_EnvelopeResponse verifies cursor pagination.
func TestRESTAPISource_Extract_EnvelopeResponse(t *testing.T) {
	var calls int
	src := newRESTSource("http://example.com", nil)
	src.client = newMockSourceClient(func(r *http.Request) (*http.Response, error) {
		calls++
		resp := newResponse(http.StatusOK, "")
		resp.Header.Set("Content-Type", "application/json")
		switch calls {
		case 1:
			if got := r.URL.Query().Get("since"); got == "" {
				t.Fatal("expected since query param on first page")
			}
			resp.Body = io.NopCloser(strings.NewReader(`{"data":[{"id":1,"name":"foo","updated_at":"2026-01-01T10:00:00Z"}],"next_cursor":"abc123"}`))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "abc123" {
				t.Fatalf("expected cursor abc123, got %q", got)
			}
			resp.Body = io.NopCloser(strings.NewReader(`{"data":[{"id":2,"name":"bar","updated_at":"2026-01-01T11:00:00Z"}]}`))
		default:
			t.Fatalf("unexpected call %d", calls)
		}
		return resp, nil
	})
	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), out)
	}()

	rows := readRows(t, out)
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// TestRESTAPISource_Extract_Empty verifies empty responses produce no rows.
func TestRESTAPISource_Extract_Empty(t *testing.T) {
	src := newRESTSource("http://example.com", nil)
	src.client = newMockSourceClient(func(r *http.Request) (*http.Response, error) {
		resp := newResponse(http.StatusOK, `[]`)
		resp.Header.Set("Content-Type", "application/json")
		return resp, nil
	})
	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
	}()

	rows := readRows(t, out)
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
}

// TestRESTAPISource_Extract_HTTPError verifies non-2xx responses fail.
func TestRESTAPISource_Extract_HTTPError(t *testing.T) {
	src := newRESTSource("http://example.com", nil)
	src.client = newMockSourceClient(func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusInternalServerError, "boom"), nil
	})
	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
	}()

	_ = readRows(t, out)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "REST API returned 500") {
		t.Fatalf("expected HTTP 500 error, got %v", err)
	}
}

// TestRESTAPISource_Extract_CtxCancel verifies cancellation between pages.
func TestRESTAPISource_Extract_CtxCancel(t *testing.T) {
	var calls int
	src := newRESTSource("http://example.com", nil)
	src.client = newMockSourceClient(func(r *http.Request) (*http.Response, error) {
		calls++
		resp := newResponse(http.StatusOK, "")
		resp.Header.Set("Content-Type", "application/json")
		if calls == 1 {
			resp.Body = io.NopCloser(strings.NewReader(`{"data":[{"id":1,"name":"foo","updated_at":"2026-01-01T10:00:00Z"}],"next_cursor":"abc123"}`))
			return resp, nil
		}
		t.Fatal("second page should not be requested after cancellation")
		return resp, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(ctx, time.Time{}, time.Time{}, out)
	}()

	rows := 0
	for range out {
		rows++
		cancel()
	}

	if err := <-done; err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if rows == 0 {
		t.Fatal("expected at least one row before cancellation")
	}
}

// TestRESTAPISource_AuthHeader verifies auth headers are applied.
func TestRESTAPISource_AuthHeader(t *testing.T) {
	headerSeen := make(chan string, 1)
	src := newRESTSource("http://example.com", nil)
	src.auth = &httpauth.APIKeyAuth{Key: "X-API-Key", Value: "secret", InHeader: true}
	src.client = newMockSourceClient(func(r *http.Request) (*http.Response, error) {
		headerSeen <- r.Header.Get("X-API-Key")
		resp := newResponse(http.StatusOK, `[]`)
		resp.Header.Set("Content-Type", "application/json")
		return resp, nil
	})
	out := make(chan row.Row)
	done := make(chan error, 1)
	go func() {
		done <- src.Extract(context.Background(), time.Time{}, time.Time{}, out)
	}()

	_ = readRows(t, out)
	if err := <-done; err != nil {
		t.Fatalf("Extract() error = %v", err)
	}

	select {
	case got := <-headerSeen:
		if got != "secret" {
			t.Fatalf("expected header secret, got %q", got)
		}
	default:
		t.Fatal("expected header to be observed")
	}
}
