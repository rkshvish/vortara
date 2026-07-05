package source

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func newWebhookSourceForTest(t *testing.T, path string, opts map[string]string) *WebhookSource {
	t.Helper()
	src := NewWebhookSource()
	cfg := config.StreamingConfig{Path: path, Options: opts}
	if err := src.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return src
}

func invokeWebhook(t *testing.T, src *WebhookSource, body []byte, headers map[string]string, out chan row.Row) (*httptest.ResponseRecorder, chan struct{}) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://example.com"+src.cfg.Path, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		src.handleWebhook(out).ServeHTTP(rr, req)
		close(done)
	}()
	return rr, done
}

// TestWebhookSource_Connect_EmptyPath verifies empty paths are rejected.
func TestWebhookSource_Connect_EmptyPath(t *testing.T) {
	src := NewWebhookSource()
	if err := src.Connect(context.Background(), config.StreamingConfig{}); err == nil {
		t.Fatal("expected error for empty path")
	}
}

// TestWebhookSource_Receive_And_Ack verifies a webhook request is acked with 200.
func TestWebhookSource_Receive_And_Ack(t *testing.T) {
	src := newWebhookSourceForTest(t, "/webhooks/deals", nil)
	out := make(chan row.Row, 1)
	rr, done := invokeWebhook(t, src, []byte(`{"id":1,"name":"deal"}`), nil, out)

	r := <-out
	if r.PrimaryKey != "id=1" {
		t.Fatalf("unexpected primary key %q", r.PrimaryKey)
	}
	if err := src.Ack(context.Background(), r.ID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	<-done
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

// TestWebhookSource_Receive_And_Nack verifies Nack causes a 500.
func TestWebhookSource_Receive_And_Nack(t *testing.T) {
	src := newWebhookSourceForTest(t, "/webhooks/deals", nil)
	out := make(chan row.Row, 1)
	rr, done := invokeWebhook(t, src, []byte(`{"id":2,"name":"deal"}`), nil, out)

	r := <-out
	if err := src.Nack(context.Background(), r.ID); err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	<-done
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

// TestWebhookSource_HMAC_Valid verifies valid signatures are accepted.
func TestWebhookSource_HMAC_Valid(t *testing.T) {
	src := newWebhookSourceForTest(t, "/webhooks/hmac", nil)
	src.cfg.Secret = "testsecret"
	out := make(chan row.Row, 1)
	body := []byte(`{"id":3,"name":"deal"}`)
	mac := hmac.New(sha256.New, []byte("testsecret"))
	_, _ = mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	rr, done := invokeWebhook(t, src, body, map[string]string{"X-Vortara-Signature": sig}, out)

	r := <-out
	if err := src.Ack(context.Background(), r.ID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	<-done
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

// TestWebhookSource_HMAC_Invalid verifies invalid signatures are rejected.
func TestWebhookSource_HMAC_Invalid(t *testing.T) {
	src := newWebhookSourceForTest(t, "/webhooks/hmac", nil)
	src.cfg.Secret = "testsecret"
	out := make(chan row.Row, 1)
	rr, done := invokeWebhook(t, src, []byte(`{"id":4}`), map[string]string{"X-Vortara-Signature": "sha256=badsig"}, out)

	<-done
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	select {
	case got := <-out:
		t.Fatalf("unexpected row: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWebhookSource_Timeout verifies unacked requests time out.
func TestWebhookSource_Timeout(t *testing.T) {
	src := newWebhookSourceForTest(t, "/webhooks/timeout", map[string]string{"ack_timeout_ms": "50"})
	out := make(chan row.Row, 1)
	rr, done := invokeWebhook(t, src, []byte(`{"id":5}`), nil, out)

	<-out
	<-done
	if rr.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", rr.Code)
	}
}

// TestWebhookSource_InvalidJSON verifies invalid payloads are rejected.
func TestWebhookSource_InvalidJSON(t *testing.T) {
	src := newWebhookSourceForTest(t, "/webhooks/json", nil)
	out := make(chan row.Row, 1)
	rr, done := invokeWebhook(t, src, []byte(`not-json`), nil, out)

	<-done
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	select {
	case got := <-out:
		t.Fatalf("unexpected row: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}
