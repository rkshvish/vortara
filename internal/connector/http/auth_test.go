package http

import (
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

type roundTripperFunc func(*nethttp.Request) (*nethttp.Response, error)

func (f roundTripperFunc) RoundTrip(r *nethttp.Request) (*nethttp.Response, error) {
	return f(r)
}

func newJSONResponse(status int, body string) *nethttp.Response {
	return &nethttp.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, nethttp.StatusText(status)),
		Header:     make(nethttp.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestBearerAuth_Apply(t *testing.T) {
	req, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	auth := &BearerAuth{Token: "mytoken"}
	if err := auth.Apply(req); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer mytoken" {
		t.Fatalf("Authorization = %q, want Bearer mytoken", got)
	}
}

func TestBearerAuth_EmptyToken(t *testing.T) {
	if _, err := NewAuthenticator(config.AuthConfig{Type: "bearer"}); err == nil {
		t.Fatal("expected error for empty bearer token")
	}
}

func TestAPIKeyAuth_Header(t *testing.T) {
	req, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	auth := &APIKeyAuth{Key: "X-API-Key", Value: "secret", InHeader: true}
	if err := auth.Apply(req); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got := req.Header.Get("X-API-Key"); got != "secret" {
		t.Fatalf("header = %q, want secret", got)
	}
}

func TestAPIKeyAuth_QueryParam(t *testing.T) {
	req, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	auth := &APIKeyAuth{Key: "api_key", Value: "secret", InHeader: false}
	if err := auth.Apply(req); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got := req.URL.Query().Get("api_key"); got != "secret" {
		t.Fatalf("query param = %q, want secret", got)
	}
}

func TestBasicAuth_Apply(t *testing.T) {
	req, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	auth := &BasicAuth{Username: "user", Password: "pass"}
	if err := auth.Apply(req); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	user, pass, ok := req.BasicAuth()
	if !ok || user != "user" || pass != "pass" {
		t.Fatalf("BasicAuth() = %q, %q, %v", user, pass, ok)
	}
}

func TestOAuth2_CachesToken(t *testing.T) {
	var calls int
	auth := &OAuth2ClientCredentials{
		ClientID:     "id",
		ClientSecret: "secret",
		TokenURL:     "https://token.example.com/token",
		client: &nethttp.Client{Transport: roundTripperFunc(func(r *nethttp.Request) (*nethttp.Response, error) {
			calls++
			return newJSONResponse(200, `{"access_token":"tok1","expires_in":3600}`), nil
		})},
	}

	req1, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	if err := auth.Apply(req1); err != nil {
		t.Fatalf("Apply() #1 error = %v", err)
	}
	req2, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	if err := auth.Apply(req2); err != nil {
		t.Fatalf("Apply() #2 error = %v", err)
	}

	if calls != 1 {
		t.Fatalf("token endpoint calls = %d, want 1", calls)
	}
	if got := req2.Header.Get("Authorization"); got != "Bearer tok1" {
		t.Fatalf("Authorization = %q, want Bearer tok1", got)
	}
}

func TestOAuth2_RefreshesExpiredToken(t *testing.T) {
	var calls int
	auth := &OAuth2ClientCredentials{
		ClientID:     "id",
		ClientSecret: "secret",
		TokenURL:     "https://token.example.com/token",
		client: &nethttp.Client{Transport: roundTripperFunc(func(r *nethttp.Request) (*nethttp.Response, error) {
			calls++
			return newJSONResponse(200, fmt.Sprintf(`{"access_token":"tok%d","expires_in":1}`, calls)), nil
		})},
	}

	req1, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	if err := auth.Apply(req1); err != nil {
		t.Fatalf("Apply() #1 error = %v", err)
	}
	auth.mu.Lock()
	auth.expiresAt = time.Now().Add(-time.Second)
	auth.mu.Unlock()

	req2, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	if err := auth.Apply(req2); err != nil {
		t.Fatalf("Apply() #2 error = %v", err)
	}

	if calls != 2 {
		t.Fatalf("token endpoint calls = %d, want 2", calls)
	}
	if got := req2.Header.Get("Authorization"); got != "Bearer tok2" {
		t.Fatalf("Authorization = %q, want Bearer tok2", got)
	}
}

func TestOAuth2_TokenServerError(t *testing.T) {
	auth := &OAuth2ClientCredentials{
		ClientID:     "id",
		ClientSecret: "secret",
		TokenURL:     "https://token.example.com/token",
		client: &nethttp.Client{Transport: roundTripperFunc(func(r *nethttp.Request) (*nethttp.Response, error) {
			return newJSONResponse(500, `{"error":"boom"}`), nil
		})},
	}
	req, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
	if err := auth.Apply(req); err == nil {
		t.Fatal("expected error from token server")
	}
}

func TestNewAuthenticator_Unknown(t *testing.T) {
	if _, err := NewAuthenticator(config.AuthConfig{Type: "magic"}); err == nil || !strings.Contains(err.Error(), "valid") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAuthenticator_NoType(t *testing.T) {
	auth, err := NewAuthenticator(config.AuthConfig{})
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}
	if auth != nil {
		t.Fatalf("auth = %#v, want nil", auth)
	}
}

func TestAPIKeyAuth_DefaultInHeaderFromYAML(t *testing.T) {
	cfg := config.AuthConfig{Type: "api_key", Key: "X-API-Key", Value: "secret"}
	auth, err := NewAuthenticator(cfg)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}
	api, ok := auth.(*APIKeyAuth)
	if !ok {
		t.Fatalf("type = %T, want *APIKeyAuth", auth)
	}
	if !api.InHeader {
		t.Fatal("expected InHeader to default to true")
	}
}

func TestOAuth2_ConcurrentApply(t *testing.T) {
	var calls int32
	auth := &OAuth2ClientCredentials{
		ClientID:     "id",
		ClientSecret: "secret",
		TokenURL:     "https://token.example.com/token",
		client: &nethttp.Client{Transport: roundTripperFunc(func(r *nethttp.Request) (*nethttp.Response, error) {
			atomic.AddInt32(&calls, 1)
			return newJSONResponse(200, `{"access_token":"tok1","expires_in":3600}`), nil
		})},
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := nethttp.NewRequest(nethttp.MethodGet, "http://example.com", nil)
			_ = auth.Apply(req)
		}()
	}
	wg.Wait()
	if atomic.LoadInt32(&calls) == 0 {
		t.Fatal("expected token endpoint to be called at least once")
	}
}
