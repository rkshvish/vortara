package destination

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestTransportsNegotiateHTTP2 verifies the connector transport shape
// (pool limits + ForceAttemptHTTP2) actually negotiates HTTP/2 — Salesforce
// and HubSpot both support h2, halving connection overhead under the
// parallel batch dispatch.
func TestTransportsNegotiateHTTP2(t *testing.T) {
	var proto string
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto = r.Proto
		w.WriteHeader(http.StatusOK)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	transport := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        6,
		MaxIdleConnsPerHost: 3,
		MaxConnsPerHost:     3,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     &tls.Config{RootCAs: srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs},
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if proto != "HTTP/2.0" {
		t.Fatalf("negotiated %q, want HTTP/2.0 — a custom TLS config without ForceAttemptHTTP2 silently downgrades", proto)
	}
}
