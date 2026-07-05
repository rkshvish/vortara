//go:build integration

package source

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// TestWebhookSource_FullFlow verifies the end-to-end POST -> Ack -> 200 flow.
func TestWebhookSource_FullFlow(t *testing.T) {
	src := NewWebhookSource()
	if err := src.Connect(context.Background(), config.StreamingConfig{
		Path: "/webhooks/full",
		Options: map[string]string{
			"port": ":0",
		},
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	out := make(chan row.Row)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- src.Subscribe(ctx, out) }()

	deadline := time.Now().Add(10 * time.Second)
	var addr string
	for {
		select {
		case err := <-done:
			if err != nil && (strings.Contains(err.Error(), "operation not permitted") || strings.Contains(err.Error(), "bind")) {
				t.Skipf("skipping webhook integration; cannot bind local port: %v", err)
			}
			t.Fatalf("Subscribe() error = %v", err)
		default:
		}
		src.mu.Lock()
		server := src.server
		if server != nil && server.Addr != "" {
			addr = server.Addr
			src.mu.Unlock()
			break
		}
		src.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("webhook server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	src.mu.Lock()
	path := src.cfg.Path
	src.mu.Unlock()
	respCh := make(chan *http.Response, 1)
	go func() {
		resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewBufferString(`{"id":1,"name":"deal"}`))
		if err != nil {
			t.Errorf("POST error = %v", err)
			return
		}
		respCh <- resp
	}()

	r := <-out
	if err := src.Ack(context.Background(), r.ID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	resp := <-respCh
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	cancel()
	_ = <-done
}
