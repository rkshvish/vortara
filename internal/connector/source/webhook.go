package source

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// WebhookSource implements StreamingSource as an HTTP webhook receiver.
type WebhookSource struct {
	cfg     config.StreamingConfig
	server  *http.Server
	pending map[string]chan error
	mu      sync.Mutex
}

var _ StreamingSource = (*WebhookSource)(nil)

func init() {
	registry.RegisterStreamingSource("webhook", func() any {
		return NewWebhookSource()
	})
}

// NewWebhookSource returns a new WebhookSource.
func NewWebhookSource() *WebhookSource {
	return &WebhookSource{
		pending: make(map[string]chan error),
	}
}

// Connect validates the configured webhook path and stores the config.
func (w *WebhookSource) Connect(ctx context.Context, cfg config.StreamingConfig) error {
	if strings.TrimSpace(cfg.Path) == "" {
		return errors.New("webhook source: path is required")
	}
	w.cfg = cfg
	return nil
}

// Subscribe starts the webhook HTTP server and blocks until ctx is cancelled.
func (w *WebhookSource) Subscribe(ctx context.Context, out chan<- row.Row) error {
	addr := w.listenAddr()
	mux := http.NewServeMux()
	mux.HandleFunc(w.cfg.Path, w.handleWebhook(out))

	srv := &http.Server{Addr: addr, Handler: mux}
	w.mu.Lock()
	w.server = srv
	w.mu.Unlock()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		w.mu.Lock()
		w.server = nil
		w.mu.Unlock()
		return err
	}
	w.mu.Lock()
	w.server.Addr = ln.Addr().String()
	w.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return ctx.Err()
		}
		return err
	}
}

// Ack signals that a row was delivered successfully and allows the handler to respond 200.
func (w *WebhookSource) Ack(ctx context.Context, rowID string) error {
	return w.signal(rowID, nil)
}

// Nack signals that processing failed and allows the handler to respond 500.
func (w *WebhookSource) Nack(ctx context.Context, rowID string) error {
	return w.signal(rowID, errors.New("delivery failed"))
}

// Close shuts down the webhook HTTP server gracefully.
func (w *WebhookSource) Close() error {
	w.mu.Lock()
	server := w.server
	w.mu.Unlock()
	if server == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

func (w *WebhookSource) listenAddr() string {
	if w.cfg.Options != nil {
		if port := strings.TrimSpace(w.cfg.Options["port"]); port != "" {
			if strings.HasPrefix(port, ":") {
				return "127.0.0.1" + port
			}
			return "127.0.0.1:" + port
		}
	}
	return "127.0.0.1:8080"
}

func (w *WebhookSource) handleWebhook(out chan<- row.Row) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			http.Error(rw, "bad request", http.StatusBadRequest)
			return
		}

		if strings.TrimSpace(w.cfg.Secret) != "" {
			if !validWebhookSignature(w.cfg.Secret, req.Header.Get("X-Vortara-Signature"), body) {
				http.Error(rw, "unauthorized", http.StatusUnauthorized)
				return
			}
		}

		payload, err := parseWebhookPayload(body)
		if err != nil {
			http.Error(rw, "bad request", http.StatusBadRequest)
			return
		}

		result := row.NewRow(
			w.sourceName(),
			w.pipelineName(),
			webhookPrimaryKey(payload),
			payload,
			time.Now().UTC(),
		)

		ackCh := make(chan error, 1)
		w.mu.Lock()
		w.pending[result.ID] = ackCh
		w.mu.Unlock()

		select {
		case out <- result:
		case <-req.Context().Done():
			w.removePending(result.ID)
			rw.WriteHeader(499)
			return
		}

		timeout := w.ackTimeout()
		select {
		case err := <-ackCh:
			w.removePending(result.ID)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusOK)
		case <-time.After(timeout):
			w.removePending(result.ID)
			http.Error(rw, "timeout", http.StatusGatewayTimeout)
		case <-req.Context().Done():
			w.removePending(result.ID)
			rw.WriteHeader(499)
		}
	}
}

func (w *WebhookSource) signal(rowID string, err error) error {
	w.mu.Lock()
	ackCh, ok := w.pending[rowID]
	if !ok {
		w.mu.Unlock()
		return fmt.Errorf("webhook source: unknown rowID %q", rowID)
	}
	delete(w.pending, rowID)
	w.mu.Unlock()

	select {
	case ackCh <- err:
		return nil
	default:
		return nil
	}
}

func (w *WebhookSource) removePending(rowID string) {
	w.mu.Lock()
	delete(w.pending, rowID)
	w.mu.Unlock()
}

func (w *WebhookSource) sourceName() string {
	return "webhook." + w.cfg.Path
}

func (w *WebhookSource) pipelineName() string {
	if w.cfg.Options != nil {
		if v := strings.TrimSpace(w.cfg.Options["pipeline"]); v != "" {
			return v
		}
	}
	return ""
}

func (w *WebhookSource) ackTimeout() time.Duration {
	if w.cfg.Options == nil {
		return 30 * time.Second
	}
	if raw := strings.TrimSpace(w.cfg.Options["ack_timeout_ms"]); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 30 * time.Second
}

func validWebhookSignature(secret, header string, body []byte) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	expected := hmac.New(sha256.New, []byte(secret))
	_, _ = expected.Write(body)
	sum := expected.Sum(nil)

	got, err := hex.DecodeString(strings.TrimPrefix(header, prefix))
	if err != nil {
		return false
	}
	return hmac.Equal(sum, got)
}

func parseWebhookPayload(body []byte) (map[string]interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var payload map[string]interface{}
	if err := dec.Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func webhookPrimaryKey(payload map[string]interface{}) string {
	if v, ok := payload["id"]; ok {
		return "id=" + formatJSONValue(v)
	}
	return uuid.NewString()
}
