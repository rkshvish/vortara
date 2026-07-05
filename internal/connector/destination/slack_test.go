package destination

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

func TestRenderMessage(t *testing.T) {
	tests := []struct {
		name     string
		template string
		data     map[string]interface{}
		want     string
	}{
		{
			name:     "single field",
			template: "Deal won: {{ row.name }}",
			data:     map[string]interface{}{"name": "Acme"},
			want:     "Deal won: Acme",
		},
		{
			name:     "multiple fields",
			template: "{{ row.name }} — ${{ row.amount }}",
			data:     map[string]interface{}{"name": "Acme", "amount": 5000},
			want:     "Acme — $5000",
		},
		{
			name:     "missing field renders empty",
			template: "Hello {{ row.missing }}!",
			data:     map[string]interface{}{},
			want:     "Hello !",
		},
		{
			name:     "no placeholders",
			template: "static message",
			data:     map[string]interface{}{"x": 1},
			want:     "static message",
		},
		{
			name:     "tight braces",
			template: "{{row.name}}",
			data:     map[string]interface{}{"name": "tight"},
			want:     "tight",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := renderMessage(tt.template, tt.data); got != tt.want {
				t.Fatalf("renderMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSlackDestination_Connect_Validation(t *testing.T) {
	d := NewSlackDestination()
	err := d.Connect(context.Background(), config.DestinationConfig{Options: map[string]string{}})
	if err == nil {
		t.Fatal("Connect() with no webhook should fail")
	}

	err = d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"webhook": "https://hooks.slack.com/services/x"},
	})
	if err == nil {
		t.Fatal("Connect() with no message template should fail")
	}

	err = d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{
			"webhook": "https://hooks.slack.com/services/x",
			"message": "hi {{ row.name }}",
		},
	})
	if err != nil {
		t.Fatalf("Connect() error = %v, want nil", err)
	}
}

func TestSlackDestination_Load(t *testing.T) {
	var mu sync.Mutex
	var messages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		mu.Lock()
		messages = append(messages, payload["text"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewSlackDestination()
	err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{
			"webhook": srv.URL,
			"message": "Deal: {{ row.name }} ({{ row.status }})",
		},
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("src", "pipe", "pk1", map[string]interface{}{"name": "Acme", "status": "won"}, time.Now()),
		row.NewRow("src", "pipe", "pk2", map[string]interface{}{"name": "Beta", "status": "lost"}, time.Now()),
	}

	result, err := d.Load(context.Background(), rows, store, "pipe", "slack")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if result.Loaded != 2 {
		t.Fatalf("Loaded = %d, want 2", result.Loaded)
	}
	mu.Lock()
	got := append([]string(nil), messages...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "Deal: Acme (won)" || got[1] != "Deal: Beta (lost)" {
		t.Fatalf("messages = %v", got)
	}

	// Second load of the same rows should skip both (idempotency).
	result, err = d.Load(context.Background(), rows, store, "pipe", "slack")
	if err != nil {
		t.Fatalf("Load() second call error = %v", err)
	}
	if result.Skipped != 2 || result.Loaded != 0 {
		t.Fatalf("second load = %+v, want 2 skipped", result)
	}
}

func TestSlackDestination_Load_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid_payload", http.StatusBadRequest)
	}))
	defer srv.Close()

	d := NewSlackDestination()
	err := d.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"webhook": srv.URL, "message": "x"},
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("src", "pipe", "pk1", map[string]interface{}{"name": "Acme"}, time.Now()),
	}
	result, err := d.Load(context.Background(), rows, store, "pipe", "slack")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(result.Errors) != 1 || result.Loaded != 0 {
		t.Fatalf("result = %+v, want 1 row error", result)
	}
}
