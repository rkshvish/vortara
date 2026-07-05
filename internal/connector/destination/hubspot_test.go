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

	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func newHubSpotDestinationForTest(t *testing.T, baseURL string) *HubSpotDestination {
	t.Helper()
	dst := NewHubSpotDestination()
	cfg := config.DestinationConfig{
		URL:     baseURL,
		MatchOn: "email",
		Options: map[string]string{"object": "contacts"},
		Auth: config.AuthConfig{
			Type:  "bearer",
			Token: "hubspot-token",
		},
	}
	if err := dst.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return dst
}

func hubspotRows(n int) []row.Row {
	rows := make([]row.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = row.Row{
			ID: fmt.Sprintf("row-%d", i),
			Data: map[string]any{
				"email":     fmt.Sprintf("user-%d@example.com", i),
				"firstname": fmt.Sprintf("First%d", i),
				"revenue":   50000 + i,
			},
		}
	}
	return rows
}

func TestHubSpotDestination_Connect_MissingObject(t *testing.T) {
	dst := NewHubSpotDestination()
	err := dst.Connect(context.Background(), config.DestinationConfig{
		MatchOn: "email",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHubSpotDestination_Connect_MissingMatchOn(t *testing.T) {
	dst := NewHubSpotDestination()
	err := dst.Connect(context.Background(), config.DestinationConfig{
		Options: map[string]string{"object": "contacts"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHubSpotDestination_Load_Success(t *testing.T) {
	var body struct {
		Inputs []struct {
			IDProperty string            `json:"idProperty"`
			ID         string            `json:"id"`
			Properties map[string]string `json:"properties"`
		} `json:"inputs"`
	}

	dst := newHubSpotDestinationForTest(t, "https://api.hubapi.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/crm/v3/objects/contacts/batch/upsert" {
			return newResponse(http.StatusNotFound, ""), nil
		}
		if got := r.Header.Get("Authorization"); got != "Bearer hubspot-token" {
			t.Fatalf("unexpected auth header %q", got)
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		return newResponse(http.StatusOK, `{"status":"COMPLETE"}`), nil
	})

	store := state.NewMemoryStore()
	rows := hubspotRows(3)
	res, err := dst.Load(context.Background(), rows, store, "pipeline", "hubspot")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 3 || res.Skipped != 0 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(body.Inputs) != 3 {
		t.Fatalf("expected 3 inputs, got %d", len(body.Inputs))
	}
	if body.Inputs[0].IDProperty != "email" || body.Inputs[0].ID != "user-0@example.com" {
		t.Fatalf("unexpected first input: %+v", body.Inputs[0])
	}
	if _, ok := body.Inputs[0].Properties["email"]; ok {
		t.Fatalf("matchOn field should be excluded from properties: %+v", body.Inputs[0].Properties)
	}
	if body.Inputs[0].Properties["revenue"] != "50000" {
		t.Fatalf("expected stringified revenue, got %q", body.Inputs[0].Properties["revenue"])
	}
}

func TestHubSpotDestination_Load_AlreadyDelivered(t *testing.T) {
	ctx := context.Background()
	var body struct {
		Inputs []struct {
			ID string `json:"id"`
		} `json:"inputs"`
	}

	dst := newHubSpotDestinationForTest(t, "https://api.hubapi.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		return newResponse(http.StatusOK, `{"status":"COMPLETE"}`), nil
	})

	store := state.NewMemoryStore()
	rows := hubspotRows(3)
	if err := store.MarkDelivered(ctx, rows[1].ID, "pipeline", "hubspot"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}

	res, err := dst.Load(context.Background(), rows, store, "pipeline", "hubspot")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 2 || res.Skipped != 1 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(body.Inputs) != 2 {
		t.Fatalf("expected 2 inputs, got %d", len(body.Inputs))
	}
}

func TestHubSpotDestination_Load_Batch100(t *testing.T) {
	var calls int32
	var batchSizes []int
	var mu sync.Mutex

	dst := newHubSpotDestinationForTest(t, "https://api.hubapi.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		var body struct {
			Inputs []map[string]any `json:"inputs"`
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		mu.Lock()
		batchSizes = append(batchSizes, len(body.Inputs))
		mu.Unlock()
		return newResponse(http.StatusOK, `{"status":"COMPLETE"}`), nil
	})

	res, err := dst.Load(context.Background(), hubspotRows(150), state.NewMemoryStore(), "pipeline", "hubspot")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 150 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	if len(batchSizes) != 2 || batchSizes[0] != 100 || batchSizes[1] != 50 {
		t.Fatalf("unexpected batch sizes: %v", batchSizes)
	}
}

func TestHubSpotDestination_Load_207PartialSuccess(t *testing.T) {
	dst := newHubSpotDestinationForTest(t, "https://api.hubapi.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusMultiStatus, `{
			"status":"COMPLETE",
			"errors":[
				{"id":"user-1@example.com","message":"duplicate property value"}
			]
		}`), nil
	})

	res, err := dst.Load(context.Background(), hubspotRows(3), state.NewMemoryStore(), "pipeline", "hubspot")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 2 || len(res.Errors) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if !strings.Contains(res.Errors[0].Err.Error(), "duplicate property value") {
		t.Fatalf("unexpected error: %v", res.Errors[0].Err)
	}
}

func TestHubSpotDestination_Load_429Retry(t *testing.T) {
	var calls int32
	dst := newHubSpotDestinationForTest(t, "https://api.hubapi.com")
	dst.cfg.Retry = config.RetryConfig{Attempts: 2, BackoffMs: 1, BackoffOn: []int{429}}
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return newResponse(http.StatusTooManyRequests, ""), nil
		}
		return newResponse(http.StatusOK, `{"status":"COMPLETE"}`), nil
	})

	res, err := dst.Load(context.Background(), hubspotRows(2), state.NewMemoryStore(), "pipeline", "hubspot")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 2 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestHubSpotDestination_DefaultBaseURL(t *testing.T) {
	dst := newHubSpotDestinationForTest(t, "")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		if got := r.URL.String(); !strings.HasPrefix(got, "https://api.hubapi.com/crm/v3/objects/contacts/batch/upsert") {
			t.Fatalf("unexpected url %q", got)
		}
		return newResponse(http.StatusOK, `{"status":"COMPLETE"}`), nil
	})

	res, err := dst.Load(context.Background(), hubspotRows(1), state.NewMemoryStore(), "pipeline", "hubspot")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
}
