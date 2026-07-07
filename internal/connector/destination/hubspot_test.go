package destination

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// hsTestStore is a minimal in-memory StateStore for HubSpot tests.
type hsTestStore struct {
	state.StateStore
	entities  map[string]*state.EntityState
	delivered map[string]bool
}

func newHSTestStore() *hsTestStore {
	return &hsTestStore{
		entities:  make(map[string]*state.EntityState),
		delivered: make(map[string]bool),
	}
}

func (s *hsTestStore) GetEntityState(_ context.Context, syncName, dest, key string) (*state.EntityState, error) {
	return s.entities[syncName+"/"+dest+"/"+key], nil
}

func (s *hsTestStore) SaveEntityState(_ context.Context, es *state.EntityState) error {
	s.entities[es.SyncName+"/"+es.Destination+"/"+es.EntityKey] = es
	return nil
}

func (s *hsTestStore) IsDelivered(_ context.Context, rowID, _, _ string) (bool, error) {
	return s.delivered[rowID], nil
}

func (s *hsTestStore) MarkDelivered(_ context.Context, rowID, _, _ string) error {
	s.delivered[rowID] = true
	return nil
}

func hsTestRow(id, primaryKey string, data map[string]any) row.Row {
	return row.Row{ID: id, PrimaryKey: primaryKey, Data: data}
}

func connectHS(t *testing.T, baseURL string) *HubSpotDestination {
	t.Helper()
	h := &HubSpotDestination{}
	err := h.Connect(context.Background(), config.DestinationConfig{
		URL: baseURL,
		Auth: config.AuthConfig{
			Type:  "bearer",
			Token: "test-token",
		},
		Options: map[string]string{"object": "contacts"},
		MatchOn: "email",
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return h
}

// TestHubSpot_NoDestinationID_SearchEmpty_Creates verifies the new-contact path:
// no stored destination_id + search returns 0 results → POST create.
func TestHubSpot_NoDestinationID_SearchEmpty_Creates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/search"):
			json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(HubSpotContact{ID: "hs_001"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "alice@example.com", "firstname": "Alice"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load returned fatal error: %v", err)
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded=%d want 1", res.Loaded)
	}
	if res.DestinationIDs["row1"] != "hs_001" {
		t.Errorf("DestinationIDs[row1]=%q want hs_001", res.DestinationIDs["row1"])
	}
}

// TestHubSpot_NoDestinationID_SearchFoundOne_Patches verifies that when search
// finds one existing contact, it PATCHes (no create) and persists the found ID.
func TestHubSpot_NoDestinationID_SearchFoundOne_Patches(t *testing.T) {
	var patchCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/search"):
			json.NewEncoder(w).Encode(map[string]any{
				"results": []any{map[string]any{"id": "hs_existing"}},
			})
		case r.Method == http.MethodPatch:
			patchCalled = true
			json.NewEncoder(w).Encode(HubSpotContact{ID: "hs_existing"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "alice@example.com"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !patchCalled {
		t.Error("expected PATCH call for existing contact, but it was not called")
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded=%d want 1", res.Loaded)
	}
	if res.DestinationIDs["row1"] != "hs_existing" {
		t.Errorf("DestinationIDs[row1]=%q want hs_existing", res.DestinationIDs["row1"])
	}
}

// TestHubSpot_StoredDestinationID_PatchesDirectly verifies that when a
// destination_id is already stored, the connector skips search and PATCHes directly.
func TestHubSpot_StoredDestinationID_PatchesDirectly(t *testing.T) {
	var searchCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/search") {
			searchCalled = true
		}
		if r.Method == http.MethodPatch {
			json.NewEncoder(w).Encode(HubSpotContact{ID: "hs_001"})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	store := newHSTestStore()
	_ = store.SaveEntityState(context.Background(), &state.EntityState{
		SyncName:      "sync",
		Destination:   "hubspot",
		EntityKey:     "lead_001",
		DestinationID: "hs_001",
	})

	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "alice@example.com", "leadScore": "88"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if searchCalled {
		t.Error("search should not be called when destination_id is already stored")
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded=%d want 1", res.Loaded)
	}
	if res.DestinationIDs["row1"] != "hs_001" {
		t.Errorf("DestinationIDs[row1]=%q want hs_001", res.DestinationIDs["row1"])
	}
}

// TestHubSpot_SearchReturnsMultiple_DLQ verifies that >1 search result produces
// a duplicate_match row error (DLQ candidate), not a fatal error.
func TestHubSpot_SearchReturnsMultiple_DLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"results": []any{
				map[string]any{"id": "hs_001"},
				map[string]any{"id": "hs_002"},
			},
		})
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "dupe@example.com"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load returned fatal error: %v", err)
	}
	if res.Loaded != 0 || len(res.Errors) != 1 {
		t.Errorf("Loaded=%d Errors=%d want Loaded=0 Errors=1", res.Loaded, len(res.Errors))
	}
	var hs *hsError
	if !stderrors.As(res.Errors[0].Err, &hs) || hs.class != hsClassDuplicate {
		t.Errorf("expected duplicate_match error, got: %v", res.Errors[0].Err)
	}
}

// TestHubSpot_ValidationError400_DLQ verifies that HTTP 400 (invalid property)
// returns a row error without aborting the run.
func TestHubSpot_ValidationError400_DLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/search") {
			json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"message": "Property 'vortara_score' does not exist"})
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "alice@example.com"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load returned fatal error: %v", err)
	}
	if res.Loaded != 0 || len(res.Errors) != 1 {
		t.Errorf("Loaded=%d Errors=%d want Loaded=0 Errors=1", res.Loaded, len(res.Errors))
	}
	var hs *hsError
	if !stderrors.As(res.Errors[0].Err, &hs) || hs.class != hsClassValidation {
		t.Errorf("expected validation_error, got: %v", res.Errors[0].Err)
	}
}

// TestHubSpot_AuthError401_AbortsRun verifies that HTTP 401 returns a fatal
// top-level error, so the engine aborts the run.
func TestHubSpot_AuthError401_AbortsRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/search") {
			json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"message": "missing token"})
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rows := []row.Row{
		hsTestRow("row1", "lead_001", map[string]any{"email": "a@example.com"}),
		hsTestRow("row2", "lead_002", map[string]any{"email": "b@example.com"}),
	}

	res, err := h.Load(context.Background(), rows, store, "sync", "hubspot")
	if err == nil {
		t.Fatal("expected fatal error for 401, got nil")
	}
	var hs *hsError
	if !stderrors.As(err, &hs) || hs.class != hsClassAuth {
		t.Errorf("expected auth_failed fatal error, got: %v", err)
	}
	if res.Loaded != 0 {
		t.Errorf("Loaded=%d want 0 (run should have aborted)", res.Loaded)
	}
}

// TestHubSpot_RateLimited_Retries verifies that HTTP 429 triggers retry up to
// maxAttempts, succeeding on the third attempt.
func TestHubSpot_RateLimited_Retries(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/search") {
			json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
			return
		}
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(HubSpotContact{ID: fmt.Sprintf("hs_%d", attempts)})
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil // disable rate limiter so test doesn't wait
	// Also shorten backoff by temporarily overriding the client timeout.
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "alice@example.com"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded=%d want 1 (should retry to success)", res.Loaded)
	}
	if attempts < 3 {
		t.Errorf("attempts=%d want >=3", attempts)
	}
}

// TestHubSpot_AlreadyDelivered_Skips verifies that a row already in the
// delivered log does not trigger any HubSpot API call.
func TestHubSpot_AlreadyDelivered_Skips(t *testing.T) {
	var apiCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalled = true
		http.NotFound(w, r)
	}))
	defer srv.Close()

	store := newHSTestStore()
	_ = store.MarkDelivered(context.Background(), "row1", "sync", "hubspot")

	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"email": "alice@example.com"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if apiCalled {
		t.Error("HubSpot API should not be called for an already-delivered row")
	}
	if res.Skipped != 1 {
		t.Errorf("Skipped=%d want 1", res.Skipped)
	}
}

// TestHubSpot_Delete_Archives verifies that a row with Metadata["_action"]="delete"
// and a stored destination_id sends DELETE to HubSpot and succeeds.
func TestHubSpot_Delete_Archives(t *testing.T) {
	var deleteCalled bool
	var deletePath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteCalled = true
			deletePath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	store := newHSTestStore()
	_ = store.SaveEntityState(context.Background(), &state.EntityState{
		SyncName:      "sync",
		Destination:   "hubspot",
		EntityKey:     "lead_001",
		DestinationID: "hs_archive_001",
	})

	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := row.Row{
		ID:         "lead_001",
		PrimaryKey: "lead_001",
		Data:       map[string]any{"email": "alice@example.com"},
		Metadata:   map[string]any{"_action": "delete"},
	}

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !deleteCalled {
		t.Error("expected DELETE request to HubSpot, but it was not called")
	}
	if !strings.Contains(deletePath, "hs_archive_001") {
		t.Errorf("DELETE path %q should contain the stored hs_archive_001 ID", deletePath)
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded=%d want 1", res.Loaded)
	}
}

// TestHubSpot_Delete_NoStoredID_Skips verifies that a delete action with no
// stored destination_id succeeds without any API call (nothing to archive).
func TestHubSpot_Delete_NoStoredID_Skips(t *testing.T) {
	var apiCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalled = true
		http.NotFound(w, r)
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := row.Row{
		ID:         "lead_999",
		PrimaryKey: "lead_999",
		Data:       map[string]any{"email": "ghost@example.com"},
		Metadata:   map[string]any{"_action": "delete"},
	}

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if apiCalled {
		t.Error("HubSpot API should not be called when there is no stored destination_id to archive")
	}
	if res.Loaded != 1 {
		t.Errorf("Loaded=%d want 1", res.Loaded)
	}
}

// TestHubSpot_MissingMatchOnField_DLQ verifies that a row with no email field
// produces a row error without any API call.
func TestHubSpot_MissingMatchOnField_DLQ(t *testing.T) {
	var apiCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalled = true
		http.NotFound(w, r)
	}))
	defer srv.Close()

	store := newHSTestStore()
	h := connectHS(t, srv.URL)
	h.rl = nil
	rw := hsTestRow("row1", "lead_001", map[string]any{"firstname": "No Email"})

	res, err := h.Load(context.Background(), []row.Row{rw}, store, "sync", "hubspot")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if apiCalled {
		t.Error("HubSpot API should not be called when match_on field is missing")
	}
	if len(res.Errors) != 1 {
		t.Errorf("Errors=%d want 1", len(res.Errors))
	}
}
