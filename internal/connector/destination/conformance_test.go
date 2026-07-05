package destination

// Destination conformance suite: every destination must satisfy the same
// delivery invariants regardless of transport. HTTP-based destinations run
// here against mock backends; postgres/snowflake are covered by their
// integration tests (build tag "integration").

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// conformanceTarget is one destination wired to a controllable mock backend.
type conformanceTarget struct {
	dest    Destination
	setFail func(bool) // make the backend reject writes
	calls   func() int // backend write-call count
}

// conformanceRow builds a row whose data satisfies every destination's
// match-on requirements (ExternalId__c for salesforce, email for hubspot).
func conformanceRow(id string) row.Row {
	return row.Row{
		ID:         id,
		PrimaryKey: "id=" + id,
		Data: map[string]interface{}{
			"ExternalId__c": id,
			"email":         id + "@example.com",
			"name":          "row " + id,
		},
		ExtractedAt: time.Now(),
	}
}

func fastRetry() config.RetryConfig {
	return config.RetryConfig{Attempts: 1, BackoffMs: 1}
}

// mockBackend is a toggleable HTTP server that counts write requests.
type mockBackend struct {
	fail  atomic.Bool
	count atomic.Int64
	srv   *httptest.Server
}

func newMockBackend(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *mockBackend {
	t.Helper()
	b := &mockBackend{}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.count.Add(1)
		if b.fail.Load() {
			http.Error(w, `{"error":"backend down"}`, http.StatusInternalServerError)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(b.srv.Close)
	return b
}

func (b *mockBackend) target(dest Destination) conformanceTarget {
	return conformanceTarget{
		dest:    dest,
		setFail: func(v bool) { b.fail.Store(v) },
		calls:   func() int { return int(b.count.Load()) },
	}
}

var conformanceTargets = map[string]func(t *testing.T) conformanceTarget{
	"restapi": func(t *testing.T) conformanceTarget {
		b := newMockBackend(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		d := NewRESTAPIDestination()
		if err := d.Connect(context.Background(), config.DestinationConfig{
			URL: b.srv.URL, Method: http.MethodPost, Retry: fastRetry(),
		}); err != nil {
			t.Fatalf("restapi Connect: %v", err)
		}
		return b.target(d)
	},
	"slack": func(t *testing.T) conformanceTarget {
		b := newMockBackend(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		d := NewSlackDestination()
		if err := d.Connect(context.Background(), config.DestinationConfig{
			Options: map[string]string{"webhook": b.srv.URL, "message": "hi {{ row.name }}"},
		}); err != nil {
			t.Fatalf("slack Connect: %v", err)
		}
		return b.target(d)
	},
	"hubspot": func(t *testing.T) conformanceTarget {
		b := newMockBackend(t, func(w http.ResponseWriter, r *http.Request) {
			var payload struct {
				Inputs []struct {
					ID string `json:"id"`
				} `json:"inputs"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			results := make([]map[string]string, 0, len(payload.Inputs))
			for _, in := range payload.Inputs {
				results = append(results, map[string]string{"id": in.ID})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
		})
		d := NewHubSpotDestination()
		if err := d.Connect(context.Background(), config.DestinationConfig{
			URL: b.srv.URL, MatchOn: "email",
			Options: map[string]string{"object": "contacts"},
			Auth:    config.AuthConfig{Type: "bearer", Token: "t"},
			Retry:   fastRetry(),
		}); err != nil {
			t.Fatalf("hubspot Connect: %v", err)
		}
		return b.target(d)
	},
	"salesforce": func(t *testing.T) conformanceTarget {
		b := newMockBackend(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"001","success":true,"errors":[]}`))
		})
		d := NewSalesforceDestination()
		if err := d.Connect(context.Background(), config.DestinationConfig{
			URL: b.srv.URL, MatchOn: "ExternalId__c",
			Options: map[string]string{"object": "Opportunity"},
			Auth:    config.AuthConfig{Type: "bearer", Token: "t"},
			Retry:   fastRetry(),
		}); err != nil {
			t.Fatalf("salesforce Connect: %v", err)
		}
		return b.target(d)
	},
	"googlesheets": func(t *testing.T) conformanceTarget {
		fake := &fakeSheets{}
		withFakeSheets(t, fake)
		d := NewGoogleSheetsDestination()
		if err := d.Connect(context.Background(), config.DestinationConfig{
			Options: map[string]string{"spreadsheet_id": "s1"},
		}); err != nil {
			t.Fatalf("googlesheets Connect: %v", err)
		}
		return conformanceTarget{
			dest: d,
			setFail: func(v bool) {
				if v {
					fake.err = fmt.Errorf("backend down")
				} else {
					fake.err = nil
				}
			},
			calls: func() int { return fake.calls },
		}
	},
}

var conformanceFactories = map[string]func() Destination{
	"restapi":      func() Destination { return NewRESTAPIDestination() },
	"slack":        func() Destination { return NewSlackDestination() },
	"hubspot":      func() Destination { return NewHubSpotDestination() },
	"salesforce":   func() Destination { return NewSalesforceDestination() },
	"googlesheets": func() Destination { return NewGoogleSheetsDestination() },
	"postgres":     func() Destination { return NewPostgresDestination() },
	"snowflake":    func() Destination { return NewSnowflakeDestination() },
}

func TestConformance_EmptyRowsIsNoOp(t *testing.T) {
	for name, mk := range conformanceTargets {
		t.Run(name, func(t *testing.T) {
			tgt := mk(t)
			store := state.NewMemoryStore()
			res, err := tgt.dest.Load(context.Background(), nil, store, "p", name)
			if err != nil {
				t.Fatalf("Load(nil) error = %v", err)
			}
			if res.Loaded != 0 || res.Skipped != 0 || len(res.Errors) != 0 {
				t.Fatalf("Load(nil) result = %+v, want zero", res)
			}
			if tgt.calls() != 0 {
				t.Fatalf("Load(nil) made %d backend calls, want 0", tgt.calls())
			}
		})
	}
}

func TestConformance_IdempotentReload(t *testing.T) {
	for name, mk := range conformanceTargets {
		t.Run(name, func(t *testing.T) {
			tgt := mk(t)
			store := state.NewMemoryStore()
			rows := []row.Row{conformanceRow("c1"), conformanceRow("c2")}

			res, err := tgt.dest.Load(context.Background(), rows, store, "p", name)
			if err != nil || res.Loaded != 2 {
				t.Fatalf("first Load = %+v err %v, want 2 loaded", res, err)
			}
			callsAfterFirst := tgt.calls()

			res, err = tgt.dest.Load(context.Background(), rows, store, "p", name)
			if err != nil {
				t.Fatalf("second Load error = %v", err)
			}
			if res.Skipped != 2 || res.Loaded != 0 {
				t.Fatalf("second Load = %+v, want 2 skipped 0 loaded", res)
			}
			if tgt.calls() != callsAfterFirst {
				t.Fatalf("second Load made %d new backend calls, want 0", tgt.calls()-callsAfterFirst)
			}
		})
	}
}

func TestConformance_FailedRowsNotMarkedDelivered(t *testing.T) {
	for name, mk := range conformanceTargets {
		t.Run(name, func(t *testing.T) {
			tgt := mk(t)
			store := state.NewMemoryStore()
			rows := []row.Row{conformanceRow("f1")}

			tgt.setFail(true)
			res, err := tgt.dest.Load(context.Background(), rows, store, "p", name)
			if err == nil && len(res.Errors) == 0 {
				t.Fatalf("failing Load = %+v err %v, want an error surface", res, err)
			}
			if res.Loaded != 0 {
				t.Fatalf("failing Load reported %d loaded, want 0", res.Loaded)
			}
			delivered, _ := store.IsDelivered(rows[0].ID, "p", name)
			if delivered {
				t.Fatal("failed row was marked delivered")
			}

			// Recovery: the same row must actually deliver, not be skipped.
			tgt.setFail(false)
			res, err = tgt.dest.Load(context.Background(), rows, store, "p", name)
			if err != nil || res.Loaded != 1 || res.Skipped != 0 {
				t.Fatalf("recovery Load = %+v err %v, want 1 loaded 0 skipped", res, err)
			}
		})
	}
}

func TestConformance_LoadBeforeConnectDoesNotPanic(t *testing.T) {
	for name, mk := range conformanceFactories {
		t.Run(name, func(t *testing.T) {
			d := mk()
			store := state.NewMemoryStore()
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Load before Connect panicked: %v", r)
				}
			}()
			res, err := d.Load(context.Background(), []row.Row{conformanceRow("x")}, store, "p", name)
			if err == nil && len(res.Errors) == 0 {
				t.Fatalf("Load before Connect = %+v, want error or row errors", res)
			}
			if res.Loaded != 0 {
				t.Fatalf("Load before Connect loaded %d rows", res.Loaded)
			}
		})
	}
}
