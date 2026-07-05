package destination

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

func newSalesforceTestDestination(t *testing.T, serverURL string) *SalesforceDestination {
	t.Helper()
	dst := NewSalesforceDestination()
	cfg := config.DestinationConfig{
		URL:     serverURL,
		MatchOn: "ExternalId__c",
		Options: map[string]string{"object": "Opportunity"},
		Auth: config.AuthConfig{
			Type:  "bearer",
			Token: "token-1",
		},
	}
	if err := dst.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return dst
}

func salesforceRows(n int) []row.Row {
	rows := make([]row.Row, n)
	for i := range rows {
		rows[i] = row.Row{
			ID: fmt.Sprintf("row-%d", i),
			Data: map[string]any{
				"ExternalId__c": fmt.Sprintf("ext-%d", i),
				"Name":          fmt.Sprintf("Deal %d", i),
				"Amount":        i,
			},
		}
	}
	return rows
}

func newSFResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestSalesforceDestination_Connect_MissingObject(t *testing.T) {
	dst := NewSalesforceDestination()
	err := dst.Connect(context.Background(), config.DestinationConfig{
		URL:     "http://example.com",
		MatchOn: "ExternalId__c",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSalesforceDestination_Connect_MissingMatchOn(t *testing.T) {
	dst := NewSalesforceDestination()
	err := dst.Connect(context.Background(), config.DestinationConfig{
		URL:     "http://example.com",
		Options: map[string]string{"object": "Opportunity"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSalesforceDestination_TransportConfigured(t *testing.T) {
	dst := NewSalesforceDestination()
	if err := dst.Connect(context.Background(), config.DestinationConfig{
		URL:              "http://example.com",
		MatchOn:          "ExternalId__c",
		Options:          map[string]string{"object": "Opportunity"},
		WriteParallelism: 5,
	}); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	transport, ok := dst.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", dst.client.Transport)
	}
	if transport.MaxConnsPerHost != 5 {
		t.Fatalf("MaxConnsPerHost = %d, want 5", transport.MaxConnsPerHost)
	}
}

func TestSalesforceDestination_Load_RESTUpsert(t *testing.T) {
	var mu sync.Mutex
	var patchCount int
	var bodyBytes []byte
	dst := newSalesforceTestDestination(t, "http://example.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, "/services/data/v58.0/sobjects/Opportunity/ExternalId__c/"):
			mu.Lock()
			patchCount++
			bodyBytes, _ = io.ReadAll(r.Body)
			mu.Unlock()
			if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
				return nil, fmt.Errorf("unexpected auth header %q", got)
			}
			return newResponse(http.StatusNoContent, ""), nil
		default:
			return newResponse(http.StatusNotFound, ""), nil
		}
	})
	store := state.NewMemoryStore()
	rw := row.Row{ID: "row-1", Data: map[string]interface{}{"ExternalId__c": "ext-1", "Name": "Deal A"}}

	res, err := dst.Load(context.Background(), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 || res.Skipped != 0 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	mu.Lock()
	defer mu.Unlock()
	if patchCount != 1 {
		t.Fatalf("patchCount = %d, want 1", patchCount)
	}
	var payload map[string]any
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if _, ok := payload["ExternalId__c"]; ok {
		t.Fatalf("matchOn field should be excluded from body: %+v", payload)
	}
	if payload["Name"] != "Deal A" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestSalesforceDestination_Load_AlreadyDelivered(t *testing.T) {
	var calls int
	dst := newSalesforceTestDestination(t, "http://example.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		calls++
		return nil, fmt.Errorf("unexpected HTTP call: %s %s", r.Method, r.URL.Path)
	})
	store := state.NewMemoryStore()
	if err := store.MarkDelivered("row-1", "pipeline", "dest"); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}
	rw := row.Row{ID: "row-1", Data: map[string]interface{}{"ExternalId__c": "ext-1", "Name": "Deal A"}}

	res, err := dst.Load(context.Background(), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Skipped != 1 || res.Loaded != 0 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if calls != 0 {
		t.Fatalf("expected no calls, got %d", calls)
	}
}

func TestSalesforceDestination_Load_429Retry(t *testing.T) {
	var mu sync.Mutex
	var calls int
	dst := newSalesforceTestDestination(t, "http://example.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPatch:
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n < 3 {
				return newResponse(http.StatusTooManyRequests, ""), nil
			}
			return newResponse(http.StatusNoContent, ""), nil
		default:
			return newResponse(http.StatusNotFound, ""), nil
		}
	})
	dst.cfg.Retry = config.RetryConfig{Attempts: 3, BackoffMs: 1, BackoffOn: []int{429}}
	store := state.NewMemoryStore()
	rw := row.Row{ID: "row-1", Data: map[string]interface{}{"ExternalId__c": "ext-1", "Name": "Deal A"}}

	res, err := dst.Load(context.Background(), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestSalesforceDestination_Load_BulkAPI(t *testing.T) {
	var mu sync.Mutex
	var jobCreated map[string]any
	var uploadCSV []byte
	var statusCalls int
	rows := make([]row.Row, 300)
	for i := range rows {
		rows[i] = row.Row{
			ID: fmt.Sprintf("row-%d", i),
			Data: map[string]any{
				"ExternalId__c": fmt.Sprintf("ext-%d", i),
				"Name":          fmt.Sprintf("Deal %d", i),
				"Amount":        i,
			},
		}
	}

	dst := newSalesforceTestDestination(t, "http://example.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/services/data/v58.0/jobs/ingest":
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&jobCreated)
			mu.Unlock()
			payload, _ := json.Marshal(map[string]any{"id": "job123", "state": "Open"})
			return newResponse(http.StatusOK, string(payload)), nil
		case r.Method == http.MethodPut && r.URL.Path == "/services/data/v58.0/jobs/ingest/job123/batches":
			mu.Lock()
			uploadCSV, _ = io.ReadAll(r.Body)
			mu.Unlock()
			return newResponse(http.StatusNoContent, ""), nil
		case r.Method == http.MethodPatch && r.URL.Path == "/services/data/v58.0/jobs/ingest/job123":
			return newResponse(http.StatusNoContent, ""), nil
		case r.Method == http.MethodGet && r.URL.Path == "/services/data/v58.0/jobs/ingest/job123":
			payload, _ := json.Marshal(map[string]any{"state": "JobComplete"})
			return newResponse(http.StatusOK, string(payload)), nil
		case r.Method == http.MethodGet && r.URL.Path == "/services/data/v58.0/jobs/ingest/job123/successfulResults":
			mu.Lock()
			statusCalls++
			mu.Unlock()
			var buf bytes.Buffer
			for i := 0; i < len(rows); i++ {
				buf.WriteString("success\n")
			}
			return newResponse(http.StatusOK, buf.String()), nil
		case r.Method == http.MethodGet && r.URL.Path == "/services/data/v58.0/jobs/ingest/job123/failedResults":
			return newResponse(http.StatusOK, ""), nil
		default:
			return newResponse(http.StatusNotFound, ""), nil
		}
	})
	dst.cfg.Options["bulk_threshold"] = "200"
	store := state.NewMemoryStore()

	res, err := dst.Load(context.Background(), rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 300 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if jobCreated["object"] != "Opportunity" {
		t.Fatalf("unexpected job payload: %+v", jobCreated)
	}
	if jobCreated["externalIdFieldName"] != "ExternalId__c" {
		t.Fatalf("unexpected job payload: %+v", jobCreated)
	}
	reader := csv.NewReader(bytes.NewReader(uploadCSV))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(records) != 301 {
		t.Fatalf("expected header + 300 rows, got %d", len(records))
	}
	if len(records[0]) != 3 || records[0][0] != "Amount" || records[0][1] != "ExternalId__c" || records[0][2] != "Name" {
		t.Fatalf("unexpected csv header: %v", records[0])
	}
	if statusCalls == 0 {
		t.Fatal("expected successfulResults to be queried")
	}
}

func TestSalesforceDestination_ParallelBatches(t *testing.T) {
	var inflight int32
	var maxInflight int32
	var jobCounter int32
	var mu sync.Mutex
	jobRows := make(map[string]int)

	dst := newSalesforceTestDestination(t, "http://example.com")
	dst.client = newMockClient(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/services/data/v58.0/jobs/ingest":
			cur := atomic.AddInt32(&inflight, 1)
			for {
				max := atomic.LoadInt32(&maxInflight)
				if cur <= max || atomic.CompareAndSwapInt32(&maxInflight, max, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&inflight, -1)
			id := fmt.Sprintf("job-%d", atomic.AddInt32(&jobCounter, 1))
			payload, _ := json.Marshal(map[string]any{"id": id, "state": "Open"})
			return newSFResponse(http.StatusOK, string(payload)), nil
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/services/data/v58.0/jobs/ingest/job-") && strings.HasSuffix(r.URL.Path, "/batches"):
			jobID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/services/data/v58.0/jobs/ingest/"), "/batches")
			records, err := csv.NewReader(r.Body).ReadAll()
			if err != nil {
				return nil, err
			}
			mu.Lock()
			jobRows[jobID] = len(records) - 1
			mu.Unlock()
			return newSFResponse(http.StatusNoContent, ""), nil
		case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, "/services/data/v58.0/jobs/ingest/job-"):
			return newSFResponse(http.StatusNoContent, ""), nil
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/services/data/v58.0/jobs/ingest/job-") && !strings.Contains(r.URL.Path, "/successfulResults") && !strings.Contains(r.URL.Path, "/failedResults"):
			payload, _ := json.Marshal(map[string]any{"state": "JobComplete"})
			return newSFResponse(http.StatusOK, string(payload)), nil
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/successfulResults"):
			jobID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/services/data/v58.0/jobs/ingest/"), "/successfulResults")
			mu.Lock()
			rows := jobRows[jobID]
			mu.Unlock()
			var buf bytes.Buffer
			for i := 0; i < rows; i++ {
				buf.WriteString("success\n")
			}
			return newSFResponse(http.StatusOK, buf.String()), nil
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/failedResults"):
			return newSFResponse(http.StatusOK, ""), nil
		default:
			return newSFResponse(http.StatusNotFound, ""), nil
		}
	})
	dst.cfg.Options["bulk_threshold"] = "200"
	dst.cfg.WriteParallelism = 3
	store := state.NewMemoryStore()

	res, err := dst.Load(context.Background(), salesforceRows(600), store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 600 || len(res.Errors) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if atomic.LoadInt32(&jobCounter) != 3 {
		t.Fatalf("expected 3 bulk jobs, got %d", jobCounter)
	}
	if atomic.LoadInt32(&maxInflight) != 3 {
		t.Fatalf("maxInflight = %d, want 3", maxInflight)
	}
}

func TestSalesforceDestination_RowsToCSV(t *testing.T) {
	rows := []row.Row{
		{ID: "1", Data: map[string]any{"b": "two", "a": 1, "c": nil}},
		{ID: "2", Data: map[string]any{"b": "four", "a": 3, "c": "x"}},
	}
	out, headers := rowsToCSV(rows)
	if strings.Join(headers, ",") != "a,b,c" {
		t.Fatalf("unexpected headers: %v", headers)
	}
	reader := csv.NewReader(bytes.NewReader(out))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if got := strings.Join(records[1], ","); got != "1,two," {
		t.Fatalf("unexpected row: %q", got)
	}
	if got := strings.Join(records[2], ","); got != "3,four,x" {
		t.Fatalf("unexpected row: %q", got)
	}
}
