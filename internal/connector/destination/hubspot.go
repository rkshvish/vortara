package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	httpauth "github.com/rkshvish/vortara/internal/connector/http"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/state"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

const hsDefaultBaseURL = "https://api.hubapi.com"

// hsClass classifies a HubSpot API error so callers can decide
// whether to DLQ, retry, or abort the run.
type hsClass string

const (
	hsClassAuth       hsClass = "auth_failed"      // 401/403 — abort run
	hsClassValidation hsClass = "validation_error" // 400/422 — terminal, DLQ
	hsClassConflict   hsClass = "conflict"         // 409     — DLQ
	hsClassDuplicate  hsClass = "duplicate_match"  // search returned >1 contact
	hsClassRetryable  hsClass = "retryable"        // 429/5xx — retry then DLQ
	hsClassAmbiguous  hsClass = "ambiguous"        // timeout — do not mark success
)

type hsError struct {
	class   hsClass
	status  int
	message string
}

func (e *hsError) Error() string {
	if e.status != 0 {
		return fmt.Sprintf("hubspot %s (HTTP %d): %s", e.class, e.status, e.message)
	}
	return fmt.Sprintf("hubspot %s: %s", e.class, e.message)
}

// HubSpotContact is a minimal representation of a HubSpot contact API response.
type HubSpotContact struct {
	ID string `json:"id"`
}

// HubSpotDestination writes contacts to HubSpot using the safe
// Resolve → Mutate → Record single-record flow:
//
//  1. Check for a stored destination_id in entity state.
//  2. If found: PATCH directly by ID.
//  3. If not found: search by match_on field.
//  4. One result → PATCH and persist the ID.
//  5. Zero results → POST create and persist the returned ID.
//  6. Multiple results → DLQ (duplicate_match — operator review required).
type HubSpotDestination struct {
	cfg     config.DestinationConfig
	auth    httpauth.Authenticator
	rl      *httpauth.RateLimiter
	client  *http.Client
	baseURL string
	object  string // "contacts"
	matchOn string // "email"
}

var _ Destination = (*HubSpotDestination)(nil)

// NewHubSpotDestination returns a new HubSpotDestination.
func NewHubSpotDestination() *HubSpotDestination {
	return &HubSpotDestination{}
}

func init() {
	registry.RegisterDestination("hubspot", func() any {
		return NewHubSpotDestination()
	})
}

// Connect validates config and initialises the HTTP client and rate limiter.
func (h *HubSpotDestination) Connect(_ context.Context, cfg config.DestinationConfig) error {
	if strings.TrimSpace(cfg.Auth.Token) == "" {
		return errors.New("hubspot: auth.token is required (use a private app token)")
	}

	object := strings.TrimSpace(cfg.Options["object"])
	if object == "" {
		object = "contacts"
	}
	// MatchOn is comma-separated; take the first field (typically "email").
	matchOn := strings.TrimSpace(strings.SplitN(cfg.MatchOn, ",", 2)[0])
	if matchOn == "" {
		matchOn = "email"
	}

	auth, err := httpauth.NewAuthenticator(cfg.Auth)
	if err != nil {
		return err
	}

	// Default: 5 req/sec (conservative for private apps on any tier).
	rlCfg := cfg.RateLimit
	if rlCfg.Requests == 0 {
		rlCfg = config.RateLimitConfig{Requests: 5, Period: "1s"}
	}
	rl, err := httpauth.NewRateLimiter(rlCfg)
	if err != nil {
		return err
	}

	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		baseURL = hsDefaultBaseURL
	}

	h.cfg = cfg
	h.auth = auth
	h.rl = rl
	h.client = &http.Client{Timeout: 30 * time.Second}
	h.baseURL = baseURL
	h.object = object
	h.matchOn = matchOn
	return nil
}

// Load delivers rows one at a time. Each row goes through Resolve → Mutate → Record.
// On fatal auth errors (401/403) the run is aborted immediately via the returned error.
// All other per-row failures are collected in LoadResult.Errors for DLQ handling.
func (h *HubSpotDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destName string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}

	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		// Idempotency: skip if this delivery key was already recorded as delivered.
		delivered, err := store.IsDelivered(ctx, rw.ID, pipeline, destName)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}

		destID, rowErr, fatalErr := h.deliver(ctx, rw, store, pipeline, destName)
		if fatalErr != nil {
			// 401/403: no point continuing — every subsequent request will also fail.
			return result, fatalErr
		}
		if rowErr != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: rowErr})
			continue
		}

		if err := store.MarkDelivered(ctx, rw.ID, pipeline, destName); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if destID != "" {
			if result.DestinationIDs == nil {
				result.DestinationIDs = make(map[string]string)
			}
			result.DestinationIDs[rw.ID] = destID
		}
		result.Loaded++
	}
	return result, nil
}

// deliver runs the Resolve → Mutate → Record flow for a single row.
// Returns (hubspotContactID, rowError, fatalError).
// fatalError is non-nil only for 401/403 — it aborts the entire run.
func (h *HubSpotDestination) deliver(
	ctx context.Context,
	rw row.Row,
	store state.StateStore,
	pipeline, destName string,
) (destID string, rowErr error, fatalErr error) {
	// Resolve: check for a stored HubSpot contact ID in entity state.
	es, _ := store.GetEntityState(ctx, pipeline, destName, rw.PrimaryKey)
	storedID := ""
	if es != nil {
		storedID = es.DestinationID
	}

	// Delete path: engine signals this via Metadata["_action"] = "delete".
	if action, _ := rw.Metadata["_action"].(string); action == "delete" {
		if storedID == "" {
			// No destination ID stored — nothing to archive; treat as success.
			return "", nil, nil
		}
		if err := h.archiveContact(ctx, storedID); err != nil {
			return h.classifyErr(err)
		}
		return storedID, nil, nil
	}

	// Build HubSpot properties map: all mapped fields, values stringified.
	props := make(map[string]string, len(rw.Data))
	for k, v := range rw.Data {
		if v == nil {
			continue
		}
		props[k] = fmt.Sprintf("%v", v)
	}

	var hubspotID string

	if storedID != "" {
		// Fast path: PATCH directly by stored ID — no search needed.
		if err := h.updateContact(ctx, storedID, props); err != nil {
			return h.classifyErr(err)
		}
		hubspotID = storedID
	} else {
		// Resolve path: search by matchOn field (typically email).
		matchVal, ok := rw.Data[h.matchOn]
		if !ok || matchVal == nil || fmt.Sprintf("%v", matchVal) == "" {
			return "", fmt.Errorf("hubspot: match_on field %q is empty for entity %q", h.matchOn, rw.PrimaryKey), nil
		}

		found, isDuplicate, err := h.searchByField(ctx, h.matchOn, fmt.Sprintf("%v", matchVal))
		if err != nil {
			return h.classifyErr(err)
		}
		if isDuplicate {
			return "", &hsError{
				class: hsClassDuplicate,
				message: fmt.Sprintf(
					"search by %s=%q returned multiple contacts — operator review required",
					h.matchOn, matchVal,
				),
			}, nil
		}

		if found != "" {
			// One contact found: patch it and remember the ID.
			if err := h.updateContact(ctx, found, props); err != nil {
				return h.classifyErr(err)
			}
			hubspotID = found
		} else {
			// No contact found: create it.
			created, err := h.createContact(ctx, props)
			if err != nil {
				return h.classifyErr(err)
			}
			hubspotID = created
		}
	}

	return hubspotID, nil, nil
}

// classifyErr maps a *hsError into (destID, rowErr, fatalErr).
// Auth errors abort the run; all others become row-level DLQ candidates.
func (h *HubSpotDestination) classifyErr(err error) (string, error, error) {
	var hs *hsError
	if errors.As(err, &hs) && hs.class == hsClassAuth {
		return "", nil, err
	}
	return "", err, nil
}

// searchByField queries HubSpot for contacts matching field=value.
// Returns (id, isDuplicate, err).
// id is "" when not found. isDuplicate is true when 2+ contacts matched.
func (h *HubSpotDestination) searchByField(ctx context.Context, field, value string) (string, bool, error) {
	body, _ := json.Marshal(map[string]any{
		"filterGroups": []map[string]any{
			{"filters": []map[string]any{
				{"propertyName": field, "operator": "EQ", "value": value},
			}},
		},
		"properties": []string{field},
		"limit":      2, // only need to know: 0, 1, or ≥2
	})

	endpoint := fmt.Sprintf("%s/crm/v3/objects/%s/search", h.baseURL, h.object)

	var result struct {
		Results []HubSpotContact `json:"results"`
	}
	if err := h.doWithRetry(ctx, http.MethodPost, endpoint, body, &result); err != nil {
		return "", false, err
	}

	switch len(result.Results) {
	case 0:
		return "", false, nil
	case 1:
		return result.Results[0].ID, false, nil
	default:
		return "", true, nil
	}
}

// createContact creates a new HubSpot contact and returns its ID.
func (h *HubSpotDestination) createContact(ctx context.Context, props map[string]string) (string, error) {
	body, _ := json.Marshal(map[string]any{"properties": props})
	endpoint := fmt.Sprintf("%s/crm/v3/objects/%s", h.baseURL, h.object)
	var result HubSpotContact
	if err := h.doWithRetry(ctx, http.MethodPost, endpoint, body, &result); err != nil {
		return "", err
	}
	return result.ID, nil
}

// updateContact patches an existing HubSpot contact by ID.
func (h *HubSpotDestination) updateContact(ctx context.Context, id string, props map[string]string) error {
	body, _ := json.Marshal(map[string]any{"properties": props})
	endpoint := fmt.Sprintf("%s/crm/v3/objects/%s/%s", h.baseURL, h.object, id)
	return h.doWithRetry(ctx, http.MethodPatch, endpoint, body, nil)
}

// archiveContact soft-deletes (archives) a HubSpot object by ID.
func (h *HubSpotDestination) archiveContact(ctx context.Context, id string) error {
	endpoint := fmt.Sprintf("%s/crm/v3/objects/%s/%s", h.baseURL, h.object, id)
	return h.doWithRetry(ctx, http.MethodDelete, endpoint, nil, nil)
}

// doWithRetry executes an HTTP request, retrying on 429/5xx up to 3 times.
// Terminal errors (400/422/409/401/403) are returned immediately without retry.
// Timeouts and network errors are returned as hsClassAmbiguous — the caller
// must NOT mark state as success for ambiguous outcomes.
func (h *HubSpotDestination) doWithRetry(ctx context.Context, method, url string, body []byte, out any) error {
	const maxAttempts = 3

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return &hsError{class: hsClassAmbiguous, message: err.Error()}
		}
		if h.rl != nil {
			if err := h.rl.Wait(ctx); err != nil {
				return &hsError{class: hsClassAmbiguous, message: err.Error()}
			}
		}

		var bodyReader io.Reader
		if len(body) > 0 {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if h.auth != nil {
			if err := h.auth.Apply(req); err != nil {
				return err
			}
		}

		resp, err := h.client.Do(req)
		if err != nil {
			// Network error or timeout. For mutating requests we don't know if
			// the request landed, so treat as ambiguous — never mark success.
			return &hsError{class: hsClassAmbiguous, message: err.Error()}
		}
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(respBody) > 0 {
				if err := json.Unmarshal(respBody, out); err != nil {
					return fmt.Errorf("hubspot: decode response: %w", err)
				}
			}
			return nil
		}

		msg := hsReadMessage(respBody)
		switch {
		case resp.StatusCode == http.StatusUnauthorized:
			return &hsError{class: hsClassAuth, status: resp.StatusCode, message: "invalid or missing private app token"}
		case resp.StatusCode == http.StatusForbidden:
			return &hsError{class: hsClassAuth, status: resp.StatusCode, message: "token lacks required scope"}
		case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == 422:
			return &hsError{class: hsClassValidation, status: resp.StatusCode, message: msg}
		case resp.StatusCode == http.StatusConflict:
			return &hsError{class: hsClassConflict, status: resp.StatusCode, message: msg}
		default:
			// 429 or 5xx: retryable.
			lastErr = &hsError{class: hsClassRetryable, status: resp.StatusCode, message: msg}
		}

		if attempt < maxAttempts-1 {
			backoff := time.Duration(1<<attempt) * time.Second
			select {
			case <-ctx.Done():
				return &hsError{class: hsClassAmbiguous, message: ctx.Err().Error()}
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}

// hsReadMessage extracts a human-readable error string from a HubSpot response body.
func hsReadMessage(body []byte) string {
	var payload struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if json.Unmarshal(body, &payload) == nil {
		if payload.Message != "" {
			return payload.Message
		}
		if payload.Error != "" {
			return payload.Error
		}
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 200 {
		return msg[:200] + "..."
	}
	return msg
}

// Close releases rate limiter resources.
func (h *HubSpotDestination) Close() error {
	if h.rl != nil {
		h.rl.Stop()
	}
	return nil
}
