package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	httpauth "github.com/rkshvish/vortara/internal/connector/http"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// RESTAPISource implements BatchSource for REST API polling endpoints.
type RESTAPISource struct {
	cfg    config.SourceConfig
	client *http.Client
	auth   httpauth.Authenticator
}

var _ BatchSource = (*RESTAPISource)(nil)

func init() {
	registry.RegisterBatchSource("restapi", func() any {
		return NewRESTAPISource()
	})
}

// NewRESTAPISource returns a new RESTAPISource with a 30 second timeout.
func NewRESTAPISource() *RESTAPISource {
	return &RESTAPISource{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Connect validates the configured endpoint URL and stores the source config.
func (r *RESTAPISource) Connect(ctx context.Context, cfg config.SourceConfig) error {
	if _, err := url.ParseRequestURI(cfg.Connection); err != nil {
		return err
	}
	auth, err := httpauth.NewAuthenticator(cfg.Auth)
	if err != nil {
		return err
	}
	r.cfg = cfg
	r.auth = auth
	return nil
}

// Extract polls the REST API endpoint and streams rows to out.
func (r *RESTAPISource) Extract(ctx context.Context, watermark time.Time, intervalEnd time.Time, out chan<- row.Row) error {
	if r.client == nil {
		r.client = &http.Client{Timeout: 30 * time.Second}
	}
	defer close(out)

	nextCursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		reqURL, err := r.buildURL(watermark, intervalEnd, nextCursor)
		if err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return err
		}
		if r.auth != nil {
			if err := r.auth.Apply(req); err != nil {
				return err
			}
		}

		resp, err := r.client.Do(req)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return err
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("REST API returned %d", resp.StatusCode)
		}

		items, cursor, err := parseAPIResponse(body)
		if err != nil {
			return err
		}

		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return err
			}

			data, pk, wm := buildRowPayload(item)
			result := row.NewRow(
				r.sourceName(),
				r.pipelineName(),
				pk,
				data,
				wm,
			)

			select {
			case out <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if cursor == "" {
			return nil
		}
		// Give cancellation a chance to win between page fetches.
		time.Sleep(5 * time.Millisecond)
		nextCursor = cursor
	}
}

// GetWatermarkColumn returns the configured watermark field or the REST default.
func (r *RESTAPISource) GetWatermarkColumn() string {
	if r.cfg.Options != nil {
		if v := strings.TrimSpace(r.cfg.Options["watermark_field"]); v != "" {
			return v
		}
	}
	return "updated_at"
}

// Close releases resources held by the source connector.
func (r *RESTAPISource) Close() error {
	return nil
}

func (r *RESTAPISource) sourceName() string {
	if r.cfg.Type == "" {
		return "restapi"
	}
	return r.cfg.Type
}

func (r *RESTAPISource) pipelineName() string {
	if r.cfg.Options != nil {
		if name := strings.TrimSpace(r.cfg.Options["pipeline"]); name != "" {
			return name
		}
	}
	return ""
}

func (r *RESTAPISource) buildURL(watermark, intervalEnd time.Time, cursor string) (string, error) {
	parsed, err := url.Parse(r.cfg.Connection)
	if err != nil {
		return "", err
	}

	q := parsed.Query()
	q.Set("since", watermark.UTC().Format(time.RFC3339))
	if !intervalEnd.IsZero() {
		q.Set("until", intervalEnd.UTC().Format(time.RFC3339))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

func parseAPIResponse(body []byte) ([]map[string]interface{}, string, error) {
	var arr []map[string]interface{}
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, "", nil
	}

	var envelope struct {
		Data       []map[string]interface{} `json:"data"`
		NextCursor string                   `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, "", fmt.Errorf("REST API JSON parse error: %w", err)
	}
	return envelope.Data, envelope.NextCursor, nil
}

func buildRowPayload(item map[string]interface{}) (map[string]interface{}, string, time.Time) {
	data := make(map[string]interface{}, len(item))
	for k, v := range item {
		data[k] = normalizeRESTValue(v)
	}

	pk := primaryKeyFromItem(item)
	wm := time.Now().UTC()
	if v, ok := item["updated_at"]; ok {
		if parsed, err := parseRESTTime(v); err == nil {
			wm = parsed
		}
	}

	return data, pk, wm
}

func primaryKeyFromItem(item map[string]interface{}) string {
	if v, ok := item["id"]; ok {
		return "id=" + formatJSONValue(v)
	}
	for k, v := range item {
		return k + "=" + formatJSONValue(v)
	}
	return ""
}

func formatJSONValue(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		if float64(int64(t)) == t {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%v", t)
	case json.Number:
		return t.String()
	case bool:
		return fmt.Sprintf("%t", t)
	default:
		return fmt.Sprint(t)
	}
}

func normalizeRESTValue(v interface{}) interface{} {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	default:
		return v
	}
}

func parseRESTTime(v interface{}) (time.Time, error) {
	switch t := v.(type) {
	case string:
		return parseTimeString(t)
	case time.Time:
		return t.UTC(), nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return time.Unix(i, 0).UTC(), nil
		}
		return time.Time{}, fmt.Errorf("invalid numeric time %q", t.String())
	default:
		return parseTimeString(fmt.Sprint(t))
	}
}
