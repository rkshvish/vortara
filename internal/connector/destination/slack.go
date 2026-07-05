package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	httpauth "github.com/rkshvish/vortaraos/internal/connector/http"
	"github.com/rkshvish/vortaraos/internal/registry"
	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// SlackDestination posts one message per row to a Slack incoming webhook.
type SlackDestination struct {
	cfg         config.DestinationConfig
	client      *http.Client
	webhookURL  string
	template    string
	rateLimiter *httpauth.RateLimiter
}

var _ Destination = (*SlackDestination)(nil)

func init() {
	registry.RegisterDestination("slack", func() any {
		return NewSlackDestination()
	})
}

// NewSlackDestination returns a new SlackDestination with a 30 second timeout.
func NewSlackDestination() *SlackDestination {
	return &SlackDestination{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Connect validates the webhook URL and message template.
func (s *SlackDestination) Connect(ctx context.Context, cfg config.DestinationConfig) error {
	webhook := strings.TrimSpace(cfg.Options["webhook"])
	if webhook == "" {
		webhook = strings.TrimSpace(cfg.URL)
	}
	if webhook == "" {
		return errors.New("slack destination: webhook is required")
	}
	if _, err := url.ParseRequestURI(webhook); err != nil {
		return fmt.Errorf("slack destination: invalid webhook url: %w", err)
	}
	message := strings.TrimSpace(cfg.Options["message"])
	if message == "" {
		return errors.New("slack destination: message template is required")
	}
	rl, err := httpauth.NewRateLimiter(cfg.RateLimit)
	if err != nil {
		return err
	}
	s.cfg = cfg
	s.webhookURL = webhook
	s.template = message
	s.rateLimiter = rl
	return nil
}

var slackFieldPattern = regexp.MustCompile(`\{\{\s*row\.([A-Za-z0-9_]+)\s*\}\}`)

// renderMessage substitutes {{ row.field }} placeholders with row data values.
func renderMessage(template string, data map[string]interface{}) string {
	return slackFieldPattern.ReplaceAllStringFunc(template, func(match string) string {
		field := slackFieldPattern.FindStringSubmatch(match)[1]
		val, ok := data[field]
		if !ok || val == nil {
			return ""
		}
		return fmt.Sprintf("%v", val)
	})
}

// Load posts one Slack message per row, skipping rows already delivered.
func (s *SlackDestination) Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error) {
	var result LoadResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if s.client == nil {
		s.client = &http.Client{Timeout: 30 * time.Second}
	}

	for _, rw := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		delivered, err := store.IsDelivered(rw.ID, pipeline, destination)
		if err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		if delivered {
			result.Skipped++
			continue
		}

		if s.rateLimiter != nil {
			if err := s.rateLimiter.Wait(ctx); err != nil {
				return result, err
			}
		}

		if err := s.postMessage(ctx, renderMessage(s.template, rw.Data)); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}

		if err := store.MarkDelivered(rw.ID, pipeline, destination); err != nil {
			result.Errors = append(result.Errors, RowError{RowID: rw.ID, Row: rw, Err: err})
			continue
		}
		result.Loaded++
	}
	return result, nil
}

func (s *SlackDestination) postMessage(ctx context.Context, text string) error {
	body, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("slack webhook returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// Close is a no-op for the Slack destination.
func (s *SlackDestination) Close() error {
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
	return nil
}
