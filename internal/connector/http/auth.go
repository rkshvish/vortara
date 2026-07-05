// Package http provides shared HTTP authentication helpers for connectors.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rkshvish/vortara/pkg/config"
)

// Authenticator applies authentication to an outbound HTTP request.
type Authenticator interface {
	// Apply adds auth credentials to the request.
	Apply(req *nethttp.Request) error

	// Name returns the auth type for logging.
	Name() string
}

// NewAuthenticator builds the correct Authenticator from config.
func NewAuthenticator(cfg config.AuthConfig) (Authenticator, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "":
		return nil, nil
	case "bearer":
		if strings.TrimSpace(cfg.Token) == "" {
			return nil, fmt.Errorf("bearer auth requires token")
		}
		return &BearerAuth{Token: cfg.Token}, nil
	case "api_key":
		if strings.TrimSpace(cfg.Key) == "" || strings.TrimSpace(cfg.Value) == "" {
			return nil, fmt.Errorf("api_key auth requires key and value")
		}
		inHeader := true
		if cfg.InHeaderSpecified() {
			inHeader = cfg.InHeader
		}
		return &APIKeyAuth{
			Key:      cfg.Key,
			Value:    cfg.Value,
			InHeader: inHeader,
		}, nil
	case "basic":
		return &BasicAuth{Username: cfg.Username, Password: cfg.Password}, nil
	case "oauth2_client_credentials":
		if strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientSecret) == "" || strings.TrimSpace(cfg.TokenURL) == "" {
			return nil, fmt.Errorf("oauth2_client_credentials requires client_id, client_secret, token_url")
		}
		return &OAuth2ClientCredentials{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			TokenURL:     cfg.TokenURL,
			Scopes:       append([]string(nil), cfg.Scopes...),
		}, nil
	default:
		return nil, fmt.Errorf("unknown auth type %q, valid: bearer, api_key, basic, oauth2_client_credentials", cfg.Type)
	}
}

// BearerAuth adds a Bearer token Authorization header.
type BearerAuth struct {
	Token string
}

// Name returns the auth type name.
func (a *BearerAuth) Name() string { return "bearer" }

// Apply adds the Authorization header.
func (a *BearerAuth) Apply(req *nethttp.Request) error {
	req.Header.Set("Authorization", "Bearer "+a.Token)
	return nil
}

// APIKeyAuth adds an API key to a header or query parameter.
type APIKeyAuth struct {
	Key      string
	Value    string
	InHeader bool
}

// Name returns the auth type name.
func (a *APIKeyAuth) Name() string { return "api_key" }

// Apply adds the API key header or query parameter.
func (a *APIKeyAuth) Apply(req *nethttp.Request) error {
	if a.InHeader {
		req.Header.Set(a.Key, a.Value)
		return nil
	}
	q := req.URL.Query()
	q.Set(a.Key, a.Value)
	req.URL.RawQuery = q.Encode()
	return nil
}

// BasicAuth adds HTTP Basic Auth credentials.
type BasicAuth struct {
	Username string
	Password string
}

// Name returns the auth type name.
func (a *BasicAuth) Name() string { return "basic" }

// Apply sets the Basic Auth header.
func (a *BasicAuth) Apply(req *nethttp.Request) error {
	req.SetBasicAuth(a.Username, a.Password)
	return nil
}

// OAuth2ClientCredentials fetches and caches a client-credentials token.
type OAuth2ClientCredentials struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string

	mu          sync.Mutex
	client      *nethttp.Client
	cachedToken string
	expiresAt   time.Time
}

// Name returns the auth type name.
func (a *OAuth2ClientCredentials) Name() string { return "oauth2_client_credentials" }

// Apply fetches a bearer token and adds it to the request.
func (a *OAuth2ClientCredentials) Apply(req *nethttp.Request) error {
	token, err := a.getToken(req.Context())
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

func (a *OAuth2ClientCredentials) getToken(ctx context.Context) (string, error) {
	a.mu.Lock()
	if a.cachedToken != "" && time.Until(a.expiresAt) > 30*time.Second {
		token := a.cachedToken
		a.mu.Unlock()
		return token, nil
	}
	a.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", a.ClientID)
	form.Set("client_secret", a.ClientSecret)
	if len(a.Scopes) > 0 {
		form.Set("scope", strings.Join(a.Scopes, " "))
	}

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, a.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := a.client
	if client == nil {
		client = nethttp.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("oauth2 token server returned %d", resp.StatusCode)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("oauth2 token server returned empty access_token")
	}

	expiry := time.Now().Add(time.Duration(payload.ExpiresIn) * time.Second)
	a.mu.Lock()
	a.cachedToken = payload.AccessToken
	a.expiresAt = expiry
	a.mu.Unlock()
	return payload.AccessToken, nil
}
