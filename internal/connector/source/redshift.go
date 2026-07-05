package source

import (
	"context"
	"strings"

	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/pkg/config"
)

// RedshiftSource extracts rows from Amazon Redshift. Redshift speaks the
// PostgreSQL wire protocol, so this embeds PostgresSource and only
// normalizes the redshift:// URL scheme (the same approach ingestr and
// bento take). Watermark windows, introspection, and pagination are the
// shared, integration-tested postgres paths.
type RedshiftSource struct {
	*PostgresSource
}

var _ BatchSource = (*RedshiftSource)(nil)

func init() {
	registry.RegisterBatchSource("redshift", func() any {
		return NewRedshiftSource()
	})
}

// NewRedshiftSource returns a new RedshiftSource.
func NewRedshiftSource() *RedshiftSource {
	return &RedshiftSource{PostgresSource: NewPostgresSource()}
}

// Connect normalizes the URL to the postgres scheme and connects.
func (r *RedshiftSource) Connect(ctx context.Context, cfg config.SourceConfig) error {
	cfg.Connection = normalizeRedshiftURI(cfg.Connection)
	cfg.URL = normalizeRedshiftURI(cfg.URL)
	return r.PostgresSource.Connect(ctx, cfg)
}

// normalizeRedshiftURI converts a redshift:// URI into a postgres:// URI so
// the pgx driver accepts it. Redshift clusters require SSL, so sslmode is
// left to the user's query parameters (default pgx behavior is prefer).
func normalizeRedshiftURI(uri string) string {
	scheme, rest, ok := strings.Cut(uri, "://")
	if !ok {
		return uri
	}
	if strings.HasPrefix(strings.ToLower(scheme), "redshift") {
		return "postgres://" + rest
	}
	return uri
}
