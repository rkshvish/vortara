package source

import (
	"context"
	"testing"

	"github.com/rkshvish/vortaraos/pkg/config"
)

func TestNormalizeRedshiftURI(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{
			in:   "redshift://user:pass@cluster.abc123.us-east-1.redshift.amazonaws.com:5439/analytics",
			want: "postgres://user:pass@cluster.abc123.us-east-1.redshift.amazonaws.com:5439/analytics",
		},
		{
			in:   "REDSHIFT://u:p@host:5439/db?sslmode=require",
			want: "postgres://u:p@host:5439/db?sslmode=require",
		},
		{
			// postgres URIs pass through untouched
			in:   "postgres://u:p@host:5432/db",
			want: "postgres://u:p@host:5432/db",
		},
		{
			// non-URI strings pass through untouched
			in:   "host=localhost dbname=x",
			want: "host=localhost dbname=x",
		},
	}
	for _, tt := range tests {
		if got := normalizeRedshiftURI(tt.in); got != tt.want {
			t.Errorf("normalizeRedshiftURI(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRedshiftSource_Connect_Validation(t *testing.T) {
	s := NewRedshiftSource()
	// Missing connection must fail through the embedded postgres validation.
	err := s.Connect(context.Background(), config.SourceConfig{Table: "t"})
	if err == nil {
		t.Fatal("Connect() without connection should fail")
	}
}
