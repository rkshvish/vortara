package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type contextKey struct{}

var stderr io.Writer = os.Stderr

var isTerminal = func() bool {
	info, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// New creates a slog.Logger with the given level and format.
func New(level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	switch normalizeFormat(format) {
	case "json":
		return slog.New(slog.NewJSONHandler(stderr, opts))
	default:
		return slog.New(slog.NewTextHandler(stderr, opts))
	}
}

// WithPipeline returns a logger with pipeline attached.
func WithPipeline(l *slog.Logger, pipeline string) *slog.Logger {
	if l == nil {
		l = New("", "")
	}
	return l.With(slog.String("pipeline", pipeline))
}

// WithRunID returns a logger with run_id attached.
func WithRunID(l *slog.Logger, runID int64) *slog.Logger {
	if l == nil {
		l = New("", "")
	}
	return l.With(slog.Int64("run_id", runID))
}

// WithDestination returns a logger with destination attached.
func WithDestination(l *slog.Logger, dest string) *slog.Logger {
	if l == nil {
		l = New("", "")
	}
	return l.With(slog.String("destination", dest))
}

// FromContext retrieves logger from context, falling back to a default logger.
func FromContext(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if l, ok := ctx.Value(contextKey{}).(*slog.Logger); ok && l != nil {
			return l
		}
	}
	return New("", "")
}

// WithContext stores logger in context.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, l)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func normalizeFormat(format string) string {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		if isTerminal() {
			return "text"
		}
		return "json"
	}
	if format == "json" {
		return "json"
	}
	return "text"
}
