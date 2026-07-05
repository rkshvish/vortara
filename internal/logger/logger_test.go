package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func withTestOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	oldStderr := stderr
	oldTTY := isTerminal
	stderr = buf
	isTerminal = func() bool { return false }
	t.Cleanup(func() {
		stderr = oldStderr
		isTerminal = oldTTY
	})
	return buf
}

func TestNew_JSONFormat(t *testing.T) {
	buf := withTestOutput(t)
	l := New("info", "json")
	l.Info("hello")

	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got["level"] == nil || got["msg"] != "hello" {
		t.Fatalf("unexpected json payload: %+v", got)
	}
}

func TestNew_TextFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	oldStderr := stderr
	oldTTY := isTerminal
	stderr = buf
	isTerminal = func() bool { return true }
	t.Cleanup(func() {
		stderr = oldStderr
		isTerminal = oldTTY
	})

	l := New("info", "text")
	l.Info("hello")
	out := buf.String()
	if !strings.Contains(out, "INFO") || !strings.Contains(out, "hello") {
		t.Fatalf("unexpected text output: %q", out)
	}
}

func TestNew_DebugLevel(t *testing.T) {
	buf := withTestOutput(t)
	l := New("debug", "json")
	l.Debug("debug message")
	if !strings.Contains(buf.String(), "debug message") {
		t.Fatalf("expected debug log, got %q", buf.String())
	}
}

func TestNew_InfoLevel_DebugSuppressed(t *testing.T) {
	buf := withTestOutput(t)
	l := New("info", "json")
	l.Debug("debug message")
	if strings.Contains(buf.String(), "debug message") {
		t.Fatalf("expected debug log to be suppressed, got %q", buf.String())
	}
}

func TestWithPipeline(t *testing.T) {
	buf := withTestOutput(t)
	l := WithPipeline(New("info", "json"), "my-pipeline")
	l.Info("msg")
	if !strings.Contains(buf.String(), `"pipeline":"my-pipeline"`) {
		t.Fatalf("expected pipeline field, got %q", buf.String())
	}
}

func TestFromContext_Default(t *testing.T) {
	if FromContext(context.Background()) == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestFromContext_WithLogger(t *testing.T) {
	l := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	ctx := WithContext(context.Background(), l)
	if got := FromContext(ctx); got != l {
		t.Fatalf("FromContext() returned wrong logger")
	}
}
