// Package metrics writes Prometheus textfile metrics for node_exporter.
package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rkshvish/vortara/internal/state"
)

// Recorder writes per-run .prom files to a directory.
type Recorder struct {
	dir string
}

// New returns a Recorder that writes to dir.
func New(dir string) *Recorder {
	return &Recorder{dir: dir}
}

// RecordRun writes Prometheus textfile metrics for a completed sync run.
// The file is written atomically (tmp + rename) so node_exporter never reads a partial file.
func (r *Recorder) RecordRun(syncName string, stats state.RunStats, elapsed time.Duration) error {
	safeName := strings.NewReplacer("/", "_", ".", "_", "-", "_").Replace(syncName)
	destPath := filepath.Join(r.dir, safeName+".prom")

	labels := fmt.Sprintf(`sync=%q`, syncName)
	statusLabel := fmt.Sprintf(`sync=%q,status=%q`, syncName, stats.Status)

	var b strings.Builder
	fmt.Fprintf(&b, "# HELP vortara_run_total Number of sync runs completed (reset on process restart)\n")
	fmt.Fprintf(&b, "# TYPE vortara_run_total counter\n")
	fmt.Fprintf(&b, "vortara_run_total{%s} 1\n", statusLabel)
	fmt.Fprintf(&b, "# HELP vortara_run_duration_seconds Duration of the last sync run\n")
	fmt.Fprintf(&b, "# TYPE vortara_run_duration_seconds gauge\n")
	fmt.Fprintf(&b, "vortara_run_duration_seconds{%s} %g\n", labels, elapsed.Seconds())
	fmt.Fprintf(&b, "# HELP vortara_source_rows_read_total Rows extracted from source in the last run\n")
	fmt.Fprintf(&b, "# TYPE vortara_source_rows_read_total gauge\n")
	fmt.Fprintf(&b, "vortara_source_rows_read_total{%s} %d\n", labels, stats.RowsExtracted)
	fmt.Fprintf(&b, "# HELP vortara_decisions_total Row counts by decision type in the last run\n")
	fmt.Fprintf(&b, "# TYPE vortara_decisions_total gauge\n")
	fmt.Fprintf(&b, "vortara_decisions_total{%s,decision=\"create\"} %d\n", labels, stats.Creates)
	fmt.Fprintf(&b, "vortara_decisions_total{%s,decision=\"update\"} %d\n", labels, stats.Updates)
	fmt.Fprintf(&b, "vortara_decisions_total{%s,decision=\"delete\"} %d\n", labels, stats.Deletes)
	fmt.Fprintf(&b, "vortara_decisions_total{%s,decision=\"skip\"} %d\n", labels, stats.RowsSkipped)
	fmt.Fprintf(&b, "# HELP vortara_delivery_failures_total Rows that failed delivery in the last run\n")
	fmt.Fprintf(&b, "# TYPE vortara_delivery_failures_total gauge\n")
	fmt.Fprintf(&b, "vortara_delivery_failures_total{%s} %d\n", labels, stats.RowsErrored)

	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return fmt.Errorf("metrics: create dir %q: %w", r.dir, err)
	}
	tmp, err := os.CreateTemp(r.dir, safeName+"*.prom.tmp")
	if err != nil {
		return fmt.Errorf("metrics: create tmp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(b.String()); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("metrics: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("metrics: close: %w", err)
	}
	if err := os.Rename(tmpName, destPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("metrics: rename to %q: %w", destPath, err)
	}
	return nil
}
