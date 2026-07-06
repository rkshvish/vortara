package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/engine"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var flagDLQFile string

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Inspect dead-lettered rows",
}

var dlqShowCmd = &cobra.Command{
	Use:   "show <sync.yaml>",
	Short: "Show dead-lettered rows",
	Args:  cobra.ExactArgs(1),
	RunE:  runDLQShow,
}

func init() {
	dlqCmd.PersistentFlags().StringVar(&flagDLQFile, "file", "", "DLQ file path (default: errors.dlq_path or ./dlq/<name>.dlq.jsonl)")
	dlqCmd.AddCommand(dlqShowCmd)
}

func runDLQShow(_ *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}

	path := flagDLQFile
	if path == "" {
		path = engine.ResolveDLQPath(f.Sync.Name, f.Sync.Errors.DLQPath)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Sync: %s\nDLQ:  %s (empty — no dead-lettered rows)\n", f.Sync.Name, path)
			return nil
		}
		return err
	}

	var records []engine.DLQRecord
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		var rec engine.DLQRecord
		if err := json.Unmarshal(line, &rec); err == nil {
			records = append(records, rec)
		}
	}

	fmt.Printf("Sync: %s\nDLQ:  %s\nRows: %d\n\n", f.Sync.Name, path, len(records))
	const preview = 10
	for i, rec := range records {
		if i >= preview {
			fmt.Printf("... and %d more\n", len(records)-preview)
			break
		}
		fmt.Printf("#%-4d %s  %s\n      error: %s\n",
			i+1, rec.FailedAt.Format("2006-01-02 15:04:05"), rec.EntityKey, rec.Error)
	}
	return nil
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
