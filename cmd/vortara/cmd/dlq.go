package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/engine"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	v2config "github.com/rkshvish/vortara/pkg/config/pipeline"
)

var flagDLQFile string

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Inspect or replay dead-lettered rows",
}

var dlqShowCmd = &cobra.Command{
	Use:   "show <pipeline.yaml>",
	Short: "Show dead-lettered rows for a pipeline",
	Args:  cobra.ExactArgs(1),
	RunE:  runDLQShow,
}

var dlqReplayCmd = &cobra.Command{
	Use:   "replay <pipeline.yaml>",
	Short: "Re-deliver dead-lettered rows; successful rows are removed from the file",
	Args:  cobra.ExactArgs(1),
	RunE:  runDLQReplay,
}

func init() {
	dlqCmd.PersistentFlags().StringVar(&flagDLQFile, "file", "", "DLQ file path (default: settings.dlq_path or <name>.dlq.jsonl)")
	dlqCmd.AddCommand(dlqShowCmd)
	dlqCmd.AddCommand(dlqReplayCmd)
}

func dlqPathFromArgs(args []string) (*v2config.PipelineConfig, string, error) {
	cfg, err := v2config.Load(args[0])
	if err != nil {
		return nil, "", err
	}
	if err := v2config.Validate(cfg); err != nil {
		return nil, "", err
	}
	path := flagDLQFile
	if path == "" {
		path = engine.ResolveDLQPath(cfg)
	}
	return cfg, path, nil
}

func runDLQShow(cmd *cobra.Command, args []string) error {
	cfg, path, err := dlqPathFromArgs(args)
	if err != nil {
		return err
	}
	records, err := engine.ReadDLQRecords(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Pipeline: %s\nDLQ:      %s (empty — no dead-lettered rows)\n", cfg.Name, path)
			return nil
		}
		return err
	}
	fmt.Printf("Pipeline: %s\nDLQ:      %s\nRows:     %d\n\n", cfg.Name, path, len(records))
	const preview = 10
	for i, rec := range records {
		if i >= preview {
			fmt.Printf("... and %d more\n", len(records)-preview)
			break
		}
		fmt.Printf("#%-4d %s  %s\n      error: %s\n", i+1, rec.FailedAt.Format("2006-01-02 15:04:05"), rec.PrimaryKey, rec.Error)
	}
	return nil
}

func runDLQReplay(cmd *cobra.Command, args []string) error {
	cfg, path, err := dlqPathFromArgs(args)
	if err != nil {
		return err
	}

	store, err := openStore(cfg)
	if err != nil {
		return err
	}
	eng := engine.NewEngine(store)
	defer eng.Close()

	baseCtx := vlogger.WithContext(
		context.Background(),
		vlogger.New(cfg.Settings.Log.Level, cfg.Settings.Log.Format),
	)
	ctx, cancel := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Replaying DLQ for %s from %s\n", cfg.Name, path)
	res, err := eng.ReplayDLQ(ctx, cfg, path)
	if err != nil {
		return err
	}
	fmt.Printf("✓ Replay: read=%d replayed=%d still-failing=%d\n", res.Read, res.Replayed, res.Failed)
	if res.Failed > 0 {
		fmt.Printf("Failing rows remain in %s — inspect with: vortara dlq show %s\n", path, args[0])
		return fmt.Errorf("%d rows failed replay", res.Failed)
	}
	return nil
}
