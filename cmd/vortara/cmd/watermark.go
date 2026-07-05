package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	v2config "github.com/rkshvish/vortara/pkg/config/v2"
)

var watermarkCmd = &cobra.Command{
	Use:   "watermark",
	Short: "Inspect or reset batch watermarks",
}

var watermarkGetCmd = &cobra.Command{
	Use:   "get <pipeline.yaml>",
	Short: "Show the current watermark",
	Args:  cobra.ExactArgs(1),
	RunE:  runWatermarkGet,
}

var watermarkResetCmd = &cobra.Command{
	Use:   "reset <pipeline.yaml>",
	Short: "Reset the current watermark",
	Args:  cobra.ExactArgs(1),
	RunE:  runWatermarkReset,
}

func init() {
	watermarkCmd.AddCommand(watermarkGetCmd)
	watermarkCmd.AddCommand(watermarkResetCmd)
}

func runWatermarkGet(cmd *cobra.Command, args []string) error {
	cfg, err := v2config.Load(args[0])
	if err != nil {
		return err
	}
	if err := v2config.Validate(cfg); err != nil {
		return err
	}

	store, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	srcName := sourceName(cfg)
	if num, err := store.GetNumericWatermark(cfg.Name, srcName); err == nil && num != 0 {
		fmt.Printf("Watermark: %d (numeric cursor)\n", num)
		return nil
	}
	wm, err := store.GetWatermark(cfg.Name, srcName)
	if err != nil {
		return err
	}
	if wm.IsZero() {
		fmt.Println("Watermark: never set")
		return nil
	}
	fmt.Printf("Watermark: %s\n", wm.UTC().Format(time.RFC3339))
	return nil
}

func runWatermarkReset(cmd *cobra.Command, args []string) error {
	cfg, err := v2config.Load(args[0])
	if err != nil {
		return err
	}
	if err := v2config.Validate(cfg); err != nil {
		return err
	}

	store, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := store.SetWatermark(cfg.Name, sourceName(cfg), time.Time{}); err != nil {
		return err
	}
	if err := store.SetNumericWatermark(cfg.Name, sourceName(cfg), 0); err != nil {
		return err
	}
	fmt.Println("Watermark reset. Next run will perform full sync.")
	return nil
}
