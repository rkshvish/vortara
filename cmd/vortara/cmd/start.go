package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortaraos/api"
	"github.com/rkshvish/vortaraos/internal/engine"
	vlogger "github.com/rkshvish/vortaraos/internal/logger"
	v2config "github.com/rkshvish/vortaraos/pkg/config/v2"
)

var flagAPIPort int

var startCmd = &cobra.Command{
	Use:   "start <pipeline.yaml>",
	Short: "Start pipeline scheduler (daemon mode)",
	Args:  cobra.ExactArgs(1),
	RunE:  runStart,
}

func init() {
	startCmd.Flags().IntVar(&flagAPIPort, "api-port", 0, "Serve /ping, /ready, /health, /metrics, /version on this port (0 = disabled)")
}

func runStart(cmd *cobra.Command, args []string) error {
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
	eng := engine.NewEngine(store)
	defer eng.Close()

	baseCtx := vlogger.WithContext(
		context.Background(),
		vlogger.New(cfg.Settings.Log.Level, cfg.Settings.Log.Format),
	)
	ctx, cancel := signal.NotifyContext(baseCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if flagAPIPort > 0 {
		api.Version = version
		apiSrv := api.NewServer(eng, flagAPIPort)
		if err := apiSrv.Start(ctx); err != nil {
			return fmt.Errorf("api server: %w", err)
		}
		defer func() { _ = apiSrv.Stop() }()
		fmt.Printf("API listening on 127.0.0.1:%d (/ping /ready /health /metrics /version)\n", flagAPIPort)
	}

	fmt.Printf("Starting pipeline: %s\n", cfg.Name)
	return eng.Run(ctx, cfg)
}
