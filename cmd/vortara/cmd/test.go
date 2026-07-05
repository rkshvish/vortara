package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/connector/source"
	"github.com/rkshvish/vortara/internal/registry"
	v2config "github.com/rkshvish/vortara/pkg/config/v2"
)

var testCmd = &cobra.Command{
	Use:   "test <pipeline.yaml>",
	Short: "Test all connections in a pipeline config",
	Args:  cobra.ExactArgs(1),
	RunE:  runTest,
}

func runTest(cmd *cobra.Command, args []string) error {
	cfg, err := v2config.Load(args[0])
	if err != nil {
		return err
	}
	if err := v2config.Validate(cfg); err != nil {
		return err
	}

	allOK := true

	if isBatchSource(cfg.Source.Type) {
		fmt.Printf("Testing source [%s]... ", cfg.Source.Type)
		raw, err := registry.GetBatchSource(cfg.Source.Type)
		if err != nil {
			fmt.Printf("x unknown type: %v\n", err)
			allOK = false
		} else if src, ok := raw.(source.BatchSource); !ok {
			fmt.Printf("x invalid type: %T\n", raw)
			allOK = false
		} else if err := connectBatchSource(cfg, src); err != nil {
			fmt.Printf("x %v\n", err)
			allOK = false
		} else {
			fmt.Printf("ok\n")
			_ = src.Close()
		}
	}

	if isStreamingSource(cfg.Source.Type) {
		fmt.Printf("Testing streaming source [%s]... ", cfg.Source.Type)
		raw, err := registry.GetStreamingSource(cfg.Source.Type)
		if err != nil {
			fmt.Printf("x unknown type: %v\n", err)
			allOK = false
		} else if src, ok := raw.(source.StreamingSource); !ok {
			fmt.Printf("x invalid type: %T\n", raw)
			allOK = false
		} else if err := connectStreamingSource(cfg.Source, src); err != nil {
			fmt.Printf("x %v\n", err)
			allOK = false
		} else {
			fmt.Printf("ok\n")
			_ = src.Close()
		}
	}

	if cfg.Also != nil {
		fmt.Printf("Testing streaming source [%s]... ", cfg.Also.Type)
		raw, err := registry.GetStreamingSource(cfg.Also.Type)
		if err != nil {
			fmt.Printf("x unknown type: %v\n", err)
			allOK = false
		} else if src, ok := raw.(source.StreamingSource); !ok {
			fmt.Printf("x invalid type: %T\n", raw)
			allOK = false
		} else if err := connectAlsoStreamingSource(*cfg.Also, src); err != nil {
			fmt.Printf("x %v\n", err)
			allOK = false
		} else {
			fmt.Printf("ok\n")
			_ = src.Close()
		}
	}

	for i, destCfg := range cfg.Destinations {
		fmt.Printf("Testing destination [%d/%s]... ", i, destCfg.Type)
		raw, err := registry.GetDestination(destCfg.Type)
		if err != nil {
			fmt.Printf("x unknown type: %v\n", err)
			allOK = false
			continue
		}
		dest, ok := raw.(destination.Destination)
		if !ok {
			fmt.Printf("x invalid type: %T\n", raw)
			allOK = false
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = dest.Connect(ctx, v2config.ToDestinationConfig(destCfg))
		cancel()
		if err != nil {
			fmt.Printf("x %v\n", err)
			allOK = false
		} else {
			fmt.Printf("ok\n")
		}
		_ = dest.Close()
	}

	if !allOK {
		return fmt.Errorf("one or more connections failed")
	}
	fmt.Println("\nAll connections OK")
	return nil
}

func connectBatchSource(cfg *v2config.PipelineConfig, src source.BatchSource) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := src.Connect(ctx, v2config.ToSourceConfig(cfg.Source))
	if err != nil {
		_ = src.Close()
	}
	return err
}

func connectStreamingSource(cfg v2config.SourceConfig, src source.StreamingSource) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := src.Connect(ctx, v2config.ToStreamingConfig(cfg))
	if err != nil {
		_ = src.Close()
	}
	return err
}

func connectAlsoStreamingSource(cfg v2config.AlsoConfig, src source.StreamingSource) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := src.Connect(ctx, v2config.AlsoToStreamingConfig(cfg))
	if err != nil {
		_ = src.Close()
	}
	return err
}

func isBatchSource(typ string) bool {
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "postgres", "mysql", "redshift", "snowflake", "bigquery", "restapi":
		return true
	default:
		return false
	}
}

func isStreamingSource(typ string) bool {
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "kafka", "webhook", "postgres_cdc":
		return true
	default:
		return false
	}
}
