package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	v2config "github.com/rkshvish/vortara/pkg/config/pipeline"
)

var offsetCmd = &cobra.Command{
	Use:   "offset",
	Short: "Inspect or reset streaming offsets",
}

var offsetGetCmd = &cobra.Command{
	Use:   "get <pipeline.yaml>",
	Short: "Show committed offsets",
	Args:  cobra.ExactArgs(1),
	RunE:  runOffsetGet,
}

var offsetResetCmd = &cobra.Command{
	Use:   "reset <pipeline.yaml>",
	Short: "Reset committed offsets",
	Args:  cobra.ExactArgs(1),
	RunE:  runOffsetReset,
}

func init() {
	offsetCmd.AddCommand(offsetGetCmd)
	offsetCmd.AddCommand(offsetResetCmd)
}

func runOffsetGet(cmd *cobra.Command, args []string) error {
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

	topic := cfg.Source.Topic
	fmt.Printf("Topic: %s\n", topic)

	found := false
	for partition := 0; partition < 10; partition++ {
		offset, err := store.GetOffset(cfg.Name, topic, partition)
		if err != nil {
			return err
		}
		if offset == -1 {
			if !found {
				fmt.Println("No offsets committed yet")
			}
			break
		}
		found = true
		fmt.Printf("Partition %d: offset %d\n", partition, offset)
	}
	return nil
}

func runOffsetReset(cmd *cobra.Command, args []string) error {
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

	topic := cfg.Source.Topic
	for partition := 0; partition < 10; partition++ {
		offset, err := store.GetOffset(cfg.Name, topic, partition)
		if err != nil {
			return err
		}
		if offset == -1 {
			break
		}
		if err := store.SetOffset(cfg.Name, topic, partition, 0); err != nil {
			return err
		}
	}
	fmt.Println("Offsets reset. Next streaming run will replay from beginning.")
	return nil
}

func sourceName(cfg *v2config.PipelineConfig) string {
	if strings.TrimSpace(cfg.Source.Query) != "" {
		return "custom_query"
	}
	if cfg.Source.Table == "" {
		return cfg.Source.Type
	}
	return cfg.Source.Type + "." + cfg.Source.Table
}
