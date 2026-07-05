package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	v2config "github.com/rkshvish/vortaraos/pkg/config/v2"
)

var validateCmd = &cobra.Command{
	Use:   "validate <pipeline.yaml>",
	Short: "Validate a pipeline config file",
	Args:  cobra.ExactArgs(1),
	RunE:  runValidate,
}

func runValidate(cmd *cobra.Command, args []string) error {
	path := args[0]
	cfg, err := v2config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		return err
	}
	if err := v2config.Validate(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Validation errors:\n%v\n", err)
		return err
	}
	fmt.Printf("✓ %s is valid\n", path)
	return nil
}
