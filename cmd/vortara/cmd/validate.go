package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var validateCmd = &cobra.Command{
	Use:   "validate <sync.yaml>",
	Short: "Validate a sync config file",
	Args:  cobra.ExactArgs(1),
	RunE:  runValidate,
}

func runValidate(_ *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		return err
	}
	if err := synccfg.Validate(f); err != nil {
		fmt.Fprintf(os.Stderr, "Validation error: %v\n", err)
		return err
	}
	fmt.Printf("OK %s is valid (sync: %s)\n", args[0], f.Sync.Name)
	return nil
}
