// Package cmd contains the Vortara CLI commands.
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	// Register all connectors via their init() functions.
	_ "github.com/rkshvish/vortaraos/internal/connector/destination"
	_ "github.com/rkshvish/vortaraos/internal/connector/source"
)

// version is stamped at build time via:
//   -ldflags "-X github.com/rkshvish/vortaraos/cmd/vortara/cmd.version=v0.1.0"
var version = "dev"

var rootCmd = &cobra.Command{
	Use:          "vortara",
	Version:      version,
	Short:        "Vortara — Simple ETL + Reverse ETL",
	Long:         "Vortara moves data between sources and destinations.",
	SilenceUsage: true,
}

// Execute runs the root command tree.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(historyCmd)
	rootCmd.AddCommand(watermarkCmd)
	rootCmd.AddCommand(offsetCmd)
	rootCmd.AddCommand(testCmd)
	rootCmd.AddCommand(dlqCmd)
}
