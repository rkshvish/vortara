// Package cmd contains the CLI commands for the sync engine.
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	// Register all connectors via their init() functions.
	_ "github.com/rkshvish/vortara/internal/connector/destination"
	_ "github.com/rkshvish/vortara/internal/connector/source"
)

// version is stamped at build time via:
//
//	-ldflags "-X github.com/rkshvish/vortara/cmd/vortara/cmd.version=v0.1.0"
var version = "dev"

var rootCmd = &cobra.Command{
	Use:          "vortara",
	Version:      version,
	Short:        "Programmable State Reverse ETL",
	Long:         "Define when records create, update, skip, retry, suppress, or trigger actions — with every decision explainable.",
	SilenceUsage: true,
}

// Execute runs the root command tree.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

// isArchiveDestination returns true for destinations that soft-archive records
// rather than hard-delete them, so the CLI can show [would-archive] instead of
// [would-delete] in diff output and explain output.
func isArchiveDestination(destType string) bool {
	switch destType {
	case "hubspot":
		return true
	default:
		return false
	}
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(dryRunCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(replayCmd)
	rootCmd.AddCommand(explainCmd)
	rootCmd.AddCommand(historyCmd)
	rootCmd.AddCommand(stateCmd)
	rootCmd.AddCommand(dlqCmd)
	rootCmd.AddCommand(testCmd)
}
