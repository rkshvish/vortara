package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var stateCmd = &cobra.Command{
	Use:   "state",
	Short: "Inspect and manage entity state",
}

var stateListLimit int

var stateListCmd = &cobra.Command{
	Use:   "list <sync.yaml>",
	Short: "List entity states",
	Args:  cobra.ExactArgs(1),
	RunE:  runStateList,
}

var stateInspectCmd = &cobra.Command{
	Use:   "inspect <sync.yaml> <entity-key>",
	Short: "Inspect state for one entity",
	Args:  cobra.ExactArgs(2),
	RunE:  runStateInspect,
}

var stateResetCmd = &cobra.Command{
	Use:   "reset <sync.yaml> <entity-key>",
	Short: "Reset state for one entity (it will be treated as first_seen on next run)",
	Args:  cobra.ExactArgs(2),
	RunE:  runStateReset,
}

var (
	statePatchSets  []string
	stateExportFile string
)

var statePatchCmd = &cobra.Command{
	Use:   "patch <sync.yaml> <entity-key>",
	Short: "Patch remembered state for one entity (--set key=value)",
	Args:  cobra.ExactArgs(2),
	RunE:  runStatePatch,
}

var stateExportCmd = &cobra.Command{
	Use:   "export <sync.yaml>",
	Short: "Export all entity states as JSONL",
	Args:  cobra.ExactArgs(1),
	RunE:  runStateExport,
}

var stateUnlockCmd = &cobra.Command{
	Use:   "unlock <sync.yaml>",
	Short: "Clear a stale pipeline lock for a sync",
	Args:  cobra.ExactArgs(1),
	RunE:  runStateUnlock,
}

func init() {
	stateListCmd.Flags().IntVar(&stateListLimit, "limit", 20, "Maximum entities to show")
	statePatchCmd.Flags().StringArrayVar(&statePatchSets, "set", nil, "key=value to set in remembered state (repeatable)")
	stateExportCmd.Flags().StringVar(&stateExportFile, "output", "-", "Output file path (- for stdout)")
	stateCmd.AddCommand(stateListCmd)
	stateCmd.AddCommand(stateInspectCmd)
	stateCmd.AddCommand(stateResetCmd)
	stateCmd.AddCommand(statePatchCmd)
	stateCmd.AddCommand(stateExportCmd)
	stateCmd.AddCommand(stateUnlockCmd)
}

func runStateUnlock(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := store.UnlockRun(cmd.Context(), f.Sync.Name); err != nil {
		return err
	}
	fmt.Printf("Pipeline lock cleared for sync %q\n", f.Sync.Name)
	return nil
}

func runStateList(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	destType := f.Sync.Destination.Type
	states, err := store.ListEntityStates(cmd.Context(), f.Sync.Name, destType, stateListLimit, 0)
	if err != nil {
		return err
	}
	if len(states) == 0 {
		fmt.Printf("No entity state recorded yet for sync %q\n", f.Sync.Name)
		return nil
	}
	fmt.Printf("%-30s  %-10s  %-8s  %-8s  %s\n", "ENTITY KEY", "DECISION", "STATUS", "VERSION", "UPDATED")
	for _, es := range states {
		fmt.Printf("%-30s  %-10s  %-8s  %-8d  %s\n",
			truncate(es.EntityKey, 30),
			es.LastDecision,
			es.LastStatus,
			es.Version,
			es.UpdatedAt.Format("2006-01-02 15:04"),
		)
	}
	return nil
}

func runStateInspect(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	destType := f.Sync.Destination.Type
	es, err := store.GetEntityState(cmd.Context(), f.Sync.Name, destType, args[1])
	if err != nil {
		return err
	}
	if es == nil {
		fmt.Printf("Entity %q not found in sync %q\n", args[1], f.Sync.Name)
		return nil
	}

	fmt.Printf("entity_key:   %s\n", es.EntityKey)
	fmt.Printf("version:      %d\n", es.Version)
	fmt.Printf("decision:     %s (%s)\n", es.LastDecision, es.LastStatus)
	fmt.Printf("fingerprint:  %s\n", es.CurrentFingerprint)
	fmt.Printf("prev_fp:      %s\n", es.PreviousFingerprint)
	fmt.Printf("dest_id:      %s\n", es.DestinationID)
	fmt.Printf("missing_runs: %d\n", es.ConsecutiveMissing)
	fmt.Printf("created:      %s\n", es.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Printf("updated:      %s\n", es.UpdatedAt.Format("2006-01-02 15:04:05 UTC"))
	if len(es.RememberedState) > 0 {
		fmt.Println("remembered:")
		for k, v := range es.RememberedState {
			fmt.Printf("  %s: %v\n", k, v)
		}
	}
	return nil
}

func runStateReset(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	destType := f.Sync.Destination.Type
	if err := store.ResetEntityState(cmd.Context(), f.Sync.Name, destType, args[1]); err != nil {
		return err
	}
	fmt.Printf("Reset state for entity %q in sync %q — will be treated as first_seen on next run\n", args[1], f.Sync.Name)
	return nil
}

func runStatePatch(cmd *cobra.Command, args []string) error {
	if len(statePatchSets) == 0 {
		return fmt.Errorf("at least one --set key=value is required")
	}
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	destType := f.Sync.Destination.Type
	es, err := store.GetEntityState(cmd.Context(), f.Sync.Name, destType, args[1])
	if err != nil {
		return err
	}
	if es == nil {
		return fmt.Errorf("entity %q not found in sync %q", args[1], f.Sync.Name)
	}

	if es.RememberedState == nil {
		es.RememberedState = make(map[string]any)
	}
	for _, kv := range statePatchSets {
		idx := strings.IndexByte(kv, '=')
		if idx < 0 {
			return fmt.Errorf("invalid --set value %q: expected key=value", kv)
		}
		es.RememberedState[kv[:idx]] = kv[idx+1:]
	}

	if err := store.SaveEntityState(cmd.Context(), es); err != nil {
		return err
	}
	fmt.Printf("Patched remembered state for entity %q in sync %q\n", args[1], f.Sync.Name)
	return nil
}

func runStateExport(cmd *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}
	store, err := openStateStore(f)
	if err != nil {
		return err
	}
	defer store.Close()

	out := os.Stdout
	if stateExportFile != "-" {
		out, err = os.Create(stateExportFile)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	destType := f.Sync.Destination.Type
	enc := json.NewEncoder(out)
	const pageSize = 500
	offset := 0
	total := 0
	for {
		states, err := store.ListEntityStates(cmd.Context(), f.Sync.Name, destType, pageSize, offset)
		if err != nil {
			return err
		}
		for _, es := range states {
			if err := enc.Encode(es); err != nil {
				return err
			}
			total++
		}
		if len(states) < pageSize {
			break
		}
		offset += pageSize
	}
	if stateExportFile != "-" {
		fmt.Fprintf(os.Stdout, "Exported %d entity states to %s\n", total, stateExportFile)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
