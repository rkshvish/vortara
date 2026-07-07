package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/decision"
	diffpkg "github.com/rkshvish/vortara/internal/diff"
	"github.com/rkshvish/vortara/internal/fingerprint"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var testCmd = &cobra.Command{
	Use:   "test <sync.yaml>",
	Short: "Run inline state unit tests defined in a sync YAML (under tests:)",
	Args:  cobra.ExactArgs(1),
	RunE:  runTestCmd,
}

func runTestCmd(_ *cobra.Command, args []string) error {
	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}

	s := f.Sync
	if len(s.Tests) == 0 {
		fmt.Printf("No state tests defined in %s\n", args[0])
		fmt.Println("Add a 'tests:' section to your sync YAML. Example:")
		fmt.Println()
		fmt.Println("  tests:")
		fmt.Println("    - name: first_seen_creates")
		fmt.Println("      previous: null")
		fmt.Println("      current:")
		fmt.Println("        id: user_001")
		fmt.Println("        email: alice@example.com")
		fmt.Println("      expect:")
		fmt.Println("        decision: create")
		return nil
	}

	// Fingerprint config matching the engine.
	fpInclude := testBuildFPInclude(s.State.Fingerprint.Include)
	var fpExclude []string
	for _, m := range s.Mapping {
		if m.ExcludeFromFingerprint {
			fpExclude = append(fpExclude, m.DestName())
		}
	}
	fpExclude = append(fpExclude, s.State.Fingerprint.Exclude...)

	passed, failed := 0, 0
	for _, tc := range s.Tests {
		// Normalize payloads (same as engine) so timestamp types don't cause false failures.
		current := fingerprint.NormalizePayload(tc.Current)
		var previous map[string]any
		if tc.Previous != nil {
			previous = fingerprint.NormalizePayload(tc.Previous)
		}

		curFP := fingerprint.Of(testFPFields(current, fpInclude), fpExclude...)
		prevFP := ""
		if previous != nil {
			prevFP = fingerprint.Of(testFPFields(previous, fpInclude), fpExclude...)
		}

		fieldDiff := diffpkg.Compute(previous, current)

		in := decision.Input{
			IsFirstSeen:        previous == nil,
			FingerprintChanged: curFP != prevFP,
			Diff:               fieldDiff,
			PreviousPayload:    previous,
			CurrentPayload:     current,
		}

		// once:true rules are always unevaluated-history in tests (pass nil checker).
		plan, evalErr := decision.Evaluate(context.Background(), s.Decisions, in, s.Name, "test", "test-entity", nil)
		if evalErr != nil {
			fmt.Printf("FAIL  %-40s  evaluation error: %v\n", tc.Name, evalErr)
			failed++
			continue
		}

		var failures []string

		// Decision assertion.
		if string(plan.Action) != tc.Expect.Decision {
			failures = append(failures, fmt.Sprintf("decision: want %q got %q", tc.Expect.Decision, plan.Action))
		}

		// Triggered rules assertion.
		if len(tc.Expect.TriggeredRules) > 0 {
			if !testSlicesEqual(plan.TriggeredRules, tc.Expect.TriggeredRules) {
				failures = append(failures, fmt.Sprintf("triggered_rules: want %v got %v",
					tc.Expect.TriggeredRules, plan.TriggeredRules))
			}
		}

		// Changed fields assertion: every listed field must appear in the diff.
		// This is a "must contain" check — other fields may also differ.
		if len(tc.Expect.ChangedFields) > 0 {
			var missing []string
			for _, f := range tc.Expect.ChangedFields {
				if !fieldDiff.Contains(f) {
					missing = append(missing, f)
				}
			}
			if len(missing) > 0 {
				sort.Strings(missing)
				failures = append(failures, fmt.Sprintf("changed_fields: expected %v not in diff (diff has: %v)",
					missing, diffKeys(fieldDiff)))
			}
		}

		if len(failures) == 0 {
			fmt.Printf("PASS  %s\n", tc.Name)
			passed++
		} else {
			fmt.Printf("FAIL  %-40s  %s\n", tc.Name, strings.Join(failures, "; "))
			failed++
		}
	}

	fmt.Printf("\n%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		return fmt.Errorf("%d test(s) failed", failed)
	}
	return nil
}

func testFPFields(data map[string]any, include map[string]struct{}) map[string]any {
	if include == nil {
		return data
	}
	out := make(map[string]any, len(include))
	for k, v := range data {
		if _, ok := include[k]; ok {
			out[k] = v
		}
	}
	return out
}

func testBuildFPInclude(include []string) map[string]struct{} {
	if len(include) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(include))
	for _, f := range include {
		s[f] = struct{}{}
	}
	return s
}

func diffKeys(d diffpkg.Result) []string {
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func testSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
