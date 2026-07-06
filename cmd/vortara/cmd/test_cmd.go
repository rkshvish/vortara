package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rkshvish/vortara/internal/decision"
	"github.com/rkshvish/vortara/internal/diff"
	"github.com/rkshvish/vortara/internal/fingerprint"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
)

var testStateTests bool

var testCmd = &cobra.Command{
	Use:   "test <sync.yaml>",
	Short: "Run inline state unit tests defined in a sync YAML",
	Args:  cobra.ExactArgs(1),
	RunE:  runTestCmd,
}

func init() {
	testCmd.Flags().BoolVar(&testStateTests, "state-tests", false, "Run state unit tests defined in the sync YAML under tests:")
}

func runTestCmd(_ *cobra.Command, args []string) error {
	if !testStateTests {
		return fmt.Errorf("specify --state-tests to run inline state unit tests")
	}

	f, err := synccfg.Load(args[0])
	if err != nil {
		return err
	}

	s := f.Sync
	if len(s.Tests) == 0 {
		fmt.Printf("No state tests defined in %s\n", args[0])
		return nil
	}

	// Build fingerprint helpers matching the engine's logic.
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
		curFP := fingerprint.Of(testFPFields(tc.Current, fpInclude), fpExclude...)
		prevFP := ""
		if tc.Previous != nil {
			prevFP = fingerprint.Of(testFPFields(tc.Previous, fpInclude), fpExclude...)
		}

		fieldDiff := diff.Compute(tc.Previous, tc.Current)

		in := decision.Input{
			IsFirstSeen:        tc.Previous == nil,
			FingerprintChanged: curFP != prevFP,
			Diff:               fieldDiff,
			PreviousPayload:    tc.Previous,
			CurrentPayload:     tc.Current,
		}

		// nil checker: once:true rules are always evaluated in tests (no prior firing state).
		plan, evalErr := decision.Evaluate(context.Background(), s.Decisions, in, s.Name, "test", "test-entity", nil)
		if evalErr != nil {
			fmt.Printf("FAIL  %s — evaluation error: %v\n", tc.Name, evalErr)
			failed++
			continue
		}

		var failures []string
		if string(plan.Action) != tc.Expect.Decision {
			failures = append(failures, fmt.Sprintf("decision: want %q got %q", tc.Expect.Decision, plan.Action))
		}
		if len(tc.Expect.TriggeredRules) > 0 {
			if !testSlicesEqual(plan.TriggeredRules, tc.Expect.TriggeredRules) {
				failures = append(failures, fmt.Sprintf("triggered_rules: want %v got %v",
					tc.Expect.TriggeredRules, plan.TriggeredRules))
			}
		}

		if len(failures) == 0 {
			fmt.Printf("PASS  %s\n", tc.Name)
			passed++
		} else {
			fmt.Printf("FAIL  %s — %s\n", tc.Name, strings.Join(failures, "; "))
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
