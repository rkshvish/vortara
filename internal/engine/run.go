package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/rkshvish/vortara/internal/artifacts"
	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/connector/source"
	"github.com/rkshvish/vortara/internal/decision"
	"github.com/rkshvish/vortara/internal/diff"
	"github.com/rkshvish/vortara/internal/fingerprint"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/metrics"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/safety"
	"github.com/rkshvish/vortara/internal/state"
	conncfg "github.com/rkshvish/vortara/pkg/config"
	synccfg "github.com/rkshvish/vortara/pkg/config/sync"
	"github.com/rkshvish/vortara/pkg/row"
)

// pendingDelivery buffers the decision phase result for a single entity,
// so field-ratio safety checks can run before any delivery begins.
type pendingDelivery struct {
	r             row.Row
	entityKey     string
	mapped        map[string]any
	prevFP        string
	curFP         string
	prevPayload   map[string]any
	es            *state.EntityState
	plan          decision.Plan
	isFirstSeen   bool
	changedFields []string
	decEv         *state.DecisionEvent
}

// Run executes a sync from a parsed SyncFile.
// It blocks until the sync completes (or cron is cancelled).
func (e *Engine) Run(ctx context.Context, f *synccfg.SyncFile) error {
	s := f.Sync
	if s.Cron != "" {
		return e.runCron(ctx, f)
	}
	return e.runOnce(ctx, f)
}

func (e *Engine) runCron(ctx context.Context, f *synccfg.SyncFile) error {
	sched, err := cron.ParseStandard(f.Sync.Cron)
	if err != nil {
		return fmt.Errorf("cron: %w", err)
	}
	l := vlogger.FromContext(ctx)
	c := cron.New()
	if _, err := c.AddFunc(f.Sync.Cron, func() {
		l.Info("cron triggered", slog.String("sync", f.Sync.Name))
		_ = e.runOnce(context.WithoutCancel(ctx), f)
	}); err != nil {
		return err
	}
	c.Start()
	defer func() {
		stopCtx := c.Stop()
		<-stopCtx.Done()
	}()
	e.setNextRunAt(sched.Next(time.Now()))
	<-ctx.Done()
	e.setNextRunAt(time.Time{})
	return ctx.Err()
}

func (e *Engine) runOnce(ctx context.Context, f *synccfg.SyncFile) error {
	s := f.Sync
	l := vlogger.FromContext(ctx)
	e.running.Store(true)
	defer e.running.Store(false)

	runStart := time.Now()

	// --- open destination ---
	dest, destName, err := e.openDest(ctx, s)
	if err != nil {
		return err
	}
	if dest != nil {
		defer dest.Close()
	}

	// --- enforce dry_run_required ---
	if s.Safety.DryRunRequired && dest != nil && e.destOverride == nil {
		return fmt.Errorf("safety: dry_run_required is set — use 'dry-run' instead of 'run'")
	}

	// --- open source ---
	src, err := e.openSource(ctx, s)
	if err != nil {
		return err
	}
	defer src.Close()

	// --- acquire pipeline lock ---
	lockOwner := fmt.Sprintf("pid-%d@%d", os.Getpid(), time.Now().UnixNano())
	if err := e.store.LockRun(ctx, s.Name, lockOwner, 10*time.Minute); err != nil {
		return err
	}
	defer func() { _ = e.store.UnlockRun(context.WithoutCancel(ctx), s.Name) }()

	// --- start run log ---
	runID, err := e.store.StartRun(ctx, s.Name, "batch")
	if err != nil {
		return err
	}
	stats := state.RunStats{Status: "success"}
	art := artifacts.New(artifacts.Config{
		BasePath:   s.Artifacts.Path,
		SyncName:   s.Name,
		RunID:      runID,
		MaxSamples: s.Artifacts.MaxSamples,
	})
	defer func() {
		_ = art.Flush(stats.Status)
		_ = e.store.FinishRun(context.WithoutCancel(ctx), runID, stats)
		statsCopy := stats
		e.statsMu.Lock()
		e.lastStats = &statsCopy
		e.statsMu.Unlock()
		if s.Metrics.Path != "" {
			rec := metrics.New(s.Metrics.Path)
			_ = rec.RecordRun(s.Name, statsCopy, time.Since(runStart))
		}
	}()

	if err := e.store.BeginBatch(ctx); err != nil {
		stats.Status = "failed"
		stats.Error = err.Error()
		return err
	}

	// --- build fingerprint field sets ---
	var fpExclude []string
	for _, m := range s.Mapping {
		if m.ExcludeFromFingerprint {
			fpExclude = append(fpExclude, m.DestName())
		}
	}
	fpExclude = append(fpExclude, s.State.Fingerprint.Exclude...)
	fpInclude := buildFPIncludeSet(s.State.Fingerprint.Include)

	// --- build redacted field set (dest field names that should be masked in output) ---
	redactedFields := buildRedactedFieldSet(s.Mapping)

	safetyEval := safety.New(s.Safety)
	dlq, _ := newDLQWriter(s.Name, s.Errors.ResolvedDLQPath())
	defer dlq.Close()

	// seenKeys tracks which entity keys appeared in this extraction pass
	seenKeys := make(map[string]struct{})

	// --- load high watermark from last successful run (incremental extraction) ---
	var prevWatermark time.Time
	if s.Source.Watermark != nil {
		if hist, err := e.store.GetRunHistory(ctx, s.Name, 10); err == nil {
			for _, run := range hist {
				if run.Status == "success" && !run.HighWatermark.IsZero() {
					prevWatermark = run.HighWatermark
					break
				}
			}
		}
	}

	// =========================================================
	// Phase 1: Decision pass — extract rows, evaluate decisions,
	// buffer non-skip results. No delivery happens here.
	// =========================================================
	extractCh := make(chan row.Row, 256)
	extractDone := make(chan error, 1)
	go func() {
		extractDone <- src.Extract(ctx, prevWatermark, time.Time{}, extractCh)
	}()

	var pending []pendingDelivery
	fieldChangeCounts := make(map[string]int)
	var maxWatermark time.Time

	for r := range extractCh {
		if ctx.Err() != nil {
			break
		}
		stats.RowsExtracted++
		if !r.Watermark.IsZero() && r.Watermark.After(maxWatermark) {
			maxWatermark = r.Watermark
		}

		if missing := missingRequiredFields(r.Data, s.Required); len(missing) > 0 {
			l.Warn("required fields missing, skipping row",
				slog.String("sync", s.Name),
				slog.String("row_id", r.ID),
				slog.Any("missing", missing),
			)
			stats.RowsSkipped++
			continue
		}

		mapped := fingerprint.NormalizePayload(applyMapping(r.Data, s.Mapping))
		entityKey := fmt.Sprintf("%v", r.Data[s.Source.EntityKey])
		if entityKey == "" || entityKey == "<nil>" {
			l.Warn("entity_key missing, skipping row",
				slog.String("sync", s.Name),
				slog.String("row_id", r.ID),
			)
			stats.RowsSkipped++
			continue
		}
		seenKeys[entityKey] = struct{}{}

		es, err := e.store.GetEntityState(ctx, s.Name, destName, entityKey)
		if err != nil {
			l.Error("get entity state failed",
				slog.String("sync", s.Name),
				slog.String("entity", entityKey),
				slog.String("error", err.Error()),
			)
			stats.RowsErrored++
			continue
		}
		isFirstSeen := es == nil

		curFP := fingerprint.Of(fpFields(mapped, fpInclude), fpExclude...)
		var prevPayload map[string]any
		var prevFP string
		if !isFirstSeen {
			prevFP = es.CurrentFingerprint
			prevPayload = es.CurrentPayload
		}

		fieldDiff := diff.Compute(prevPayload, mapped)

		in := decision.Input{
			IsFirstSeen:        isFirstSeen,
			FingerprintChanged: curFP != prevFP,
			Diff:               fieldDiff,
			PreviousPayload:    prevPayload,
			CurrentPayload:     mapped,
		}
		if !isFirstSeen {
			in.RememberedState = es.RememberedState
		}
		plan, err := decision.Evaluate(ctx, s.Decisions, in, s.Name, destName, entityKey, e.store)
		if err != nil {
			l.Error("decision evaluation failed",
				slog.String("entity", entityKey),
				slog.String("error", err.Error()),
			)
			stats.RowsErrored++
			continue
		}

		decEv := &state.DecisionEvent{
			SyncName: s.Name, Destination: destName, EntityKey: entityKey,
			RunID: runID, Decision: string(plan.Action),
			TriggeredRules: plan.TriggeredRules, Reasons: plan.Reasons,
		}
		_ = e.store.RecordDecision(ctx, decEv)
		art.RecordDecision(decEv)

		if plan.Skipped() {
			stats.RowsSkipped++
			art.RecordSkip(entityKey)
			continue
		}

		// Collect changed fields for ratio check and artifacts.
		var changedFields []string
		for field := range fieldDiff {
			changedFields = append(changedFields, field)
			fieldChangeCounts[field]++
		}

		pending = append(pending, pendingDelivery{
			r: r, entityKey: entityKey, mapped: mapped,
			prevFP: prevFP, curFP: curFP,
			prevPayload: prevPayload, es: es,
			plan: plan, isFirstSeen: isFirstSeen,
			changedFields: changedFields, decEv: decEv,
		})
	}

	extractErr := <-extractDone

	// Persist watermark and field-change breakdown from Phase 1.
	stats.HighWatermark = maxWatermark
	stats.FieldChangeCounts = fieldChangeCounts

	// =========================================================
	// Safety: field-ratio check before any delivery begins.
	// If violated, rollback and return — no records delivered.
	// =========================================================
	if ratioErr := safetyEval.CheckFieldRatios(fieldChangeCounts, stats.RowsExtracted); ratioErr != nil {
		_ = e.store.RollbackBatch()
		stats.Status = "failed"
		stats.Error = ratioErr.Error()
		return ratioErr
	}

	// =========================================================
	// Safety: approval check — block if require_approval_above or
	// require_approval_for is triggered. Operator can bypass by
	// re-running with --approve-snapshot <hash>.
	// =========================================================
	var pendingCounts safety.RunCounts
	for _, pd := range pending {
		safetyEval.Record(string(pd.plan.Action), &pendingCounts)
	}
	if approvalNeeded, reason := safetyEval.ApprovalRequired(pendingCounts); approvalNeeded {
		hash := computeApprovalHash(s.Name, destName, pendingCounts)
		e.statsMu.RLock()
		provided := e.approvalHash
		e.statsMu.RUnlock()
		if provided != hash {
			_ = e.store.RollbackBatch()
			stats.Status = "failed"
			stats.ApprovalRequired = true
			stats.ApprovalHash = hash
			stats.Error = fmt.Sprintf("approval required: %s — re-run with --approve-snapshot %s", reason, hash)
			return fmt.Errorf("%s", stats.Error)
		}
		l.Info("approval gate bypassed", slog.String("hash", hash))
	}

	// =========================================================
	// Phase 2: Delivery pass — deliver buffered decisions.
	// =========================================================
	var counts safety.RunCounts
	var firstErr error

	for _, pd := range pending {
		if ctx.Err() != nil {
			break
		}

		if err := safetyEval.Allow(string(pd.plan.Action), counts); err != nil {
			l.Warn("safety limit reached",
				slog.String("sync", s.Name),
				slog.String("error", err.Error()),
			)
			stats.RowsSkipped++
			continue
		}

		// Build a deterministic idempotency key that is stable for the same
		// entity+action+fingerprint, so real-run retries are safe and dry-runs
		// never pollute the delivery log of a later real run.
		opKey := deliveryOpKey(s.Name, destName, pd.entityKey, string(pd.plan.Action), pd.curFP)

		var res destination.LoadResult
		if dest != nil {
			deliveryRow := row.Row{
				ID:         opKey,
				PrimaryKey: pd.entityKey,
				Data:       pd.mapped,
				Watermark:  pd.r.Watermark,
			}
			var loadErr error
			res, loadErr = dest.Load(ctx, []row.Row{deliveryRow}, e.store, s.Name, destName)
			stats.RowsLoaded += res.Loaded
			stats.RowsSkipped += res.Skipped
			if loadErr != nil {
				stats.RowsErrored++
				if dlq.Enabled() {
					_ = dlq.Write(deliveryRow, pd.entityKey, loadErr)
				} else if firstErr == nil && strings.ToLower(s.Errors.OnError) != "skip" {
					firstErr = loadErr
				}
				if !e.dryRun {
					es := buildEntityState(
						s.Name, destName, pd.entityKey, pd.prevFP, pd.curFP,
						pd.prevPayload, pd.mapped, pd.es, pd.plan, "failed",
					)
					if pd.es != nil {
						es.DestinationID = pd.es.DestinationID
					}
					_ = e.store.SaveEntityState(ctx, es)
				}
				continue
			}
			// Destination reported the row as already delivered (idempotency skip).
			// Do not overwrite entity state — it was already saved on the prior run.
			if res.Loaded == 0 && res.Skipped > 0 && len(res.Errors) == 0 {
				continue
			}
			for _, re := range res.Errors {
				stats.RowsErrored++
				if dlq.Enabled() {
					_ = dlq.Write(deliveryRow, pd.entityKey, re.Err)
				}
				if !e.dryRun {
					es := buildEntityState(
						s.Name, destName, pd.entityKey, pd.prevFP, pd.curFP,
						pd.prevPayload, pd.mapped, pd.es, pd.plan, "failed",
					)
					if pd.es != nil {
						es.DestinationID = pd.es.DestinationID
					}
					_ = e.store.SaveEntityState(ctx, es)
				}
			}
			// If all rows errored, skip the success state write below.
			if len(res.Errors) > 0 && res.Loaded == 0 {
				continue
			}
		} else {
			stats.RowsLoaded++
		}

		// Persist entity state and rule firings only for real (non-dry-run) runs.
		// Dry-run reads real state but must never write it, so subsequent real runs
		// see the correct baseline and are not skipped by stale fingerprints.
		if !e.dryRun {
			// Prefer the destination_id returned by this delivery (e.g. HubSpot
			// contact ID from create/search); fall back to whatever was stored before.
			destID := ""
			if dest != nil && res.DestinationIDs != nil {
				destID = res.DestinationIDs[opKey]
			}
			if destID == "" && pd.es != nil {
				destID = pd.es.DestinationID
			}
			newState := buildEntityState(
				s.Name, destName, pd.entityKey, pd.prevFP, pd.curFP,
				pd.prevPayload, pd.mapped, pd.es, pd.plan, "success",
			)
			newState.DestinationID = destID
			if err := e.store.SaveEntityState(ctx, newState); err != nil {
				l.Warn("save entity state failed", slog.String("entity", pd.entityKey), slog.String("error", err.Error()))
			}

			for _, ruleName := range pd.plan.TriggeredRules {
				for _, rc := range s.Decisions.Rules {
					if rc.Name == ruleName && rc.Once {
						_ = e.store.MarkRuleFired(ctx, s.Name, destName, pd.entityKey, ruleName)
					}
				}
			}
		}

		// Record artifact sample (redacting masked fields before storing).
		sampleData := redactPayload(pd.mapped, redactedFields)
		if pd.isFirstSeen {
			art.RecordCreate(pd.entityKey, sampleData)
		} else {
			art.RecordUpdate(pd.entityKey, sampleData, pd.changedFields)
		}

		safetyEval.Record(string(pd.plan.Action), &counts)
	}

	// Persist Phase 2 delivery breakdown.
	stats.Creates = counts.Creates
	stats.Updates = counts.Updates
	stats.Deletes = counts.Deletes

	// --- process entities missing from this extraction pass ---
	if s.OnMissingFrom.Action != "" && ctx.Err() == nil {
		if missingErr := e.processMissingEntities(ctx, s, destName, runID, dest, seenKeys, &stats, dlq); missingErr != nil {
			l.Warn("missing-entity processing error", slog.String("error", missingErr.Error()))
		}
	}

	commitErr := e.store.CommitBatch(context.WithoutCancel(ctx))
	if commitErr != nil {
		_ = e.store.RollbackBatch()
		stats.Status = "failed"
		stats.Error = commitErr.Error()
		return commitErr
	}

	if extractErr != nil && !errors.Is(extractErr, context.Canceled) && !errors.Is(extractErr, context.DeadlineExceeded) {
		if firstErr == nil {
			firstErr = extractErr
		}
	}
	if ctx.Err() != nil {
		stats.Status = "timeout"
	} else if firstErr != nil {
		stats.Status = "failed"
		stats.Error = firstErr.Error()
	}
	if s.Errors.FailureWebhookURL != "" && firstErr != nil {
		sendFailureAlert(context.WithoutCancel(ctx), s.Name, s.Errors.FailureWebhookURL, firstErr)
	}
	return firstErr
}

// processMissingEntities iterates over all known entity states for this sync+dest,
// increments ConsecutiveMissing for any entity not seen in the current run, and
// applies the configured on_missing_from_source action when the threshold is met.
func (e *Engine) processMissingEntities(
	ctx context.Context,
	s synccfg.SyncSpec,
	destName string,
	runID int64,
	dest destination.Destination,
	seenKeys map[string]struct{},
	stats *state.RunStats,
	dlq *dlqWriter,
) error {
	l := vlogger.FromContext(ctx)
	threshold := s.OnMissingFrom.AfterMissingRuns
	if threshold <= 0 {
		threshold = 1
	}

	const pageSize = 500
	offset := 0
	for {
		page, err := e.store.ListEntityStates(ctx, s.Name, destName, pageSize, offset)
		if err != nil {
			return fmt.Errorf("list entity states: %w", err)
		}
		for _, es := range page {
			if _, seen := seenKeys[es.EntityKey]; seen {
				continue
			}
			es.ConsecutiveMissing++
			if es.ConsecutiveMissing < threshold {
				es.UpdatedAt = time.Now().UTC()
				_ = e.store.SaveEntityState(ctx, es)
				continue
			}

			action := strings.ToLower(s.OnMissingFrom.Action)
			switch action {
			case "delete":
				if dest != nil {
					deliveryRow := row.Row{
						ID:         es.EntityKey,
						PrimaryKey: es.EntityKey,
						Data:       es.CurrentPayload,
					}
					res, loadErr := dest.Load(ctx, []row.Row{deliveryRow}, e.store, s.Name, destName)
					stats.RowsLoaded += res.Loaded
					if loadErr != nil {
						stats.RowsErrored++
						if dlq.Enabled() {
							_ = dlq.Write(deliveryRow, es.EntityKey, loadErr)
						}
					}
				}
				_ = e.store.RecordDecision(ctx, &state.DecisionEvent{
					SyncName: s.Name, Destination: destName, EntityKey: es.EntityKey,
					RunID: runID, Decision: "delete", Reasons: []string{"missing from source"},
				})
				es.LastDecision = "delete"
				es.ConsecutiveMissing = 0
			case "clear_fields":
				if dest != nil && len(s.OnMissingFrom.Fields) > 0 {
					cleared := make(map[string]any, len(es.CurrentPayload))
					for k, v := range es.CurrentPayload {
						cleared[k] = v
					}
					for _, f := range s.OnMissingFrom.Fields {
						cleared[f] = nil
					}
					deliveryRow := row.Row{
						ID:         es.EntityKey,
						PrimaryKey: es.EntityKey,
						Data:       cleared,
					}
					res, loadErr := dest.Load(ctx, []row.Row{deliveryRow}, e.store, s.Name, destName)
					stats.RowsLoaded += res.Loaded
					if loadErr != nil {
						stats.RowsErrored++
						if dlq.Enabled() {
							_ = dlq.Write(deliveryRow, es.EntityKey, loadErr)
						}
					}
				}
				_ = e.store.RecordDecision(ctx, &state.DecisionEvent{
					SyncName: s.Name, Destination: destName, EntityKey: es.EntityKey,
					RunID: runID, Decision: "clear_fields", Reasons: []string{"missing from source"},
				})
				es.LastDecision = "clear_fields"
			default:
				// "skip" or unknown: just save incremented ConsecutiveMissing
			}

			l.Info("missing entity processed",
				slog.String("sync", s.Name),
				slog.String("entity", es.EntityKey),
				slog.String("action", action),
				slog.Int("consecutive_missing", es.ConsecutiveMissing),
			)
			es.UpdatedAt = time.Now().UTC()
			_ = e.store.SaveEntityState(ctx, es)
		}
		if len(page) < pageSize {
			break
		}
		offset += pageSize
	}
	return nil
}

// buildFPIncludeSet returns a set of field names for fingerprint inclusion,
// or nil if no include list is configured (meaning all fields are included).
func buildFPIncludeSet(include []string) map[string]struct{} {
	if len(include) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(include))
	for _, f := range include {
		s[f] = struct{}{}
	}
	return s
}

// fpFields returns a filtered copy of data containing only include-listed fields.
// If include is nil, the original map is returned unchanged.
func fpFields(data map[string]any, include map[string]struct{}) map[string]any {
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

// missingRequiredFields returns the names of any required fields absent from data.
func missingRequiredFields(data map[string]any, required []string) []string {
	var missing []string
	for _, f := range required {
		if v, ok := data[f]; !ok || v == nil {
			missing = append(missing, f)
		}
	}
	return missing
}

// buildRedactedFieldSet returns the set of dest field names marked redacted:true in mapping.
func buildRedactedFieldSet(mapping []synccfg.MappingEntry) map[string]struct{} {
	var out map[string]struct{}
	for _, m := range mapping {
		if m.Redacted {
			if out == nil {
				out = make(map[string]struct{})
			}
			out[m.DestName()] = struct{}{}
		}
	}
	return out
}

// redactPayload returns a copy of data with redacted field values replaced by "[REDACTED]".
// If redacted is nil or empty, the original map is returned unchanged.
func redactPayload(data map[string]any, redacted map[string]struct{}) map[string]any {
	if len(redacted) == 0 {
		return data
	}
	out := make(map[string]any, len(data))
	for k, v := range data {
		if _, masked := redacted[k]; masked {
			out[k] = "[REDACTED]"
		} else {
			out[k] = v
		}
	}
	return out
}

// applyMapping projects source data onto destination field names.
func applyMapping(data map[string]any, mapping []synccfg.MappingEntry) map[string]any {
	if len(mapping) == 0 {
		// No mapping defined: pass all fields through as-is.
		out := make(map[string]any, len(data))
		for k, v := range data {
			out[k] = v
		}
		return out
	}
	out := make(map[string]any, len(mapping))
	for _, m := range mapping {
		if v, ok := data[m.Source]; ok {
			out[m.DestName()] = v
		}
	}
	return out
}

// buildEntityState constructs the new EntityState after a delivery attempt.
func buildEntityState(
	syncName, destName, entityKey string,
	prevFP, curFP string,
	prevPayload, curPayload map[string]any,
	prev *state.EntityState,
	plan decision.Plan,
	status string,
) *state.EntityState {
	remembered := make(map[string]any)
	if prev != nil && prev.RememberedState != nil {
		for k, v := range prev.RememberedState {
			remembered[k] = v
		}
	}
	for k, v := range plan.Remember {
		remembered[k] = v
	}
	version := 1
	if prev != nil {
		version = prev.Version + 1
	}
	return &state.EntityState{
		SyncName:            syncName,
		Destination:         destName,
		EntityKey:           entityKey,
		CurrentFingerprint:  curFP,
		PreviousFingerprint: prevFP,
		CurrentPayload:      curPayload,
		PreviousPayload:     prevPayload,
		RememberedState:     remembered,
		LastDecision:        string(plan.Action),
		LastStatus:          status,
		Version:             version,
	}
}

// computeApprovalHash returns a short deterministic hash for an approval snapshot.
// The hash encodes the sync name, destination, and pending action counts so the
// operator can verify what they're approving.
// deliveryOpKey builds a stable, content-addressed idempotency key for a single
// planned delivery. The key encodes every dimension that distinguishes one
// operation from another: the sync, the destination, the entity, the action,
// and the content fingerprint. It is deterministic across runs so that:
//   - a real run that retries after a transient error recognises the previous
//     successful delivery and skips re-sending to the destination;
//   - a dry-run cannot "use up" the key that a later real run needs.
func deliveryOpKey(syncName, destName, entityKey, action, fingerprint string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s:%s:%s:%s:%s", syncName, destName, entityKey, action, fingerprint)
	return hex.EncodeToString(h.Sum(nil))
}

func computeApprovalHash(syncName, destName string, counts safety.RunCounts) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s:%s:c=%d:u=%d:d=%d", syncName, destName, counts.Creates, counts.Updates, counts.Deletes)
	return "appr-" + hex.EncodeToString(h.Sum(nil))[:8]
}

// openDest opens the destination connector from config.
func (e *Engine) openDest(ctx context.Context, s synccfg.SyncSpec) (destination.Destination, string, error) {
	if e.destOverride != nil {
		return e.destOverride, s.Destination.Type, nil
	}
	raw, err := registry.GetDestination(s.Destination.Type)
	if err != nil {
		return nil, "", err
	}
	dest, ok := raw.(destination.Destination)
	if !ok {
		return nil, "", fmt.Errorf("destination %q invalid type %T", s.Destination.Type, raw)
	}
	cfg := syncDestToConnCfg(s.Destination)
	if err := dest.Connect(ctx, cfg); err != nil {
		return nil, "", fmt.Errorf("destination connect: %w", err)
	}
	return dest, s.Destination.Type, nil
}

// openSource opens the source connector from config and returns a BatchSource.
func (e *Engine) openSource(ctx context.Context, s synccfg.SyncSpec) (batchSourceCloser, error) {
	raw, err := registry.GetBatchSource(s.Source.Type)
	if err != nil {
		return nil, err
	}
	src, ok := raw.(source.BatchSource)
	if !ok {
		return nil, fmt.Errorf("source %q invalid type %T", s.Source.Type, raw)
	}
	cfg := syncSrcToConnCfg(s.Source)
	if err := src.Connect(ctx, cfg); err != nil {
		return nil, fmt.Errorf("source connect: %w", err)
	}
	return src, nil
}

// batchSourceCloser combines BatchSource with Close.
type batchSourceCloser interface {
	source.BatchSource
	Close() error
}

// syncSrcToConnCfg converts a SyncConfig source to the connector config type.
func syncSrcToConnCfg(s synccfg.SourceConfig) conncfg.SourceConfig {
	cfg := conncfg.SourceConfig{
		Type:       s.Type,
		Connection: s.URL,
		URL:        s.URL,
		Table:      s.Table,
		Query:      s.Query,
		BatchSize:  s.BatchSize,
		Options:    map[string]string{},
	}
	if s.Watermark != nil {
		cfg.WatermarkColumn = s.Watermark.Column
	}
	if s.Auth != nil {
		cfg.Auth = conncfg.AuthConfig{
			Type:         s.Auth.Type,
			Token:        s.Auth.Token,
			ClientID:     s.Auth.ClientID,
			ClientSecret: s.Auth.ClientSecret,
			TokenURL:     s.Auth.TokenURL,
			Username:     s.Auth.Username,
			Password:     s.Auth.Password,
		}
	}
	return cfg
}

// syncDestToConnCfg converts a SyncConfig destination to the connector config type.
func syncDestToConnCfg(d synccfg.DestinationConfig) conncfg.DestinationConfig {
	cfg := conncfg.DestinationConfig{
		Type:       d.Type,
		Connection: d.URL,
		URL:        d.URL,
		MatchOn:    strings.Join(d.MatchOn, ","),
		Strategy:   "upsert",
		Options: map[string]string{
			"object": d.Object,
			"table":  d.Table,
		},
	}
	for k, v := range d.Options {
		cfg.Options[k] = v
	}
	if d.Auth != nil {
		cfg.Auth = conncfg.AuthConfig{
			Type:         d.Auth.Type,
			Token:        d.Auth.Token,
			ClientID:     d.Auth.ClientID,
			ClientSecret: d.Auth.ClientSecret,
			TokenURL:     d.Auth.TokenURL,
			Username:     d.Auth.Username,
			Password:     d.Auth.Password,
		}
	}
	return cfg
}
