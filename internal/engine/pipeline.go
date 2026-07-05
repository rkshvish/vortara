package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/connector/source"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/steps"
	"github.com/rkshvish/vortara/internal/strategy"
	v2cfg "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

var batchSourceTypes = map[string]bool{
	"postgres":  true,
	"mysql":     true,
	"redshift":  true,
	"snowflake": true,
	"bigquery":  true,
	"restapi":   true,
}

var streamingSourceTypes = map[string]bool{
	"kafka":        true,
	"webhook":      true,
	"postgres_cdc": true,
}

// Run starts a pipeline from parsed v2 config.
func (e *Engine) Run(ctx context.Context, cfg *v2cfg.PipelineConfig) error {
	if e == nil {
		return errors.New("engine: nil engine")
	}
	if cfg == nil {
		return errors.New("engine: nil config")
	}
	if e.store == nil {
		return errors.New("engine: nil state store")
	}
	e.cfg = cfg
	e.running.Store(true)
	defer e.running.Store(false)
	ctx = vlogger.WithContext(ctx, vlogger.WithPipeline(vlogger.FromContext(ctx), cfg.Name))

	proc, err := steps.New(cfg.Transform)
	if err != nil {
		return err
	}
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		return err
	}
	dests, err := e.buildDestinations(cfg.Destinations)
	if err != nil {
		return err
	}
	defer closeDestinations(dests)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg   sync.WaitGroup
		errs = make(chan error, 4)
	)
	start := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				select {
				case errs <- err:
				default:
				}
				cancel()
			}
		}()
	}

	if batchSourceTypes[strings.ToLower(strings.TrimSpace(cfg.Source.Type))] {
		batchSrc, srcName, err := e.buildBatchSource(cfg.Source)
		if err != nil {
			return err
		}
		start(func() error {
			defer batchSrc.Close()
			return e.runBatch(runCtx, cfg, batchSrc, proc, rt, dests, srcName)
		})
	}

	if streamingSourceTypes[strings.ToLower(strings.TrimSpace(cfg.Source.Type))] {
		streamSrc, srcName, err := e.buildStreamingSource(cfg.Source)
		if err != nil {
			return err
		}
		start(func() error {
			defer streamSrc.Close()
			return e.runStreaming(runCtx, cfg, streamSrc, proc, rt, dests, srcName)
		})
	}

	if cfg.Also != nil {
		alsoType := strings.ToLower(strings.TrimSpace(cfg.Also.Type))
		if !streamingSourceTypes[alsoType] {
			return fmt.Errorf("also.type %q is not a streaming source", cfg.Also.Type)
		}
		streamSrc, srcName, err := e.buildAlsoStreamingSource(*cfg.Also)
		if err != nil {
			return err
		}
		start(func() error {
			defer streamSrc.Close()
			return e.runStreaming(runCtx, cfg, streamSrc, proc, rt, dests, srcName)
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case err := <-errs:
		cancel()
		<-done
		return err
	case <-ctx.Done():
		cancel()
		<-done
		return ctx.Err()
	}
}

// RunOnce executes a single batch or streaming pass without cron scheduling.
func (e *Engine) RunOnce(ctx context.Context, cfg *v2cfg.PipelineConfig) error {
	if e == nil {
		return errors.New("engine: nil engine")
	}
	if cfg == nil {
		return errors.New("engine: nil config")
	}
	if e.store == nil {
		return errors.New("engine: nil state store")
	}
	e.cfg = cfg
	e.running.Store(true)
	defer e.running.Store(false)
	ctx = vlogger.WithContext(ctx, vlogger.FromContext(ctx))

	proc, err := steps.New(cfg.Transform)
	if err != nil {
		return err
	}
	rt, err := router.New(cfg.Destinations)
	if err != nil {
		return err
	}
	dests, err := e.buildDestinations(cfg.Destinations)
	if err != nil {
		return err
	}
	defer closeDestinations(dests)

	if batchSourceTypes[strings.ToLower(strings.TrimSpace(cfg.Source.Type))] {
		batchSrc, srcName, err := e.buildBatchSource(cfg.Source)
		if err != nil {
			return err
		}
		defer batchSrc.Close()
		return e.runBatchOnce(ctx, cfg, batchSrc, proc, rt, dests, srcName)
	}

	if streamingSourceTypes[strings.ToLower(strings.TrimSpace(cfg.Source.Type))] {
		streamSrc, srcName, err := e.buildStreamingSource(cfg.Source)
		if err != nil {
			return err
		}
		defer streamSrc.Close()
		return e.runStreaming(ctx, cfg, streamSrc, proc, rt, dests, srcName)
	}

	return fmt.Errorf("engine: unknown source type %q", cfg.Source.Type)
}

func (e *Engine) buildBatchSource(cfg v2cfg.SourceConfig) (source.BatchSource, string, error) {
	raw, err := registry.GetBatchSource(cfg.Type)
	if err != nil {
		return nil, "", err
	}
	batchSrc, ok := raw.(source.BatchSource)
	if !ok {
		return nil, "", fmt.Errorf("registry batch source %q has invalid type %T", cfg.Type, raw)
	}
	if err := batchSrc.Connect(context.Background(), v2cfg.ToSourceConfig(cfg)); err != nil {
		_ = batchSrc.Close()
		return nil, "", err
	}
	return batchSrc, batchSourceName(cfg), nil
}

func (e *Engine) buildStreamingSource(cfg v2cfg.SourceConfig) (source.StreamingSource, string, error) {
	raw, err := registry.GetStreamingSource(cfg.Type)
	if err != nil {
		return nil, "", err
	}
	streamSrc, ok := raw.(source.StreamingSource)
	if !ok {
		return nil, "", fmt.Errorf("registry streaming source %q has invalid type %T", cfg.Type, raw)
	}
	if err := streamSrc.Connect(context.Background(), v2cfg.ToStreamingConfig(cfg)); err != nil {
		_ = streamSrc.Close()
		return nil, "", err
	}
	return streamSrc, streamingSourceName(cfg.Type, cfg.Topic, cfg.Path), nil
}

func (e *Engine) buildAlsoStreamingSource(cfg v2cfg.AlsoConfig) (source.StreamingSource, string, error) {
	raw, err := registry.GetStreamingSource(cfg.Type)
	if err != nil {
		return nil, "", err
	}
	streamSrc, ok := raw.(source.StreamingSource)
	if !ok {
		return nil, "", fmt.Errorf("registry streaming source %q has invalid type %T", cfg.Type, raw)
	}
	if err := streamSrc.Connect(context.Background(), v2cfg.AlsoToStreamingConfig(cfg)); err != nil {
		_ = streamSrc.Close()
		return nil, "", err
	}
	return streamSrc, streamingSourceName(cfg.Type, cfg.Topic, cfg.Path), nil
}

func (e *Engine) buildDestinations(cfgs []v2cfg.DestinationConfig) ([]destination.Destination, error) {
	dests := make([]destination.Destination, len(cfgs))
	for i, cfg := range cfgs {
		if override, ok := e.destinations[strconv.Itoa(i)]; ok && override != nil {
			dests[i] = override
			continue
		}
		raw, err := registry.GetDestination(cfg.Type)
		if err != nil {
			closeDestinations(dests[:i])
			return nil, err
		}
		dest, ok := raw.(destination.Destination)
		if !ok {
			closeDestinations(dests[:i])
			return nil, fmt.Errorf("registry destination %q has invalid type %T", cfg.Type, raw)
		}
		if err := dest.Connect(context.Background(), v2cfg.ToDestinationConfig(cfg)); err != nil {
			_ = dest.Close()
			closeDestinations(dests[:i])
			return nil, fmt.Errorf("destinations[%d] connect: %w", i, err)
		}
		dests[i] = dest
	}
	return dests, nil
}

type dispatchSummary struct {
	Loaded  int
	Skipped int
	Errors  int
}

// dispatchRetryAttempts is the total number of delivery attempts per row
// when settings.on_error is "retry".
const dispatchRetryAttempts = 3

// loadBatch delivers a batch of rows to one destination, applying the
// per-destination strategy context and the on_error: retry policy.
func (e *Engine) loadBatch(ctx context.Context, cfg *v2cfg.PipelineConfig, dests []destination.Destination, idx int, batch []row.Row) (destination.LoadResult, error) {
	dest := dests[idx]
	if override, ok := e.destinations[strconv.Itoa(idx)]; ok && override != nil {
		dest = override
	}
	if dest == nil {
		return destination.LoadResult{}, nil
	}
	loadCtx := ctx
	if cfg != nil && idx < len(cfg.Destinations) {
		loadCtx = strategy.WithStrategyName(loadCtx, cfg.Destinations[idx].Strategy)
	}
	destName := strconv.Itoa(idx)

	res, err := dest.Load(loadCtx, batch, e.store, cfg.Name, destName)
	if err == nil || cfg == nil || strings.ToLower(strings.TrimSpace(cfg.Settings.OnError)) != "retry" {
		return res, err
	}
	for attempt := 1; attempt < dispatchRetryAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return res, err
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
		res, err = dest.Load(loadCtx, batch, e.store, cfg.Name, destName)
		if err == nil {
			return res, nil
		}
	}
	return res, err
}

// dispatchWithPolicy dispatches a row, applying the on_error: retry policy.
// Retries use linear backoff (500ms, 1s) between attempts.
func (e *Engine) dispatchWithPolicy(ctx context.Context, cfg *v2cfg.PipelineConfig, router *router.Router, dests []destination.Destination, r row.Row) (dispatchSummary, bool, error) {
	summary, ok, err := e.dispatchRow(ctx, cfg, router, dests, r)
	if err == nil || cfg == nil || strings.ToLower(strings.TrimSpace(cfg.Settings.OnError)) != "retry" {
		return summary, ok, err
	}
	for attempt := 1; attempt < dispatchRetryAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return summary, ok, err
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
		summary, ok, err = e.dispatchRow(ctx, cfg, router, dests, r)
		if err == nil {
			return summary, ok, nil
		}
	}
	return summary, ok, err
}

func (e *Engine) dispatchRow(ctx context.Context, cfg *v2cfg.PipelineConfig, router *router.Router, dests []destination.Destination, r row.Row) (dispatchSummary, bool, error) {
	if router == nil {
		return dispatchSummary{}, true, nil
	}
	indices := router.Route(r)
	if len(indices) == 0 {
		return dispatchSummary{}, true, nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(indices))
	var mu sync.Mutex
	summary := dispatchSummary{}
	for _, idx := range indices {
		if idx < 0 || idx >= len(dests) {
			continue
		}
		idx := idx
		wg.Add(1)
		go func() {
			defer wg.Done()
			dest := dests[idx]
			if override, ok := e.destinations[strconv.Itoa(idx)]; ok && override != nil {
				dest = override
			}
			if dest == nil {
				return
			}
			destName := strconv.Itoa(idx)
			loadCtx := ctx
			if idx < len(cfg.Destinations) {
				loadCtx = strategy.WithStrategyName(loadCtx, cfg.Destinations[idx].Strategy)
			}
			res, err := dest.Load(loadCtx, []row.Row{r}, e.store, cfg.Name, destName)
			mu.Lock()
			summary.Loaded += res.Loaded
			summary.Skipped += res.Skipped
			summary.Errors += len(res.Errors)
			mu.Unlock()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if len(res.Errors) > 0 {
				select {
				case errCh <- res.Errors[0].Err:
				default:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return summary, false, err
	default:
		return summary, true, nil
	}
}

func batchBufferSize(cfg *v2cfg.PipelineConfig) int {
	if cfg == nil || cfg.Settings.Concurrency.BatchSize <= 0 {
		return 1000
	}
	return cfg.Settings.Concurrency.BatchSize
}

func batchSourceName(cfg v2cfg.SourceConfig) string {
	if strings.TrimSpace(cfg.Query) != "" {
		return "custom_query"
	}
	if strings.TrimSpace(cfg.Table) == "" {
		return cfg.Type
	}
	return cfg.Type + "." + cfg.Table
}

func streamingSourceName(typ, topic, path string) string {
	if strings.TrimSpace(topic) != "" {
		return typ + "." + topic
	}
	if strings.TrimSpace(path) != "" {
		return typ + "." + strings.TrimPrefix(path, "/")
	}
	return typ
}

func closeDestinations(dests []destination.Destination) {
	for _, d := range dests {
		if d != nil {
			_ = d.Close()
		}
	}
}
