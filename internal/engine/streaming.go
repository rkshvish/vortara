package engine

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rkshvish/vortara/internal/connector/destination"
	"github.com/rkshvish/vortara/internal/connector/source"
	vlogger "github.com/rkshvish/vortara/internal/logger"
	"github.com/rkshvish/vortara/internal/router"
	"github.com/rkshvish/vortara/internal/steps"
	v2cfg "github.com/rkshvish/vortara/pkg/config/pipeline"
	"github.com/rkshvish/vortara/pkg/row"
)

func (e *Engine) runStreaming(ctx context.Context, cfg *v2cfg.PipelineConfig, src source.StreamingSource, proc *steps.Processor, router *router.Router, dests []destination.Destination, srcName string) error {
	if cfg == nil || src == nil || proc == nil || router == nil {
		return errors.New("streaming run: invalid arguments")
	}
	l := vlogger.FromContext(ctx)
	rowCh := make(chan row.Row, 1000)
	subscribeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- src.Subscribe(subscribeCtx, rowCh)
		close(rowCh)
	}()
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-subscribeCtx.Done():
		}
	}()

	dlq, dlqErr := newDLQWriter(cfg)
	if dlqErr != nil {
		l.Warn("dlq unavailable, falling back to skip",
			slog.String("pipeline", cfg.Name),
			slog.String("error", dlqErr.Error()),
		)
	}
	defer func() {
		if n := dlq.Count(); n > 0 {
			l.Warn("rows dead-lettered; replay from the DLQ file",
				slog.String("pipeline", cfg.Name),
				slog.Int("rows", n),
				slog.String("path", dlq.Path()),
			)
		}
		_ = dlq.Close()
	}()

	for {
		select {
		case r, ok := <-rowCh:
			if !ok {
				err := <-done
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					sendFailureAlert(ctx, cfg, err)
					return err
				}
				// Report cancellation consistently regardless of whether the
				// subscribe goroutine or the ctx.Done() branch observed it first.
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return nil
			}
			if proc != nil {
				var keep bool
				r, keep = proc.Apply(r)
				if !keep {
					_ = src.Ack(ctx, r.ID)
					continue
				}
			}
			_, ok, err := e.dispatchWithPolicy(context.WithoutCancel(ctx), cfg, router, dests, r)
			if err != nil || !ok {
				if err != nil && dlq.Enabled() {
					// DLQ mode: capture the failed event and ack so the stream advances.
					if writeErr := dlq.Write(r, err); writeErr == nil {
						_ = src.Ack(ctx, r.ID)
						continue
					} else {
						l.Warn("dlq write failed",
							slog.String("pipeline", cfg.Name),
							slog.String("row_id", r.ID),
							slog.String("error", writeErr.Error()),
						)
					}
				}
				_ = src.Nack(ctx, r.ID)
				if err != nil {
					sendFailureAlert(ctx, cfg, err)
					return err
				}
				continue
			}
			_ = src.Ack(ctx, r.ID)
		case <-ctx.Done():
			_ = <-done
			return ctx.Err()
		}
	}
}
