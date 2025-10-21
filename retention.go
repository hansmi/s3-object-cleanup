package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

type retentionExtenderState interface {
	SetObjectRetention(string, string, time.Time) error
}

type retentionExtenderClient interface {
	PutObjectRetention(context.Context, string, string, time.Time) error
}

type retentionExtenderRequest struct {
	object objectVersion
	until  time.Time
}

type retentionExtender struct {
	logger       *slog.Logger
	stats        *cleanupStats
	state        retentionExtenderState
	client       retentionExtenderClient
	workers      int
	now          time.Time
	minRemaining time.Duration
	dryRun       bool
}

type retentionExtenderOptions struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  retentionExtenderState
	client retentionExtenderClient
	dryRun bool

	// Current time for computations. Defaults to [time.Now()].
	now time.Time

	// Update retention when it's missing or the remaining duration is less
	// than minRemaining.
	minRemaining time.Duration
}

func newRetentionExtender(opts retentionExtenderOptions) *retentionExtender {
	if opts.now.IsZero() {
		opts.now = time.Now()
	}

	return &retentionExtender{
		logger:       opts.logger,
		stats:        opts.stats,
		state:        opts.state,
		client:       opts.client,
		dryRun:       opts.dryRun,
		now:          opts.now,
		minRemaining: max(0, opts.minRemaining),
		workers:      4,
	}
}

func (e *retentionExtender) process(ctx context.Context, req retentionExtenderRequest) error {
	if req.object.deleteMarker {
		// Delete markers don't support retention periods.
		return nil
	}

	if req.until.IsZero() {
		return fmt.Errorf("%w: missing retention time", os.ErrInvalid)
	}

	remaining := req.until.Sub(e.now).Truncate(time.Second)

	if req.object.retainUntil.IsZero() || remaining < e.minRemaining {
		e.logger.InfoContext(ctx, "Extending object retention",
			slog.Any("object", req.object),
			slog.String("remaining", remaining.String()),
			slog.Time("until", req.until),
		)

		// TODO: Log remaining time range.
		e.stats.addRetention(req.object)

		if !e.dryRun {
			ov := req.object

			if err := e.client.PutObjectRetention(ctx, ov.key, ov.versionID, req.until); err != nil {
				return fmt.Errorf("setting object retention via API: %w", err)
			}

			if err := e.state.SetObjectRetention(ov.key, ov.versionID, req.until); err != nil {
				return fmt.Errorf("setting object retention in state: %w", err)
			}
		}
	}

	return nil
}

// run sets the retention time on objects received via the incoming channel.
func (e *retentionExtender) run(ctx context.Context, in <-chan retentionExtenderRequest) error {
	g, ctx := errgroup.WithContext(ctx)

	for range max(1, e.workers) {
		g.Go(func() error {
			for req := range in {
				if err := e.process(ctx, req); err != nil {
					e.logger.Error("Retention extension failed",
						slog.Any("request", req),
						slog.Any("error", err))
					e.stats.addRetentionError()
					continue
				}
			}

			return nil
		})
	}

	return g.Wait()
}
