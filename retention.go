package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

type retentionExtenderState interface {
	SetObjectRetention(string, string, time.Time) error
}

type retentionExtenderClient interface {
	PutObjectRetention(context.Context, string, string, time.Time) error
}

type retentionExtender struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  retentionExtenderState
	client retentionExtenderClient

	workers int

	now time.Time

	minRetention time.Duration
	threshold    time.Duration

	dryRun bool
}

type retentionExtenderOptions struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  retentionExtenderState
	client retentionExtenderClient
	dryRun bool

	// Current time for computations. Defaults to [time.Now()].
	now time.Time

	// Object version retention must be at least least the given duration.
	minRetention time.Duration

	// Set retention when it's missing or the remaining duration is less than
	// the threshold.
	threshold time.Duration
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
		minRetention: max(0, opts.minRetention),
		threshold:    max(0, opts.threshold),
		workers:      4,
	}
}

func (e *retentionExtender) extend(ctx context.Context, ov objectVersion) error {
	if ov.deleteMarker {
		// Delete markers don't support retention periods.
		return nil
	}

	until := e.now.Add(e.minRetention).Truncate(time.Second)

	if ov.retainUntil.IsZero() || (until.After(ov.retainUntil) && ov.retainUntil.Sub(e.now) < e.threshold) {
		e.logger.InfoContext(ctx, "Extending object retention",
			slog.Any("object", ov),
			slog.Time("until", until),
		)

		e.stats.addRetention(ov)

		if !e.dryRun {
			if err := e.client.PutObjectRetention(ctx, ov.key, ov.versionID, until); err != nil {
				return fmt.Errorf("setting object retention via API: %w", err)
			}

			if err := e.state.SetObjectRetention(ov.key, ov.versionID, until); err != nil {
				return fmt.Errorf("setting object retention in state: %w", err)
			}
		}
	}

	return nil
}

// run extends the retention duration on all objects received from the incoming
// channel.
func (e *retentionExtender) run(ctx context.Context, in <-chan objectVersion) error {
	g, ctx := errgroup.WithContext(ctx)

	for range max(1, e.workers) {
		g.Go(func() error {
			for ov := range in {
				if err := e.extend(ctx, ov); err != nil {
					e.logger.Error("Retention extension failed",
						slog.Any("object", ov),
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
