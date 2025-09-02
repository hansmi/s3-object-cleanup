package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

type retentionAnnotatorState interface {
	GetObjectRetention(string, string) (time.Time, error)
	SetObjectRetention(string, string, time.Time) error
}

type retentionAnnotatorClient interface {
	GetObjectRetention(context.Context, string, string) (time.Time, error)
}

type retentionAnnotatorOptions struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  retentionAnnotatorState
	client retentionAnnotatorClient
}

type retentionAnnotator struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  retentionAnnotatorState
	client retentionAnnotatorClient

	workers int
}

func newRetentionAnnotator(opts retentionAnnotatorOptions) *retentionAnnotator {
	return &retentionAnnotator{
		logger: opts.logger,
		stats:  opts.stats,
		state:  opts.state,
		client: opts.client,

		workers: 4,
	}
}

func (a *retentionAnnotator) annotate(ctx context.Context, ov objectVersion) (objectVersion, error) {
	if until := ov.retainUntil; until.IsZero() {
		var err error

		until, err = a.state.GetObjectRetention(ov.key, ov.versionID)
		if err != nil {
			return ov, fmt.Errorf("getting object retention from state: %w", err)
		}

		// Delete markers don't support retention periods.
		if until.IsZero() && !ov.deleteMarker {
			until, err = a.client.GetObjectRetention(ctx, ov.key, ov.versionID)
			if err != nil {
				return ov, fmt.Errorf("getting object retention from API: %w", err)
			}

			if err := a.state.SetObjectRetention(ov.key, ov.versionID, until); err != nil {
				return ov, fmt.Errorf("setting object retention in state: %w", err)
			}
		}

		if !until.IsZero() {
			ov.retainUntil = until
		}
	}

	return ov, nil
}

// run sets the retention configuration on all objects received from the
// incoming channel before forwarding them to the output channel.
func (a *retentionAnnotator) run(ctx context.Context, in <-chan objectVersion, out chan<- objectVersion) error {
	g, ctx := errgroup.WithContext(ctx)

	for range max(1, a.workers) {
		g.Go(func() error {
			for ov := range in {
				ov, err := a.annotate(ctx, ov)
				if err != nil {
					a.logger.Error("Retention annotation failed",
						slog.Any("object", ov),
						slog.Any("error", err))
					a.stats.addRetentionAnnotationError()
					continue
				}

				out <- ov
			}

			return nil
		})
	}

	return g.Wait()
}
