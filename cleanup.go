package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/hansmi/s3-object-cleanup/internal/state"
	"golang.org/x/sync/errgroup"
)

type versionSeries struct {
	key   string
	items []objectVersion
}

func newVersionSeries(key string) *versionSeries {
	return &versionSeries{
		key: key,
	}
}

func (s *versionSeries) add(v objectVersion) {
	if len(s.items) == 0 {
		s.items = append(s.items, v)
		return
	}

	// Maintain a sorted list of items.
	pos, _ := slices.BinarySearchFunc(s.items, v, func(a, b objectVersion) int {
		return cmp.Or(
			a.lastModified.Compare(b.lastModified),
			cmp.Compare(a.versionID, b.versionID),
		)
	})

	s.items = slices.Insert(s.items, pos, v)
}

func (s *versionSeries) popOldVersions(modifiedBefore time.Time) []objectVersion {
	// Avoid deleting unless the latest version is known.
	if latestKnown := slices.ContainsFunc(s.items, func(v objectVersion) bool {
		return v.isLatest
	}); !latestKnown {
		return nil
	}

	end := -1

	// Find most recent version to delete.
	for idx, i := range s.items {
		if i.isLatest && !i.deleteMarker {
			// Ignore latest version and anything newer.
			break
		}

		if i.lastModified.After(modifiedBefore) {
			// Too recent.
			break
		}

		if (idx+1) < len(s.items) && !i.deleteMarker {
			// Keep last version before deletion until the delete marker
			// expires.
			if next := s.items[idx+1]; next.deleteMarker && next.lastModified.After(modifiedBefore) {
				break
			}
		}

		end = idx
	}

	var result []objectVersion

	if end >= 0 {
		result = slices.Clone(s.items[:end+1])

		s.items = slices.Replace(s.items, 0, end+1)
	}

	return result
}

type processor struct {
	stats          *cleanupStats
	modifiedBefore time.Time
}

func newProcessor(stats *cleanupStats, modifiedBefore time.Time) *processor {
	return &processor{
		stats:          stats,
		modifiedBefore: modifiedBefore,
	}
}

func (p *processor) run(ctx context.Context, in <-chan objectVersion, deleteCh chan<- objectVersion) error {
	objects := map[string]*versionSeries{}

	for {
		var ov objectVersion
		var ok bool

		select {
		case <-ctx.Done():
			return ctx.Err()

		case ov, ok = <-in:
			if !ok {
				return nil
			}
		}

		p.stats.discovered(ov)

		s := objects[ov.key]

		if s == nil {
			s = newVersionSeries(ov.key)

			objects[ov.key] = s
		}

		s.add(ov)

		for _, i := range s.popOldVersions(p.modifiedBefore) {
			deleteCh <- i
		}
	}

	return nil
}

type cleanupOptions struct {
	logger         *slog.Logger
	stats          *cleanupStats
	state          *state.Store
	client         *client
	dryRun         bool
	modifiedBefore time.Time
}

func cleanup(ctx context.Context, opts cleanupOptions) error {
	bucketState, err := opts.state.Bucket(opts.client.name)
	if err != nil {
		return fmt.Errorf("bucket state: %w", err)
	}

	annotateCh := make(chan objectVersion, 8)
	handleCh := make(chan objectVersion, 8)
	deleteCh := make(chan objectVersion, 8)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(annotateCh)

		return listObjectVersions(ctx, opts.client.client, opts.client.name, opts.client.prefix, annotateCh)
	})
	g.Go(func() error {
		defer close(handleCh)

		a := newRetentionAnnotator(bucketState, opts.client)

		return a.run(ctx, annotateCh, handleCh)
	})
	g.Go(func() error {
		defer close(deleteCh)

		p := newProcessor(opts.stats, opts.modifiedBefore)

		return p.run(ctx, handleCh, deleteCh)
	})
	g.Go(func() error {
		deleter := newBatchDeleter(opts.logger, opts.stats, opts.client, opts.dryRun)

		return deleter.run(ctx, deleteCh)
	})

	return g.Wait()
}
