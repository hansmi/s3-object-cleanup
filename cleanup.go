package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/hansmi/s3-object-cleanup/internal/client"
	"github.com/hansmi/s3-object-cleanup/internal/state"
	"golang.org/x/sync/errgroup"
)

type versionSeriesResult struct {
	expired []objectVersion
	keep    []objectVersion
}

type versionSeries struct {
	key        string
	items      []objectVersion
	haveLatest bool
}

func newVersionSeries(key string) *versionSeries {
	return &versionSeries{
		key: key,
	}
}

func (s *versionSeries) add(v objectVersion) {
	s.haveLatest = s.haveLatest || v.isLatest

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

func (s *versionSeries) check(minModTime time.Time) (result versionSeriesResult) {
	// Avoid making changes unless the latest version is known.
	if !s.haveLatest {
		result.keep = s.items
		return
	}

	end := -1

	// Find most recent version to delete.
	for idx, i := range s.items {
		if i.isLatest && !i.deleteMarker {
			// Ignore latest version and anything newer.
			break
		}

		if minModTime.Before(i.lastModified) || (!i.retainUntil.IsZero() && minModTime.Before(i.retainUntil)) {
			// Too recent.
			break
		}

		if (idx+1) < len(s.items) && !i.deleteMarker {
			// Keep last version before deletion until the delete marker
			// expires.
			if next := s.items[idx+1]; next.deleteMarker && next.lastModified.After(minModTime) {
				break
			}
		}

		end = idx
	}

	if end >= 0 {
		result.expired = slices.Clone(s.items[:end+1])

		s.items = slices.Replace(s.items, 0, end+1)
	}

	result.keep = s.items

	return result
}

type processor struct {
	stats      *cleanupStats
	minModTime time.Time
}

func newProcessor(stats *cleanupStats, minModTime time.Time) *processor {
	return &processor{
		stats:      stats,
		minModTime: minModTime,
	}
}

func (p *processor) run(in <-chan objectVersion, extendCh, deleteCh chan<- objectVersion) error {
	objects := map[string]*versionSeries{}

	for ov := range in {
		p.stats.discovered(ov)

		s := objects[ov.key]

		if s == nil {
			s = newVersionSeries(ov.key)

			objects[ov.key] = s
		}

		s.add(ov)

		for _, i := range s.check(p.minModTime).expired {
			// Early deletions
			deleteCh <- i
		}
	}

	for _, s := range objects {
		checkResult := s.check(p.minModTime)

		for _, i := range checkResult.expired {
			deleteCh <- i
		}

		for _, i := range checkResult.keep {
			extendCh <- i
		}
	}

	return nil
}

type cleanupOptions struct {
	logger     *slog.Logger
	stats      *cleanupStats
	state      *state.Store
	client     *client.Client
	dryRun     bool
	minModTime time.Time

	minRetention          time.Duration
	minRetentionThreshold time.Duration
}

func cleanup(ctx context.Context, opts cleanupOptions) error {
	bucketState, err := opts.state.Bucket(opts.client.Name())
	if err != nil {
		return fmt.Errorf("bucket state: %w", err)
	}

	annotateCh := make(chan objectVersion, 8)
	handleCh := make(chan objectVersion, 8)
	extendCh := make(chan objectVersion, 8)
	deleteCh := make(chan objectVersion, 8)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(annotateCh)

		return listObjectVersions(ctx, opts.client.S3(), opts.client.Name(), opts.client.Prefix(), annotateCh)
	})
	g.Go(func() error {
		defer close(handleCh)

		a := newRetentionAnnotator(bucketState, opts.client)

		return a.run(ctx, annotateCh, handleCh)
	})
	g.Go(func() error {
		defer close(deleteCh)
		defer close(extendCh)

		p := newProcessor(opts.stats, opts.minModTime)

		return p.run(handleCh, extendCh, deleteCh)
	})
	g.Go(func() error {
		e := newRetentionExtender(retentionExtenderOptions{
			logger:       opts.logger,
			stats:        opts.stats,
			state:        bucketState,
			client:       opts.client,
			dryRun:       opts.dryRun,
			minRetention: opts.minRetention,
			threshold:    opts.minRetentionThreshold,
		})

		return e.run(ctx, extendCh)
	})
	g.Go(func() error {
		deleter := newBatchDeleter(opts.logger, opts.stats, opts.client, opts.dryRun)

		return deleter.run(ctx, deleteCh)
	})

	return g.Wait()
}
