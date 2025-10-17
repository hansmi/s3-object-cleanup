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

// findFirstExtended identifies the first version which needs to have its
// retention period extended.
func findFirstExtended(versions []objectVersion, recent func(objectVersion) bool) int {
	for idx, ov := range slices.Backward(versions) {
		if !ov.isLatest {
			continue
		}

		if !ov.deleteMarker {
			return idx
		}

		if recent(ov) {
			// Extend from the most recent regular version preceding the
			// delete marker.
			for i := idx - 1; i >= 0; i-- {
				if !versions[i].deleteMarker {
					return i
				}
			}

			return idx
		}

		break
	}

	return -1
}

type versionSeriesResult struct {
	expired []objectVersion
	extend  []objectVersion
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

func (s *versionSeries) check(cutoff time.Time) (result versionSeriesResult) {
	// Avoid making changes unless the latest version is known.
	if !s.haveLatest {
		result.extend = s.items
		return
	}

	recent := func(ov objectVersion) bool {
		if !ov.retainUntil.IsZero() && cutoff.Before(ov.retainUntil) {
			return true
		}

		return cutoff.Before(ov.lastModified)
	}

	earlier := s.items

	if firstExtended := findFirstExtended(s.items, recent); firstExtended >= 0 {
		result.extend = slices.Clone(s.items[firstExtended:])
		earlier = s.items[:firstExtended]
	}

	firstKept := 0

	for ; firstKept < len(earlier) && !recent(earlier[firstKept]); firstKept++ {
	}

	if firstKept > 0 {
		result.expired = slices.Clone(s.items[:firstKept])

		// Remove expired versions.
		s.items = slices.Replace(s.items, 0, firstKept)
	}

	return
}

type processor struct {
	stats  *cleanupStats
	cutoff time.Time
}

func newProcessor(stats *cleanupStats, minAge time.Duration) *processor {
	return &processor{
		stats:  stats,
		cutoff: time.Now().Add(-minAge).Truncate(time.Minute),
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

		for _, i := range s.check(p.cutoff).expired {
			// Early deletions
			deleteCh <- i
		}
	}

	for _, s := range objects {
		checkResult := s.check(p.cutoff)

		for _, i := range checkResult.expired {
			deleteCh <- i
		}

		for _, i := range checkResult.extend {
			extendCh <- i
		}
	}

	return nil
}

type cleanupOptions struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  *state.Store
	client *client.Client
	dryRun bool

	minAge                time.Duration
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

		a := newRetentionAnnotator(retentionAnnotatorOptions{
			logger: opts.logger,
			stats:  opts.stats,
			state:  bucketState,
			client: opts.client,
		})

		return a.run(ctx, annotateCh, handleCh)
	})
	g.Go(func() error {
		defer close(deleteCh)
		defer close(extendCh)

		p := newProcessor(opts.stats, opts.minAge)

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
