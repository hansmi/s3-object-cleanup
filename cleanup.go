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
	expired   []objectVersion
	retention []retentionExtenderRequest
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

type versionSeriesFinalizeOptions struct {
	now            time.Time
	minRetention   time.Duration
	minDeletionAge time.Duration
}

func (o *versionSeriesFinalizeOptions) extendFromNow(ov objectVersion) (retentionExtenderRequest, bool) {
	origin := o.now

	if origin.Before(ov.lastModified) {
		origin = ov.lastModified
	}

	return o.extend(ov, origin.Add(o.minRetention))
}

func (o *versionSeriesFinalizeOptions) extend(ov objectVersion, until time.Time) (retentionExtenderRequest, bool) {
	req := retentionExtenderRequest{
		object: ov,
		until:  until,
	}

	return req, (ov.retainUntil.IsZero() || ov.retainUntil.Before(req.until)) && !ov.deleteMarker
}

func (s *versionSeries) finalize(opts versionSeriesFinalizeOptions) (result versionSeriesResult) {
	// Apply the default retention extension and avoid deletions unless the
	// latest version is known.
	if !s.haveLatest {
		for _, ov := range s.items {
			if req, ok := opts.extendFromNow(ov); ok {
				result.retention = append(result.retention, req)
			}
		}

		return
	}

	pos := len(s.items) - 1

	// Look for latest version and extend all versions until there.
	for ; pos >= 0; pos-- {
		ov := s.items[pos]

		if ov.isLatest {
			// Delete markers don't support retention periods.
			if ov.deleteMarker {
				expires := ov.lastModified.Add(opts.minDeletionAge)

				if expires.Before(opts.now) {
					// Already expired
					pos++
					break
				}

				// Extend retention of the most recent regular version
				// preceding the delete marker.
				for ; pos >= 0; pos-- {
					ov = s.items[pos]

					if ov.deleteMarker {
						continue
					}

					if req, ok := opts.extend(ov, expires); ok {
						result.retention = append(result.retention, req)
					}

					break
				}
			} else if req, ok := opts.extendFromNow(ov); ok {
				result.retention = append(result.retention, req)
			}

			break
		}

		if req, ok := opts.extendFromNow(ov); ok {
			result.retention = append(result.retention, req)
		}
	}

	if pos >= 0 {
		cutoff := opts.now.Add(-opts.minDeletionAge)

		for _, ov := range s.items[:pos] {
			if !ov.lastModified.Before(cutoff) {
				break
			}

			if !(ov.retainUntil.IsZero() || ov.retainUntil.Before(opts.now)) {
				break
			}

			result.expired = append(result.expired, ov)
		}
	}

	return
}

type processor struct {
	stats          *cleanupStats
	minRetention   time.Duration
	minDeletionAge time.Duration
}

type processorOptions struct {
	stats          *cleanupStats
	minDeletionAge time.Duration
	minRetention   time.Duration
}

func newProcessor(opts processorOptions) *processor {
	return &processor{
		stats:          opts.stats,
		minDeletionAge: opts.minDeletionAge,
		minRetention:   opts.minRetention,
	}
}

func (p *processor) run(in <-chan objectVersion, retentionCh chan<- retentionExtenderRequest, deleteCh chan<- objectVersion) {
	objects := map[string]*versionSeries{}

	for ov := range in {
		p.stats.discovered(ov)

		s := objects[ov.key]

		if s == nil {
			s = newVersionSeries(ov.key)

			objects[ov.key] = s
		}

		s.add(ov)
	}

	finalizeOpts := versionSeriesFinalizeOptions{
		now:            time.Now(),
		minDeletionAge: p.minDeletionAge,
		minRetention:   p.minRetention,
	}

	for _, s := range objects {
		result := s.finalize(finalizeOpts)

		for _, i := range result.expired {
			deleteCh <- i
		}

		for _, i := range result.retention {
			retentionCh <- i
		}
	}
}

type cleanupOptions struct {
	logger *slog.Logger
	stats  *cleanupStats
	state  *state.Store
	client *client.Client
	dryRun bool

	minDeletionAge        time.Duration
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
	retentionCh := make(chan retentionExtenderRequest, 8)
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
		defer close(retentionCh)

		p := newProcessor(processorOptions{
			stats:          opts.stats,
			minRetention:   opts.minRetention,
			minDeletionAge: opts.minDeletionAge,
		})
		p.run(handleCh, retentionCh, deleteCh)

		return nil
	})
	g.Go(func() error {
		e := newRetentionExtender(retentionExtenderOptions{
			logger:       opts.logger,
			stats:        opts.stats,
			state:        bucketState,
			client:       opts.client,
			minRemaining: opts.minRetentionThreshold,
			dryRun:       opts.dryRun,
		})

		return e.run(ctx, retentionCh)
	})
	g.Go(func() error {
		deleter := newBatchDeleter(batchDeleterOptions{
			logger: opts.logger,
			stats:  opts.stats,
			state:  bucketState,
			client: opts.client.S3(),
			bucket: opts.client.Name(),
			dryRun: opts.dryRun,
		})

		return deleter.run(ctx, deleteCh)
	})

	return g.Wait()
}
