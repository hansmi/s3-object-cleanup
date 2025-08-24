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

type objectVersionTracker struct {
	key      string
	versions []objectVersion
}

func (t *objectVersionTracker) append(v objectVersion) {
	if len(t.versions) == 0 {
		t.versions = append(t.versions, v)
		return
	}

	// Maintain a sorted list of versions.
	pos, _ := slices.BinarySearchFunc(t.versions, v, func(a, b objectVersion) int {
		return cmp.Or(
			a.lastModified.Compare(b.lastModified),
			cmp.Compare(a.versionID, b.versionID),
		)
	})

	t.versions = slices.Insert(t.versions, pos, v)
}

func (t *objectVersionTracker) popOldVersions(modifiedBefore time.Time) []objectVersion {
	// Avoid deleting unless the latest version is known.
	if latestKnown := slices.ContainsFunc(t.versions, func(v objectVersion) bool {
		return v.isLatest
	}); !latestKnown {
		return nil
	}

	end := -1

	// Find most recent version to delete.
	for idx, i := range t.versions {
		if i.isLatest && !i.deleteMarker {
			// Ignore latest version and anything newer.
			break
		}

		if i.lastModified.After(modifiedBefore) {
			// Too recent.
			break
		}

		if (idx+1) < len(t.versions) && !i.deleteMarker {
			// Keep last version before deletion until the delete marker
			// expires.
			if next := t.versions[idx+1]; next.deleteMarker && next.lastModified.After(modifiedBefore) {
				break
			}
		}

		end = idx
	}

	var result []objectVersion

	if end >= 0 {
		result = slices.Clone(t.versions[:end+1])

		t.versions = slices.Replace(t.versions, 0, end+1)
	}

	return result
}

type cleanupHandler struct {
	stats    *cleanupStats
	deleteCh chan<- objectVersion

	modifiedBefore time.Time

	objects map[string]*objectVersionTracker
}

func newCleanupHandler(stats *cleanupStats, deleteCh chan<- objectVersion, modifiedBefore time.Time) *cleanupHandler {
	return &cleanupHandler{
		stats:    stats,
		deleteCh: deleteCh,

		modifiedBefore: modifiedBefore,

		objects: map[string]*objectVersionTracker{},
	}
}

func (h *cleanupHandler) handle(v objectVersion) error {
	h.stats.discovered(v)

	t := h.objects[v.key]

	if t == nil {
		t = &objectVersionTracker{
			key: v.key,
		}

		h.objects[v.key] = t
	}

	t.append(v)

	for _, i := range t.popOldVersions(h.modifiedBefore) {
		h.deleteCh <- i
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

		c := newCleanupHandler(opts.stats, deleteCh, opts.modifiedBefore)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case ov, ok := <-handleCh:
				if !ok {
					return nil
				}

				if err := c.handle(ov); err != nil {
					return err
				}
			}
		}
	})
	g.Go(func() error {
		deleter := newBatchDeleter(opts.logger, opts.stats, opts.client, opts.dryRun)

		return deleter.run(ctx, deleteCh)
	})

	return g.Wait()
}
