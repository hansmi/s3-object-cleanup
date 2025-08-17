package main

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/hansmi/s3-object-cleanup/internal/state"
	"golang.org/x/sync/errgroup"
)

type objectVersionHandler struct {
	key      string
	versions []objectVersion
}

func (t *objectVersionHandler) append(v objectVersion) {
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

func (t *objectVersionHandler) popOldVersions(modifiedBefore time.Time) []objectVersion {
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
	stats *cleanupStats
	ch    chan<- objectVersion

	modifiedBefore time.Time

	objects map[string]*objectVersionHandler
}

func newCleanupHandler(stats *cleanupStats, ch chan<- objectVersion, modifiedBefore time.Time) *cleanupHandler {
	return &cleanupHandler{
		stats: stats,
		ch:    ch,

		modifiedBefore: modifiedBefore,

		objects: map[string]*objectVersionHandler{},
	}
}

func (h *cleanupHandler) handle(v objectVersion) {
	h.stats.discovered(v)

	oh := h.objects[v.key]

	if oh == nil {
		oh = &objectVersionHandler{
			key: v.key,
		}

		h.objects[v.key] = oh
	}

	oh.append(v)

	for _, i := range oh.popOldVersions(h.modifiedBefore) {
		h.ch <- i
	}
}

func (h *cleanupHandler) handleVersion(ov types.ObjectVersion) error {
	h.handle(objectVersion{
		key:          aws.ToString(ov.Key),
		versionID:    aws.ToString(ov.VersionId),
		lastModified: aws.ToTime(ov.LastModified),
		isLatest:     aws.ToBool(ov.IsLatest),
		size:         aws.ToInt64(ov.Size),
	})

	return nil
}

func (h *cleanupHandler) handleDeleteMarker(marker types.DeleteMarkerEntry) error {
	h.handle(objectVersion{
		key:          aws.ToString(marker.Key),
		versionID:    aws.ToString(marker.VersionId),
		lastModified: aws.ToTime(marker.LastModified),
		isLatest:     aws.ToBool(marker.IsLatest),
		deleteMarker: true,
		size:         0,
	})

	return nil
}

func cleanup(ctx context.Context, s *cleanupStats, state *state.Store, b *bucket, dryRun bool, modifiedBefore time.Time) error {
	ch := make(chan objectVersion, 8)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		deleter := newBatchDeleter(slog.Default(), s, b, dryRun)

		return deleter.run(ctx, ch)
	})
	g.Go(func() error {
		defer close(ch)

		return b.listObjectVersions(ctx, newCleanupHandler(s, ch, modifiedBefore))
	})

	return g.Wait()
}
