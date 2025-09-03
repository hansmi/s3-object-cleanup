package main

import (
	"log/slog"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

type timeRange struct {
	oldest, newest time.Time
}

var _ slog.LogValuer = (*timeRange)(nil)

func (r *timeRange) update(t time.Time) {
	if r.oldest.IsZero() || t.Before(r.oldest) {
		r.oldest = t
	}

	if r.newest.IsZero() || t.After(r.newest) {
		r.newest = t
	}
}

func (r timeRange) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Time("oldest", r.oldest),
		slog.Time("newest", r.newest),
	)
}

type sizeStats int64

var _ slog.LogValuer = (*sizeStats)(nil)

func (s *sizeStats) add(bytes int64) {
	*(*int64)(s) += bytes
}

func (s sizeStats) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("bytes", int64(s)),
		slog.String("text", humanize.IBytes(uint64(s))),
	)
}

type cleanupStats struct {
	mu sync.Mutex

	retentionAnnotationErrorCount int64

	totalCount       int64
	totalSize        sizeStats
	totalModTime     timeRange
	totalRetainUntil timeRange

	retentionSuccessCount int64
	retentionErrorCount   int64
	retentionModTime      timeRange
	retentionOriginal     timeRange

	deleteCount   int64
	deleteSize    sizeStats
	deleteModTime timeRange

	deleteSuccessCount int64
	deleteErrorCount   int64
}

func newCleanupStats() *cleanupStats {
	return &cleanupStats{}
}

func (s *cleanupStats) addRetentionAnnotationError() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retentionAnnotationErrorCount++
}

func (s *cleanupStats) discovered(v objectVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalCount++
	s.totalSize.add(v.size)
	s.totalModTime.update(v.lastModified)
	s.totalRetainUntil.update(v.retainUntil)
}

func (s *cleanupStats) addRetention(v objectVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retentionSuccessCount++
	s.retentionModTime.update(v.lastModified)

	if !v.retainUntil.IsZero() {
		s.retentionOriginal.update(v.retainUntil)
	}
}

func (s *cleanupStats) addRetentionError() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retentionErrorCount++
}

func (s *cleanupStats) addDelete(v objectVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteCount++
	s.deleteSize.add(v.size)
	s.deleteModTime.update(v.lastModified)
}

func (s *cleanupStats) addDeleteResults(successCount, errorCount int) {
	s.mu.Lock()
	s.deleteSuccessCount += int64(successCount)
	s.deleteErrorCount += int64(errorCount)
	s.mu.Unlock()
}

func (s *cleanupStats) attrs() []any {
	s.mu.Lock()
	defer s.mu.Unlock()

	return []any{
		slog.Group("total",
			slog.Int64("count", s.totalCount),
			slog.Any("size", s.totalSize),
			slog.Any("mod_time", s.totalModTime),
			slog.Any("retain_until", s.totalRetainUntil),
		),
		slog.Group("retention_annotation",
			slog.Int64("error_count", s.retentionAnnotationErrorCount),
		),
		slog.Group("retention",
			slog.Int64("success_count", s.retentionSuccessCount),
			slog.Int64("error_count", s.retentionErrorCount),
			slog.Any("mod_time", s.retentionModTime),
			slog.Any("original", s.retentionOriginal),
		),
		slog.Group("delete",
			slog.Int64("count", s.deleteCount),
			slog.Any("size", s.deleteSize),
			slog.Any("mod_time", s.deleteModTime),
			slog.Int64("success_count", s.deleteSuccessCount),
			slog.Int64("error_count", s.deleteErrorCount),
		),
	}
}
