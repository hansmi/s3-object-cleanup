package main

import (
	"log/slog"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

type timeRange struct {
	lower, upper time.Time
}

var _ slog.LogValuer = (*timeRange)(nil)

func (r *timeRange) update(t time.Time) {
	if t.IsZero() {
		return
	}

	if r.lower.IsZero() || t.Before(r.lower) {
		r.lower = t
	}

	if r.upper.IsZero() || t.After(r.upper) {
		r.upper = t
	}
}

func (r timeRange) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Time("lower", r.lower),
		slog.Time("upper", r.upper),
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

	deleteCount       int64
	deleteSize        sizeStats
	deleteModTime     timeRange
	deleteRetainUntil timeRange

	deleteSuccessCount int64
	deleteErrorCount   int64
}

func newCleanupStats() *cleanupStats {
	return &cleanupStats{}
}

func (s *cleanupStats) addRetentionAnnotationError() {
	s.mu.Lock()
	s.retentionAnnotationErrorCount++
	s.mu.Unlock()
}

func (s *cleanupStats) discovered(v objectVersion) {
	s.mu.Lock()
	s.totalCount++
	s.totalSize.add(v.size)
	s.totalModTime.update(v.lastModified)
	s.totalRetainUntil.update(v.retainUntil)
	s.mu.Unlock()
}

func (s *cleanupStats) addRetention(v objectVersion) {
	s.mu.Lock()
	s.retentionSuccessCount++
	s.retentionModTime.update(v.lastModified)
	s.retentionOriginal.update(v.retainUntil)
	s.mu.Unlock()
}

func (s *cleanupStats) addRetentionError() {
	s.mu.Lock()
	s.retentionErrorCount++
	s.mu.Unlock()
}

func (s *cleanupStats) addDelete(v objectVersion) {
	s.mu.Lock()
	s.deleteCount++
	s.deleteSize.add(v.size)
	s.deleteModTime.update(v.lastModified)
	s.deleteRetainUntil.update(v.retainUntil)
	s.mu.Unlock()
}

func (s *cleanupStats) addDeleteResults(successCount, errorCount int) {
	if successCount == 0 && errorCount == 0 {
		return
	}

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
			slog.Any("retain_until", s.deleteRetainUntil),
			slog.Int64("success_count", s.deleteSuccessCount),
			slog.Int64("error_count", s.deleteErrorCount),
		),
	}
}
