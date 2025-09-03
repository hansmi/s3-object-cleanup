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

func (r *timeRange) update(t time.Time) {
	if r.oldest.IsZero() || t.Before(r.oldest) {
		r.oldest = t
	}

	if r.newest.IsZero() || t.After(r.newest) {
		r.newest = t
	}
}

func (r *timeRange) attrs() []any {
	return []any{
		slog.Time("oldest", r.oldest),
		slog.Time("newest", r.newest),
	}
}

type sizeStats int64

func (s *sizeStats) add(bytes int64) {
	*(*int64)(s) += bytes
}

func (s sizeStats) attrs() []any {
	return []any{
		slog.Int64("bytes", int64(s)),
		slog.String("text", humanize.IBytes(uint64(s))),
	}
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
			slog.Group("size", s.totalSize.attrs()...),
			slog.Group("mod_time", s.totalModTime.attrs()...),
			slog.Group("retain_until", s.totalRetainUntil.attrs()...),
		),
		slog.Group("retention_annotation",
			slog.Int64("error_count", s.retentionAnnotationErrorCount),
		),
		slog.Group("retention",
			slog.Int64("success_count", s.retentionSuccessCount),
			slog.Int64("error_count", s.retentionErrorCount),
			slog.Group("mod_time", s.retentionModTime.attrs()...),
			slog.Group("original", s.retentionOriginal.attrs()...),
		),
		slog.Group("delete",
			slog.Int64("count", s.deleteCount),
			slog.Group("size", s.deleteSize.attrs()...),
			slog.Group("mod_time", s.deleteModTime.attrs()...),
			slog.Int64("success_count", s.deleteSuccessCount),
			slog.Int64("error_count", s.deleteErrorCount),
		),
	}
}
