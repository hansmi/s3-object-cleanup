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

type cleanupStats struct {
	mu sync.Mutex

	totalCount       int64
	totalBytes       int64
	totalModTime     timeRange
	totalRetainUntil timeRange

	retentionCount   int64
	retentionModTime timeRange

	deleteCount   int64
	deleteBytes   int64
	deleteModTime timeRange

	deleteSuccessCount int64
	deleteErrorCount   int64
}

func newCleanupStats() *cleanupStats {
	return &cleanupStats{}
}

func (s *cleanupStats) discovered(v objectVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalCount++
	s.totalBytes += v.size
	s.totalModTime.update(v.lastModified)
	s.totalRetainUntil.update(v.retainUntil)
}

func (s *cleanupStats) addRetention(v objectVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retentionCount++
	s.retentionModTime.update(v.lastModified)
}

func (s *cleanupStats) addDelete(v objectVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteCount++
	s.deleteBytes += v.size
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
			slog.Int64("bytes", s.totalBytes),
			slog.String("bytes_text", humanize.IBytes(uint64(s.totalBytes))),
			slog.Group("mod_time", s.totalModTime.attrs()...),
			slog.Group("retain_until", s.totalRetainUntil.attrs()...),
		),
		slog.Group("retention",
			slog.Int64("count", s.retentionCount),
			slog.Group("mod_time", s.retentionModTime.attrs()...),
		),
		slog.Group("delete",
			slog.Int64("count", s.deleteCount),
			slog.Int64("bytes", s.deleteBytes),
			slog.String("bytes_text", humanize.IBytes(uint64(s.deleteBytes))),
			slog.Group("mod_time", s.deleteModTime.attrs()...),
			slog.Int64("success_count", s.deleteSuccessCount),
			slog.Int64("error_count", s.deleteErrorCount),
		),
	}
}
