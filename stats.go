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

type cleanupStats struct {
	mu sync.Mutex

	totalCount   int64
	totalBytes   int64
	totalModTime timeRange

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
			slog.Time("oldest", s.totalModTime.oldest),
			slog.Time("newest", s.totalModTime.newest),
		),
		slog.Group("delete",
			slog.Int64("count", s.deleteCount),
			slog.Int64("bytes", s.deleteBytes),
			slog.String("bytes_text", humanize.IBytes(uint64(s.deleteBytes))),
			slog.Time("oldest", s.deleteModTime.oldest),
			slog.Time("newest", s.deleteModTime.newest),
			slog.Int64("success", s.deleteSuccessCount),
			slog.Int64("error", s.deleteErrorCount),
		),
	}
}
