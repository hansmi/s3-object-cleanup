package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTimeRange(t *testing.T) {
	for _, tc := range []struct {
		name       string
		values     []time.Time
		wantOldest time.Time
		wantNewest time.Time
	}{
		{name: "empty"},
		{
			name: "one",
			values: []time.Time{
				time.Date(2020, time.December, 1, 2, 3, 4, 0, time.UTC),
			},
			wantOldest: time.Date(2020, time.December, 1, 2, 3, 4, 0, time.UTC),
			wantNewest: time.Date(2020, time.December, 1, 2, 3, 4, 0, time.UTC),
		},
		{
			name: "three",
			values: []time.Time{
				time.Date(2020, time.March, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.December, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.April, 1, 0, 0, 0, 0, time.UTC),
			},
			wantOldest: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
			wantNewest: time.Date(2020, time.December, 1, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r timeRange

			for _, i := range tc.values {
				r.update(i)
			}

			want := []any{
				slog.Time("oldest", tc.wantOldest),
				slog.Time("newest", tc.wantNewest),
			}

			if diff := cmp.Diff(want, r.attrs()); diff != "" {
				t.Errorf("timeRange diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStats(t *testing.T) {
	type timeRangeStructure struct {
		Oldest *time.Time `json:"oldest"`
		Newest *time.Time `json:"newest"`
	}

	// Missing attributes are detected via the use of pointers.
	type structure struct {
		Total *struct {
			Count     *int64              `json:"count"`
			Bytes     *int64              `json:"bytes"`
			BytesText *string             `json:"bytes_text"`
			ModTime   *timeRangeStructure `json:"mod_time"`
		} `json:"total"`
		RetentionAnnotation *struct {
			ErrorCount *int64 `json:"error_count"`
		} `json:"retention_annotation"`
		Retention *struct {
			SuccessCount *int64              `json:"success_count"`
			ErrorCount   *int64              `json:"error_count"`
			ModTime      *timeRangeStructure `json:"mod_time"`
			Original     *timeRangeStructure `json:"original"`
		} `json:"retention"`
		Delete *struct {
			Count        *int64              `json:"count"`
			Bytes        *int64              `json:"bytes"`
			BytesText    *string             `json:"bytes_text"`
			SuccessCount *int64              `json:"success_count"`
			ErrorCount   *int64              `json:"error_count"`
			ModTime      *timeRangeStructure `json:"mod_time"`
		} `json:"delete"`
	}

	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, s *cleanupStats)
		want    string
	}{
		{
			name: "empty",
			want: `{
				"total": {
					"count": 0,
					"bytes": 0,
					"bytes_text": "0 B",
					"mod_time": {
						"oldest": "0001-01-01T00:00:00Z",
						"newest": "0001-01-01T00:00:00Z"
					}
				},
				"retention_annotation": {
					"error_count": 0
				},
				"retention": {
					"success_count": 0,
					"error_count": 0,
					"mod_time": {
						"oldest": "0001-01-01T00:00:00Z",
						"newest": "0001-01-01T00:00:00Z"
					},
					"original": {
						"oldest": "0001-01-01T00:00:00Z",
						"newest": "0001-01-01T00:00:00Z"
					}
				},
				"delete": {
					"count": 0,
					"bytes": 0,
					"bytes_text": "0 B",
					"success_count": 0,
					"error_count": 0,
					"mod_time": {
						"oldest": "0001-01-01T00:00:00Z",
						"newest": "0001-01-01T00:00:00Z"
					}
				}
			}`,
		},
		{
			name: "populated",
			prepare: func(_ *testing.T, s *cleanupStats) {
				s.discovered(objectVersion{
					size:         2 * 1024 * 1024,
					lastModified: time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC),
					retainUntil:  time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC),
				})
				s.discovered(objectVersion{
					size:         5 * 1024 * 1024,
					lastModified: time.Date(2011, time.October, 1, 0, 0, 0, 0, time.UTC),
					retainUntil:  time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC),
				})
				s.addRetention(objectVersion{
					lastModified: time.Date(2012, time.October, 1, 0, 0, 0, 0, time.UTC),
					retainUntil:  time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC),
				})
				s.addDelete(objectVersion{
					size:         3 * 1024 * 1024,
					lastModified: time.Date(2021, time.March, 1, 0, 0, 0, 0, time.UTC),
				})
				s.addDeleteResults(10, 20)
			},
			want: `{
				"total": {
					"count": 2,
					"bytes": 7340032,
					"bytes_text": "7.0 MiB",
					"mod_time": {
						"oldest": "2011-10-01T00:00:00Z",
						"newest": "2015-01-01T00:00:00Z"
					}
				},
				"retention_annotation": {
					"error_count": 0
				},
				"retention": {
					"success_count": 1,
					"error_count": 0,
					"mod_time": {
						"oldest": "2012-10-01T00:00:00Z",
						"newest": "2012-10-01T00:00:00Z"
					},
					"original": {
						"oldest": "2019-01-01T00:00:00Z",
						"newest": "2019-01-01T00:00:00Z"
					}
				},
				"delete": {
					"count": 1,
					"bytes": 3145728,
					"bytes_text": "3.0 MiB",
					"success_count": 10,
					"error_count": 20,
					"mod_time": {
						"oldest": "2021-03-01T00:00:00Z",
						"newest": "2021-03-01T00:00:00Z"
					}
				}
			}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			s := newCleanupStats()

			if tc.prepare != nil {
				tc.prepare(t, s)
			}

			h := slog.New(slog.NewJSONHandler(&buf, nil))
			h.Info("test", s.attrs()...)

			var got structure

			if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
				t.Fatalf("Unmarshal() failed: %v", err)
			}

			var want structure

			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatalf("Unmarshal(%q) failed: %v", tc.want, err)
			}

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Log message diff (-want +got):\n%s", diff)
			}
		})
	}
}
