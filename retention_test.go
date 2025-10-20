package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type fakeExtenderClient struct {
	mu       sync.Mutex
	requests []time.Time
	err      error
}

func (c *fakeExtenderClient) PutObjectRetention(_ context.Context, _ string, _ string, until time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requests = append(c.requests, until)

	return c.err
}

func TestExtend(t *testing.T) {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	now := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name         string
		ov           objectVersion
		minRetention time.Duration
		threshold    time.Duration
		want         []time.Time
		wantErr      error
	}{
		{
			name: "zero",
			want: []time.Time{now},
		},
		{
			name: "normal extension",
			ov: objectVersion{
				retainUntil: time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
			},
			minRetention: 14 * 24 * time.Hour,
			threshold:    10 * 24 * time.Hour,
			want: []time.Time{
				time.Date(2015, time.January, 15, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "already retained beyond extension time",
			ov: objectVersion{
				retainUntil: time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
			},
			minRetention: 7 * 24 * time.Hour,
			threshold:    14 * 24 * time.Hour,
		},
		{
			name: "not yet time for extension",
			ov: objectVersion{
				retainUntil: time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
			},
			minRetention: 14 * 24 * time.Hour,
			threshold:    24 * time.Hour,
		},
		{
			name:         "version has no retention",
			minRetention: 7 * 24 * time.Hour,
			threshold:    24 * time.Hour,
			want: []time.Time{
				time.Date(2015, time.January, 8, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "delete marker",
			ov: objectVersion{
				deleteMarker: true,
			},
			minRetention: 7 * 24 * time.Hour,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newRetentionStateForTest(t)
			var client fakeExtenderClient

			opts := retentionExtenderOptions{
				logger:       logger,
				stats:        newCleanupStats(),
				state:        state,
				client:       &client,
				now:          now,
				minRetention: tc.minRetention,
				threshold:    tc.threshold,
			}

			err := newRetentionExtender(opts).extend(ctx, tc.ov)

			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Error diff (-want +got):\n%s", diff)
			}

			if diff := cmp.Diff(tc.want, client.requests, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Requests diff (-want +got):\n%s", diff)
			}

			var wantState time.Time

			if len(tc.want) > 0 {
				wantState = tc.want[len(tc.want)-1]
			}

			if gotState, err := state.GetObjectRetention(tc.ov.key, tc.ov.versionID); err != nil {
				t.Errorf("GetObjectRetention() failed: %v", err)
			} else if diff := cmp.Diff(wantState, gotState); diff != "" {
				t.Errorf("GetObjectRetention() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExtenderRun(t *testing.T) {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	state := newRetentionStateForTest(t)
	var client fakeExtenderClient

	opts := retentionExtenderOptions{
		logger: logger,
		stats:  newCleanupStats(),
		state:  state,
		client: &client,
	}

	ch := make(chan objectVersion)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)

		for range 100 {
			ch <- objectVersion{}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := newRetentionExtender(opts).run(ctx, ch); err != nil {
			t.Errorf("run() failed: %v", err)
		}
	}()

	wg.Wait()
}
