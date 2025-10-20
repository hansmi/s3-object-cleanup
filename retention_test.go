package main

import (
	"context"
	"io"
	"log/slog"
	"os"
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

func TestRetentionProcess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	now := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name         string
		req          retentionExtenderRequest
		minRemaining time.Duration
		want         []time.Time
		wantErr      error
	}{
		{
			name:    "zero",
			wantErr: os.ErrInvalid,
		},
		{
			name: "normal",
			req: retentionExtenderRequest{
				object: objectVersion{
					retainUntil: time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
				},
				until: time.Date(2015, time.January, 20, 0, 0, 0, 0, time.UTC),
			},
			minRemaining: 100 * 24 * time.Hour,
			want: []time.Time{
				time.Date(2015, time.January, 20, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "already retained beyond extension time",
			req: retentionExtenderRequest{
				object: objectVersion{
					retainUntil: time.Date(2015, time.January, 30, 0, 0, 0, 0, time.UTC),
				},
				until: time.Date(2015, time.January, 20, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "not yet time for extension",
			req: retentionExtenderRequest{
				object: objectVersion{
					retainUntil: time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
				},
				until: time.Date(2015, time.January, 20, 0, 0, 0, 0, time.UTC),
			},
			minRemaining: 3 * 24 * time.Hour,
		},
		{
			name: "version has no retention",
			req: retentionExtenderRequest{
				object: objectVersion{},
				until:  time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
			},
			want: []time.Time{
				time.Date(2015, time.January, 10, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "delete marker",
			req: retentionExtenderRequest{
				object: objectVersion{
					deleteMarker: true,
				},
			},
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
				minRemaining: tc.minRemaining,
			}

			err := newRetentionExtender(opts).process(t.Context(), tc.req)

			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Error diff (-want +got):\n%s", diff)
			}

			if err == nil {
				if diff := cmp.Diff(tc.want, client.requests, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Requests diff (-want +got):\n%s", diff)
				}

				var wantState time.Time

				if len(tc.want) > 0 {
					wantState = tc.want[len(tc.want)-1]
				}

				if gotState, err := state.GetObjectRetention(tc.req.object.key, tc.req.object.versionID); err != nil {
					t.Errorf("GetObjectRetention() failed: %v", err)
				} else if diff := cmp.Diff(wantState, gotState); diff != "" {
					t.Errorf("GetObjectRetention() diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestExtenderRun(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	state := newRetentionStateForTest(t)
	var client fakeExtenderClient

	opts := retentionExtenderOptions{
		logger: logger,
		stats:  newCleanupStats(),
		state:  state,
		client: &client,
	}

	ch := make(chan retentionExtenderRequest)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)

		for range 100 {
			ch <- retentionExtenderRequest{
				object: objectVersion{},
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := newRetentionExtender(opts).run(t.Context(), ch); err != nil {
			t.Errorf("run() failed: %v", err)
		}
	}()

	wg.Wait()
}
