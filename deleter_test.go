package main

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestBatchDeleter(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name     string
		versions []objectVersion
	}{
		{
			name: "empty",
		},
		{
			name: "three",
			versions: []objectVersion{
				{key: "a"},
				{key: "b"},
				{key: "c"},
			},
		},
		{
			name: "many",
			versions: func() []objectVersion {
				var result []objectVersion

				for i := range (3 * batchSize * maxConcurrentDelete) + (batchSize / 3) {
					result = append(result, objectVersion{
						key: strconv.Itoa(i),
					})
				}

				return result
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)

			stats := newCleanupStats()

			b, err := newClientFromName(aws.Config{}, "test")
			if err != nil {
				t.Fatalf("newClientFromName() failed: %v", err)
			}

			d := newBatchDeleter(logger, stats, b, true)

			ch := make(chan objectVersion)

			go func() {
				defer close(ch)

				for _, i := range tc.versions {
					select {
					case <-ctx.Done():
						break
					case ch <- i:
					}
				}
			}()

			if err := d.run(ctx, ch); err != nil {
				t.Errorf("run() failed %v", err)
			}

			if got, want := stats.deleteCount, int64(len(tc.versions)); got != want {
				t.Errorf("deleteCount=%d, want %d", got, want)
			}
		})
	}
}
