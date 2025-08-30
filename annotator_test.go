package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hansmi/s3-object-cleanup/internal/state"
	"golang.org/x/sync/errgroup"
)

func newRetentionStateForTest(t *testing.T) *state.Bucket {
	t.Helper()

	s, err := state.New(t.TempDir())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	b, err := s.Bucket(t.Name())
	if err != nil {
		t.Fatalf("Bucket() failed: %v", err)
	}

	return b
}

type fakeRetentionClient struct {
	until time.Time
	err   error
}

func (c *fakeRetentionClient) GetObjectRetention(context.Context, string, string) (time.Time, error) {
	return c.until, c.err
}

func TestRetentionAnnotator(t *testing.T) {
	ctx := context.Background()

	want := time.Date(2001, time.January, 1, 2, 3, 0, 0, time.UTC)

	client := fakeRetentionClient{
		until: want,
	}

	a := newRetentionAnnotator(newRetentionStateForTest(t), &client)

	for range 5 {
		got, err := a.annotate(ctx, objectVersion{})
		if err != nil {
			t.Errorf("annotate() failed: %v", err)
		}

		if diff := cmp.Diff(want, got.retainUntil); diff != "" {
			t.Errorf("annotate() diff (-want +got):\n%s", diff)
		}

		// Value is cached after the first call.
		client.err = os.ErrInvalid
	}
}

func TestRetentionAnnotatorRun(t *testing.T) {
	ctx := context.Background()

	in := make(chan objectVersion)
	out := make(chan objectVersion)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(in)

		for i := range 10 {
			in <- objectVersion{
				key: fmt.Sprint(i),
			}
		}

		return nil
	})
	g.Go(func() error {
		defer close(out)

		client := fakeRetentionClient{
			until: time.Date(2003, time.June, 1, 2, 3, 0, 0, time.UTC),
		}

		a := newRetentionAnnotator(newRetentionStateForTest(t), &client)

		if err := a.run(ctx, in, out); err != nil {
			t.Errorf("run() failed: %v", err)
			return err
		}

		return nil
	})
	g.Go(func() error {
		var got []string

		for ov := range out {
			got = append(got, ov.key)
		}

		sort.Strings(got)

		want := strings.Split("0123456789", "")

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("run() output diff (-want +got):\n%s", diff)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		t.Errorf("Test failed: %v", err)
	}
}

func TestRetentionAnnotatorRunError(t *testing.T) {
	errTest := errors.New("test")

	in := make(chan objectVersion)
	out := make(chan objectVersion)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for range out {
		}
	}()

	finished := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(in)

		// Send objects until we're told to stop.
		for range 100 {
			select {
			case <-finished:
				return

			case in <- objectVersion{}:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(finished)
		defer close(out)

		client := fakeRetentionClient{
			err: errTest,
		}

		a := newRetentionAnnotator(newRetentionStateForTest(t), &client)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)

		err := a.run(ctx, in, out)

		if diff := cmp.Diff(errTest, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Error diff (-want +got):\n%s", diff)
		}
	}()

	wg.Wait()
}
