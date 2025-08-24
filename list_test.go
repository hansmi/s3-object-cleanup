package main

import (
	stdcmp "cmp"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-cmp/cmp"
)

func sortObjectVersions(versions []objectVersion) {
	slices.SortFunc(versions, func(a, b objectVersion) int {
		return stdcmp.Or(
			strings.Compare(a.key, b.key),
			strings.Compare(a.versionID, b.versionID),
		)
	})
}

func formatMiB(b int64) string {
	return fmt.Sprintf("%.1f MiB", float64(b)/1024/1024)
}

func TestListHandler(t *testing.T) {
	ch := make(chan objectVersion)

	var wg sync.WaitGroup
	var got []objectVersion

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := range ch {
			got = append(got, i)
		}
	}()

	h := newListHandler(ch)
	h.handleVersion(types.ObjectVersion{
		Key:       aws.String("k1"),
		VersionId: aws.String("v2"),
	})
	h.handleDeleteMarker(types.DeleteMarkerEntry{
		Key:       aws.String("k1"),
		VersionId: aws.String("del"),
	})
	h.handleVersion(types.ObjectVersion{
		Key:       aws.String("k2"),
		VersionId: aws.String("v2"),
	})
	h.handleVersion(types.ObjectVersion{
		Key:       aws.String("k2"),
		VersionId: aws.String("v1"),
	})

	close(ch)

	wg.Wait()

	sortObjectVersions(got)

	want := []objectVersion{
		{key: "k1", versionID: "del", deleteMarker: true},
		{key: "k1", versionID: "v2"},
		{key: "k2", versionID: "v1"},
		{key: "k2", versionID: "v2"},
	}

	if diff := cmp.Diff(want, got, cmp.AllowUnexported(objectVersion{})); diff != "" {
		t.Errorf("ListHandler diff (-want +got):\n%s", diff)
	}
}

func TestListHandlerInternString(t *testing.T) {
	var before, after runtime.MemStats

	const distinctValues = 1000
	const repetitions = 100

	stringSize := int64(reflect.TypeOf("").Size())
	got := make([]string, distinctValues*repetitions)
	h := newListHandler(nil)

	var heapEstimate int64

	runtime.GC()
	runtime.ReadMemStats(&before)

	{
		want := make([]string, distinctValues*repetitions)

		// Generate unique strings.
		for idx := range distinctValues {
			value := strings.Repeat(strconv.Itoa(idx), 100)
			heapEstimate += stringSize + int64(len(value))

			want[idx] = value
		}

		// Intern strings.
		for idx := range len(want) {
			value := want[idx%distinctValues]

			want[idx] = value

			// Values must have a unique memory location.
			value = strings.Clone(value)

			got[idx] = h.internString(&value)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Result diff (-want +got):\n%s", diff)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	runtime.KeepAlive(h)
	runtime.KeepAlive(got)

	heapDiff := int64(after.HeapAlloc) - int64(before.HeapAlloc)

	t.Logf("Heap before: %s, after: %s, diff: %s, estimate: %s",
		formatMiB(int64(before.HeapAlloc)),
		formatMiB(int64(after.HeapAlloc)),
		formatMiB(heapDiff),
		formatMiB(heapEstimate))

	if heapDiff > 0 && heapDiff > 2*heapEstimate {
		t.Errorf("Heap increase of %s is more than twice the estimate of %s",
			formatMiB(heapDiff),
			formatMiB(heapEstimate))
	}
}

type fakeListObjectVersionsAPIClient struct {
	offset  int
	results []*s3.ListObjectVersionsOutput
}

func (c *fakeListObjectVersionsAPIClient) ListObjectVersions(_ context.Context, _ *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	var result *s3.ListObjectVersionsOutput

	if c.offset < len(c.results) {
		result = c.results[c.offset]
		c.offset++
	}

	if result == nil {
		result = &s3.ListObjectVersionsOutput{
			IsTruncated: aws.Bool(false),
		}
	}

	return result, nil
}

func TestListObjectVersions(t *testing.T) {
	ctx := context.Background()

	var c fakeListObjectVersionsAPIClient

	var want []objectVersion

	for pageIdx := range 10 {
		page := &s3.ListObjectVersionsOutput{
			IsTruncated:   aws.Bool(true),
			KeyMarker:     aws.String(fmt.Sprint(pageIdx)),
			NextKeyMarker: aws.String(fmt.Sprint(pageIdx + 1)),
		}

		for i := range 100 {
			key := fmt.Sprintf("key%d", i)
			version := fmt.Sprintf("v%d", pageIdx+i)

			if i%17 == 0 {
				page.DeleteMarkers = append(page.DeleteMarkers, types.DeleteMarkerEntry{
					Key:       aws.String(key),
					VersionId: aws.String(version),
				})
				want = append(want, objectVersion{
					key:          key,
					versionID:    version,
					deleteMarker: true,
				})
			} else {
				page.Versions = append(page.Versions, types.ObjectVersion{
					Key:       aws.String(key),
					VersionId: aws.String(version),
				})
				want = append(want, objectVersion{
					key:       key,
					versionID: version,
				})
			}
		}

		c.results = append(c.results, page)
	}

	ch := make(chan objectVersion)

	var wg sync.WaitGroup
	var got []objectVersion

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := range ch {
			got = append(got, i)
		}
	}()

	if err := listObjectVersions(ctx, &c, "bucket", "prefix", ch); err != nil {
		t.Errorf("listObjectversions() failed: %v", err)
	}

	close(ch)

	wg.Wait()

	sortObjectVersions(want)
	sortObjectVersions(got)

	if diff := cmp.Diff(want, got, cmp.AllowUnexported(objectVersion{})); diff != "" {
		t.Errorf("ListHandler diff (-want +got):\n%s", diff)
	}
}
