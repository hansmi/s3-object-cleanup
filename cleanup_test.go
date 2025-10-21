package main

import (
	"fmt"
	"slices"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gonum.org/v1/gonum/stat/combin"
)

func TestVersionSeriesAdd(t *testing.T) {
	versions := []objectVersion{
		{
			lastModified: time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
			versionID:    "jan-1",
		},
		{
			lastModified: time.Date(2000, time.February, 1, 0, 0, 0, 0, time.UTC),
			versionID:    "feb-1",
		},
		{
			lastModified: time.Date(2000, time.February, 2, 0, 0, 0, 0, time.UTC),
			versionID:    "feb-2-del",
			deleteMarker: true,
		},
		{
			lastModified: time.Date(2000, time.March, 1, 0, 0, 0, 0, time.UTC),
			versionID:    "mar-1",
		},
		{
			lastModified: time.Date(2000, time.April, 1, 0, 0, 0, 0, time.UTC),
			versionID:    "apr-1-del",
			deleteMarker: true,
			isLatest:     true,
		},
	}

	for _, order := range combin.Permutations(len(versions), len(versions)) {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			t.Parallel()

			var selected []objectVersion

			for _, i := range order {
				selected = append(selected, versions[i])
			}

			for count := range len(selected) {
				s := newVersionSeries(t.Name())

				for _, ov := range selected[:count] {
					s.add(ov)
				}

				var want []objectVersion

				for _, i := range slices.Sorted(slices.Values(order[:count])) {
					want = append(want, versions[i])
				}

				if diff := cmp.Diff(want, s.items, cmpopts.EquateEmpty(), cmp.AllowUnexported(objectVersion{})); diff != "" {
					t.Errorf("Versions diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestVersionSeriesFinalize(t *testing.T) {
	for _, tc := range []struct {
		name           string
		items          []objectVersion
		now            time.Time
		minRetention   time.Duration
		minDeletionAge time.Duration
		wantRetention  map[string]time.Time
		wantExpired    []string
	}{
		{name: "empty"},
		{
			name: "no latest",
			items: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2001, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1",
					retainUntil:  time.Date(2001, time.July, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2001, time.April, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "apr-1",
				},
			},
			now:            time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 999 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"jan-1": time.Date(2001, time.January, 11, 0, 0, 0, 0, time.UTC),
				"apr-1": time.Date(2001, time.April, 11, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "no latest with delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					retainUntil:  time.Date(2001, time.July, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2001, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1-del",
					deleteMarker: true,
				},
				{
					lastModified: time.Date(2001, time.April, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "apr-1",
				},
				{
					lastModified: time.Date(2001, time.May, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "may-1",
					retainUntil:  time.Date(2001, time.May, 7, 0, 0, 0, 0, time.UTC),
				},
			},
			now:            time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   11 * 24 * time.Hour,
			minDeletionAge: 999 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"apr-1": time.Date(2001, time.April, 12, 0, 0, 0, 0, time.UTC),
				"may-1": time.Date(2001, time.May, 12, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "one",
			items: []objectVersion{
				{
					lastModified: time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					isLatest:     true,
				},
			},
			now:            time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   100 * 24 * time.Hour,
			minDeletionAge: 999 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"jan-1": time.Date(2003, time.April, 11, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "current delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1-del",
					isLatest:     true,
					deleteMarker: true,
				},
			},
			now:            time.Date(2003, time.January, 14, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
		},
		{
			name: "expired delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1-del",
					isLatest:     true,
					deleteMarker: true,
				},
			},
			now:            time.Date(2003, time.March, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantExpired:    []string{"jan-1-del"},
		},
		{
			name: "expired delete marker before latest",
			items: []objectVersion{
				{
					lastModified: time.Date(2002, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1-del",
					deleteMarker: true,
				},
				{
					lastModified: time.Date(2002, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
					isLatest:     true,
				},
			},
			now:            time.Date(2002, time.September, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"feb-1": time.Date(2002, time.September, 11, 0, 0, 0, 0, time.UTC),
			},
			wantExpired: []string{"jan-1-del"},
		},
		{
			name: "versions before recent delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2003, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
				},
				{
					lastModified: time.Date(2003, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			now:            time.Date(2003, time.March, 15, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"feb-1": time.Date(2003, time.March, 21, 0, 0, 0, 0, time.UTC),
			},
			wantExpired: []string{"jan-1"},
		},
		{
			name: "versions before expired delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2003, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
				},
				{
					lastModified: time.Date(2003, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			now:            time.Date(2003, time.March, 21, 1, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantExpired:    []string{"jan-1", "feb-1", "mar-1-del"},
		},
		{
			name: "two versions",
			items: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					retainUntil:  time.Date(2008, time.March, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
					isLatest:     true,
				},
			},
			now:            time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"feb-1": time.Date(2010, time.January, 11, 0, 0, 0, 0, time.UTC),
			},
			wantExpired: []string{"jan-1"},
		},
		{
			name: "two versions and delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
				},
				{
					lastModified: time.Date(2004, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			now:            time.Date(2004, time.June, 1, 0, 0, 0, 0, time.UTC),
			minRetention:   12 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantExpired:    []string{"jan-1", "feb-1", "mar-1-del"},
		},
		{
			name: "two versions with retention and delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					retainUntil:  time.Date(2004, time.January, 15, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
					retainUntil:  time.Date(2004, time.February, 15, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2004, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			now:            time.Date(2004, time.March, 2, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"feb-1": time.Date(2004, time.March, 21, 0, 0, 0, 0, time.UTC),
			},
			wantExpired: []string{"jan-1"},
		},
		{
			name: "retention not yet expired",
			items: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					retainUntil:  time.Date(2004, time.April, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
					isLatest:     true,
				},
			},
			now:            time.Date(2004, time.March, 28, 0, 0, 0, 0, time.UTC),
			minRetention:   12 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"feb-1": time.Date(2004, time.April, 9, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "version after delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1-del",
					deleteMarker: true,
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
					isLatest:     true,
				},
			},
			now:            time.Date(2004, time.March, 28, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantRetention: map[string]time.Time{
				"feb-1": time.Date(2004, time.April, 7, 0, 0, 0, 0, time.UTC),
			},
			wantExpired: []string{"jan-1-del"},
		},
		{
			name: "version and delete marker",
			items: []objectVersion{
				{
					lastModified: time.Date(2025, time.August, 29, 0, 0, 0, 0, time.UTC),
					versionID:    "aug-29",
					retainUntil:  time.Date(2025, time.October, 16, 0, 0, 0, 0, time.UTC),
				},
				{
					lastModified: time.Date(2025, time.August, 30, 0, 0, 0, 0, time.UTC),
					versionID:    "aug-30-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			now:            time.Date(2025, time.October, 22, 0, 0, 0, 0, time.UTC),
			minRetention:   10 * 24 * time.Hour,
			minDeletionAge: 20 * 24 * time.Hour,
			wantExpired:    []string{"aug-29", "aug-30-del"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newVersionSeries(t.Name())

			for _, i := range tc.items {
				s.add(i)
			}

			got := s.finalize(versionSeriesFinalizeOptions{
				now:            tc.now,
				minRetention:   tc.minRetention,
				minDeletionAge: tc.minDeletionAge,
			})

			gotRetention := map[string]time.Time{}

			for _, req := range got.retention {
				gotRetention[req.object.versionID] = req.until
			}

			var gotExpired []string

			for _, ov := range got.expired {
				gotExpired = append(gotExpired, ov.versionID)
			}

			if intersection := set.NewSet(gotExpired...).Intersect(
				set.NewSetFromMapKeys(gotRetention),
			); !intersection.IsEmpty() {
				t.Errorf("Retained and deleted versions intersect: %q", set.Sorted(intersection))
			}

			if diff := cmp.Diff(tc.wantRetention, gotRetention, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Retention diff (-want +got):\n%s", diff)
			}

			if diff := cmp.Diff(tc.wantExpired, gotExpired, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Expired versions diff (-want +got):\n%s", diff)
			}
		})
	}
}
