package main

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestVersionSeriesAdd(t *testing.T) {
	want := []objectVersion{
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

	for range len(want) {
		versions := slices.Clone(want)

		rand.Shuffle(len(versions), reflect.Swapper(versions))

		s := newVersionSeries(t.Name())

		for _, i := range versions {
			s.add(i)
		}

		if diff := cmp.Diff(want, s.items, cmp.AllowUnexported(objectVersion{})); diff != "" {
			t.Errorf("Versions diff (-want +got):\n%s", diff)
		}
	}
}

func TestVersionSeriesCheck(t *testing.T) {
	for _, tc := range []struct {
		name        string
		versions    []objectVersion
		cutoff      time.Time
		wantExpired []string
		wantExtend  []string
	}{
		{name: "empty"},
		{
			name: "no latest",
			versions: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2001, time.March, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "mar-1",
				},
				{
					lastModified: time.Date(2001, time.April, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "apr-1-del",
					deleteMarker: true,
				},
			},
			cutoff:     time.Date(2002, time.January, 1, 0, 0, 0, 0, time.UTC),
			wantExtend: []string{"jan-1", "mar-1", "apr-1-del"},
		},
		{
			name: "one",
			versions: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					isLatest:     true,
				},
			},
			cutoff:     time.Date(2002, time.January, 1, 0, 0, 0, 0, time.UTC),
			wantExtend: []string{"jan-1"},
		},
		{
			name: "recent delete marker",
			versions: []objectVersion{
				{
					lastModified: time.Date(2001, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1-del",
					isLatest:     true,
					deleteMarker: true,
				},
			},
			cutoff:     time.Date(2001, time.January, 30, 0, 0, 0, 0, time.UTC),
			wantExtend: []string{"feb-1-del"},
		},
		{
			name: "expired delete marker",
			versions: []objectVersion{
				{
					lastModified: time.Date(2001, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1-del",
					isLatest:     true,
					deleteMarker: true,
				},
			},
			cutoff:      time.Date(2001, time.August, 1, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"feb-1-del"},
		},
		{
			name: "expired delete marker",
			versions: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			cutoff:      time.Date(2001, time.December, 1, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"jan-1-del"},
		},
		{
			name: "expired delete marker before latest",
			versions: []objectVersion{
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
			cutoff:      time.Date(2002, time.December, 1, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"jan-1-del"},
			wantExtend:  []string{"feb-1"},
		},
		{
			name: "version before recent delete marker",
			versions: []objectVersion{
				{
					lastModified: time.Date(2003, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2003, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			cutoff:     time.Date(2003, time.January, 15, 0, 0, 0, 0, time.UTC),
			wantExtend: []string{"jan-1", "feb-1-del"},
		},
		{
			name: "version before expired delete marker",
			versions: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			cutoff:      time.Date(2004, time.June, 1, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"jan-1", "feb-1-del"},
		},
		{
			name: "two versions",
			versions: []objectVersion{
				{
					lastModified: time.Date(2004, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
				},
				{
					lastModified: time.Date(2004, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1",
					isLatest:     true,
				},
			},
			cutoff:      time.Date(2010, time.June, 1, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"jan-1"},
			wantExtend:  []string{"feb-1"},
		},
		{
			name: "two versions and delete marker",
			versions: []objectVersion{
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
			cutoff:      time.Date(2004, time.June, 1, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"jan-1", "feb-1", "mar-1-del"},
		},
		{
			name: "two versions with retention and delete marker",
			versions: []objectVersion{
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
			cutoff:      time.Date(2004, time.February, 25, 0, 0, 0, 0, time.UTC),
			wantExpired: []string{"jan-1"},
			wantExtend:  []string{"feb-1", "mar-1-del"},
		},
		{
			name: "retention not yet expired",
			versions: []objectVersion{
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
			cutoff:     time.Date(2004, time.March, 28, 0, 0, 0, 0, time.UTC),
			wantExtend: []string{"jan-1", "feb-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newVersionSeries(t.Name())

			for _, i := range tc.versions {
				s.add(i)
			}

			got := s.check(tc.cutoff)

			extract := func(versions []objectVersion) (result []string) {
				for _, i := range versions {
					result = append(result, i.versionID)
				}
				return
			}

			gotExpired := extract(got.expired)
			gotExtend := extract(got.extend)

			if diff := cmp.Diff(tc.wantExpired, gotExpired, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Expired versions diff (-want +got):\n%s", diff)
			}

			if diff := cmp.Diff(tc.wantExtend, gotExtend, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Extend versions diff (-want +got):\n%s", diff)
			}
		})
	}
}
