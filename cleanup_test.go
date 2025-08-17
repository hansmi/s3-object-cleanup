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

func TestObjectVersionTrackerAppend(t *testing.T) {
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
		objects := slices.Clone(want)

		rand.Shuffle(len(objects), reflect.Swapper(objects))

		var tr objectVersionTracker

		for _, i := range objects {
			tr.append(i)
		}

		if diff := cmp.Diff(want, tr.versions, cmp.AllowUnexported(objectVersion{})); diff != "" {
			t.Errorf("Versions diff (-want +got):\n%s", diff)
		}
	}
}

func TestObjectVersionTrackerPopOldVersions(t *testing.T) {
	for _, tc := range []struct {
		name           string
		objects        []objectVersion
		modifiedBefore time.Time
		want           []string
	}{
		{name: "empty"},
		{
			name: "no latest",
			objects: []objectVersion{
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
			modifiedBefore: time.Date(2002, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "one",
			objects: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1",
					isLatest:     true,
				},
			},
			modifiedBefore: time.Date(2002, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "recent delete marker",
			objects: []objectVersion{
				{
					lastModified: time.Date(2001, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1-del",
					isLatest:     true,
					deleteMarker: true,
				},
			},
			modifiedBefore: time.Date(2001, time.January, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "expired delete marker",
			objects: []objectVersion{
				{
					lastModified: time.Date(2001, time.February, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "feb-1-del",
					isLatest:     true,
					deleteMarker: true,
				},
			},
			modifiedBefore: time.Date(2001, time.August, 1, 0, 0, 0, 0, time.UTC),
			want:           []string{"feb-1-del"},
		},
		{
			name: "expired delete marker",
			objects: []objectVersion{
				{
					lastModified: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
					versionID:    "jan-1-del",
					deleteMarker: true,
					isLatest:     true,
				},
			},
			modifiedBefore: time.Date(2001, time.December, 1, 0, 0, 0, 0, time.UTC),
			want:           []string{"jan-1-del"},
		},
		{
			name: "expired delete marker before latest",
			objects: []objectVersion{
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
			modifiedBefore: time.Date(2002, time.December, 1, 0, 0, 0, 0, time.UTC),
			want:           []string{"jan-1-del"},
		},
		{
			name: "version before recent delete marker",
			objects: []objectVersion{
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
			modifiedBefore: time.Date(2003, time.January, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "version before expired delete marker",
			objects: []objectVersion{
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
			modifiedBefore: time.Date(2004, time.June, 1, 0, 0, 0, 0, time.UTC),
			want:           []string{"jan-1", "feb-1-del"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tr objectVersionTracker

			for _, i := range tc.objects {
				tr.append(i)
			}

			var got []string

			for _, i := range tr.popOldVersions(tc.modifiedBefore) {
				got = append(got, i.versionID)
			}

			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Versions diff (-want +got):\n%s", diff)
			}
		})
	}
}
