package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReport(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objects []objectVersion
		want    [][]string
	}{
		{
			name: "empty",
			want: [][]string{
				reportFields,
			},
		},
		{
			name: "simple",
			objects: []objectVersion{
				{key: "k1", versionID: "v3", isLatest: true},
				{key: "k2", versionID: "v1"},
				{key: "k1", versionID: "v1"},
				{key: "k3", versionID: "v1", deleteMarker: true},
				{key: "k1", versionID: "v2"},
				{key: "sized", size: 1234},
				{
					key:          "times",
					lastModified: time.Date(2000, time.February, 2, 0, 0, 0, 0, time.UTC),
					retainUntil:  time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			want: [][]string{
				reportFields,
				{"k1", "v1", "false", "false", "", "", "0", "", ""},
				{"k1", "v2", "false", "false", "", "", "0", "", ""},
				{"k1", "v3", "true", "false", "", "", "0", "", ""},
				{"k2", "v1", "false", "false", "", "", "0", "", ""},
				{"k3", "v1", "false", "true", "", "", "0", "", ""},
				{"sized", "", "false", "false", "", "", "1234", "", ""},
				{
					"times", "", "false", "false", "2000-02-02 00:00:00", "2001-01-01 00:00:00",
					"0", "", "",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newReportBuilder()

			for _, ov := range tc.objects {
				if err := b.discovered(ov); err != nil {
					t.Errorf("discovered(%v) failed: %v", ov, err)
				}
			}

			var buf bytes.Buffer

			if err := b.writeTo(&buf); err != nil {
				t.Errorf("writeTo() failed: %v", err)
			}

			got, err := csv.NewReader(&buf).ReadAll()
			if err != nil {
				t.Errorf("Reading failed: %v", err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Records diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReportGroup(t *testing.T) {
	g, err := newReportGroup(t.TempDir())
	if err != nil {
		t.Errorf("newReportBuilder(): %v", err)
	}

	f, err := g.writeArchive(t.TempDir())
	if err != nil {
		t.Errorf("writeArchive(): %v", err)
	}

	if err := f.Close(); err != nil {
		t.Errorf("Close(): %v", err)
	}

	for i := range 10 {
		if err := g.add(fmt.Sprintf("report%d", i), newReportBuilder()); err != nil {
			t.Errorf("add(): %v", err)
		}
	}

	f, err = g.writeArchive(t.TempDir())
	if err != nil {
		t.Errorf("writeArchive(): %v", err)
	}

	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Errorf("NewReader(): %v", err)
	}

	got := map[string]string{}

	for tr := tar.NewReader(zr); ; {
		fmt.Printf("###\n")
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Errorf("Next(): %v", err)
		}

		var contents bytes.Buffer

		if _, err := io.Copy(&contents, tr); err != nil {
			t.Errorf("Copy(): %v", err)
		}

		got[hdr.Name] = contents.String()
	}

	if err := f.Close(); err != nil {
		t.Errorf("Close(): %v", err)
	}
}
