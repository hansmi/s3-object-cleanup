package main

import (
	"archive/tar"
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hansmi/s3-object-cleanup/internal/client"
	"github.com/hansmi/s3-object-cleanup/internal/state"
	"github.com/klauspost/compress/gzip"
)

const (
	reportObjectExpired  = "EXPIRED"
	reportObjectExtended = "EXTENDED"
)

var reportFields = []string{
	"Key",
	"Version ID",
	"Latest",
	"Delete marker",
	"Last modified",
	"Retain until",
	"Size",
	"Action",
	"Action data",
}

func formatReportTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.DateTime)
}

type reportObjectKey struct {
	key       string
	versionID string
}

func (ov objectVersion) reportKey() reportObjectKey {
	return reportObjectKey{
		key:       ov.key,
		versionID: ov.versionID,
	}
}

type reportObject struct {
	lastModified time.Time
	retainUntil  time.Time

	size int64

	isLatest     bool
	deleteMarker bool

	action     string
	actionData string
}

type reportBuilder struct {
	objects map[reportObjectKey]*reportObject
}

func newReportBuilder() *reportBuilder {
	return &reportBuilder{
		objects: map[reportObjectKey]*reportObject{},
	}
}

func (b *reportBuilder) discovered(ov objectVersion) error {
	key := ov.reportKey()

	if _, ok := b.objects[key]; ok {
		return fmt.Errorf("key %v already discovered previously", key)
	}

	b.objects[key] = &reportObject{
		lastModified: ov.lastModified,
		retainUntil:  ov.retainUntil,

		size: ov.size,

		isLatest:     ov.isLatest,
		deleteMarker: ov.deleteMarker,
	}

	return nil
}

func (b *reportBuilder) addExpired(versions []objectVersion) {
	for _, ov := range versions {
		key := ov.reportKey()

		b.objects[key].action = reportObjectExpired
	}
}

func (b *reportBuilder) addRetention(versions []retentionExtenderRequest) {
	for _, req := range versions {
		key := req.object.reportKey()

		o := b.objects[key]
		o.action = reportObjectExtended
		o.actionData = formatReportTime(req.until)
	}
}

func (b *reportBuilder) writeTo(w io.Writer) error {
	type row struct {
		*reportObjectKey
		*reportObject
	}

	rows := make([]row, 0, len(b.objects))

	for key, o := range b.objects {
		rows = append(rows, row{
			reportObjectKey: &key,
			reportObject:    o,
		})
	}

	slices.SortFunc(rows, func(a, b row) int {
		return cmp.Or(
			strings.Compare(a.key, b.key),
			a.lastModified.Compare(b.lastModified),
			strings.Compare(a.versionID, b.versionID),
		)
	})

	cw := csv.NewWriter(w)
	cw.Write(reportFields)

	var fields []string

	for _, i := range rows {
		fields = append(fields[:0],
			i.key,
			i.versionID,
			strconv.FormatBool(i.isLatest),
			strconv.FormatBool(i.deleteMarker),
			formatReportTime(i.lastModified),
			formatReportTime(i.retainUntil),
			strconv.FormatInt(i.size, 10),
			i.action,
			i.actionData,
		)

		if err := cw.Write(fields); err != nil {
			return err
		}
	}

	cw.Flush()

	return cw.Error()
}

type reportGroup struct {
	dir string
}

func newReportGroup(parent string) (*reportGroup, error) {
	dir, err := os.MkdirTemp(parent, "report*")
	if err != nil {
		return nil, err
	}

	return &reportGroup{
		dir: dir,
	}, nil
}

func (g *reportGroup) add(name string, b *reportBuilder) (err error) {
	dest := filepath.Join(g.dir, fmt.Sprintf("%s.csv", name))

	f, err := os.Create(dest)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, f.Close())
	}()

	return b.writeTo(f)
}

func (g *reportGroup) writeArchive(tmpdir string) (io.ReadCloser, error) {
	tmpfile, err := state.CreateUnlinkedTemp(tmpdir, "report*")
	if err != nil {
		return nil, err
	}

	zw := gzip.NewWriter(tmpfile)

	tw := tar.NewWriter(zw)

	if fs, err := os.OpenRoot(g.dir); err != nil {
		return nil, errors.Join(fmt.Errorf("dir: %w", err), tmpfile.Close())
	} else if err := tw.AddFS(fs.FS()); err != nil {
		return nil, errors.Join(fmt.Errorf("tar: %w", err), tmpfile.Close())
	}

	if err := tw.Close(); err != nil {
		return nil, errors.Join(fmt.Errorf("tar: %w", err), tmpfile.Close())
	}

	if err := zw.Close(); err != nil {
		return nil, errors.Join(fmt.Errorf("compression: %w", err), tmpfile.Close())
	}

	if _, err := tmpfile.Seek(0, os.SEEK_SET); err != nil {
		return nil, errors.Join(err, tmpfile.Close())
	}

	return tmpfile, nil
}

func uploadReportsToBucket(ctx context.Context, g *reportGroup, tmpdir string, c *client.Client, key string) (err error) {
	f, err := g.writeArchive(tmpdir)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, f.Close())
	}()

	return c.UploadObject(ctx, f, key)
}
