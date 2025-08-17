package state

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	bolterr "go.etcd.io/bbolt/errors"
)

func TestWriteCompressed(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Errorf("New() failed: %v", err)
	}

	r, err := s.WriteCompressed(t.TempDir())
	if err != nil {
		t.Errorf("WriteCompressed() failed: %v", err)
	}

	buf, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("ReadAll() failed: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	zr, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		t.Errorf("NewReader() failed: %v", err)
	}

	if _, err := io.ReadAll(zr); err != nil {
		t.Errorf("ReadAll() failed: %v", err)
	}
}

func TestOpenCompressed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		populate func(*testing.T, io.Writer)
		wantErr  error
	}{
		{
			name: "empty",
		},
		{
			name: "invalid content",
			populate: func(_ *testing.T, w io.Writer) {
				io.WriteString(w, "Not a database!")
			},
			wantErr: bolterr.ErrInvalid,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			zw := gzip.NewWriter(&buf)

			if tc.populate != nil {
				tc.populate(t, zw)
			}

			if err := zw.Close(); err != nil {
				t.Errorf("Close() failed: %v", err)
			}

			_, err := OpenCompressed(t.TempDir(), &buf)

			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Error diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Errorf("New() failed: %v", err)
	}

	r, err := s.WriteCompressed(t.TempDir())
	if err != nil {
		t.Errorf("WriteCompressed() failed: %v", err)
	}

	s2, err := OpenCompressed(t.TempDir(), r)
	if err != nil {
		t.Errorf("OpenCompressed() failed: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	if err := s2.db.Bolt().Sync(); err != nil {
		t.Errorf("Sync() failed: %v", err)
	}
}
