package state

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
)

func CreateUnlinkedTemp(dir, pattern string) (*os.File, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}

	if err := os.Remove(f.Name()); err != nil {
		err = errors.Join(err, f.Close())
		return nil, err
	}

	return f, nil
}

// WriteCompressed writes a compressed database snapshot. Callers must close
// the returned reader.
func (s *Store) WriteCompressed(tmpdir string) (io.ReadCloser, error) {
	tmpfile, err := CreateUnlinkedTemp(tmpdir, "compressed*")
	if err != nil {
		return nil, err
	}

	zw := gzip.NewWriter(tmpfile)

	if _, err := s.WriteTo(zw); err != nil {
		return nil, errors.Join(fmt.Errorf("database snapshot: %w", err), tmpfile.Close())
	}

	if err := zw.Close(); err != nil {
		return nil, errors.Join(fmt.Errorf("compression: %w", err), tmpfile.Close())
	}

	if _, err := tmpfile.Seek(0, os.SEEK_SET); err != nil {
		return nil, errors.Join(err, tmpfile.Close())
	}

	return tmpfile, nil
}

// OpenCompressed decompresses the contents of a state database before opening
// it.
func OpenCompressed(tmpdir string, r io.Reader) (_ *Store, err error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("decompression: %w", err)
	}

	f, err := os.CreateTemp(tmpdir, "state*")
	if err != nil {
		return nil, err
	}

	defer func() {
		err = errors.Join(err, f.Close())
	}()

	if _, err := io.Copy(f, zr); err != nil {
		return nil, fmt.Errorf("copying: %w", err)
	}

	if err := zr.Close(); err != nil {
		return nil, fmt.Errorf("decompression: %w", err)
	}

	return Open(f.Name())
}
