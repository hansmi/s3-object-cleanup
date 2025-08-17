package state

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Errorf("New() failed: %v", err)
	}

	if err := s.db.Bolt().Sync(); err != nil {
		t.Errorf("Sync() failed: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}
}

func TestWriteTo(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Errorf("Open() failed: %v", err)
	}

	var buf bytes.Buffer

	if err := s.WriteTo(&buf); err != nil {
		t.Errorf("WriteTo() failed: %v", err)
	}

	if got, want := buf.Len(), 4*1024; got < want {
		t.Errorf("%d bytes written, want at least %d", got, want)
	}
}
