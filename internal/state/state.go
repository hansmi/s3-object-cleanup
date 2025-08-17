package state

import (
	"fmt"
	"io"
	"os"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

type Store struct {
	db *bolthold.Store
}

func New(tmpdir string) (*Store, error) {
	f, err := os.CreateTemp(tmpdir, "state*")
	if err != nil {
		return nil, err
	}

	if err := f.Close(); err != nil {
		return nil, err
	}

	return Open(f.Name())
}

func Open(path string) (*Store, error) {
	db, err := bolthold.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("opening state %q: %w", path, err)
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// WriteTo writes the entire database to a writer.
func (s *Store) WriteTo(w io.Writer) (int64, error) {
	var n int64
	var err error

	err = s.db.Bolt().View(func(tx *bolt.Tx) error {
		n, err = tx.WriteTo(w)

		return err
	})

	return n, err
}
