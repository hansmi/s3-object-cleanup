package state

import (
	"errors"
	"fmt"
	"time"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

const bucketMetadataKey = "metadata:v1"

type Bucket struct {
	db   *bolthold.Store
	name []byte
}

func (b *Bucket) get(tx *bolt.Tx) *bolt.Bucket {
	return tx.Bucket(b.name)
}

type bucketMetadata struct {
	Name   string
	SeenAt time.Time
}

func (s *Store) Bucket(name string) (*Bucket, error) {
	b := &Bucket{
		db:   s.db,
		name: []byte(name),
	}

	now := time.Now()

	if err := b.db.Bolt().Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(b.name)
		if err != nil {
			return nil
		}

		return b.db.UpsertBucket(bucket, bucketMetadataKey, bucketMetadata{
			Name:   name,
			SeenAt: now,
		})
	}); err != nil {
		return nil, fmt.Errorf("updating metadata: %w", err)
	}

	return b, nil
}

type objectRetentionRecordKey struct {
	Key       string
	VersionID string
}

type objectRetentionRecord struct {
	PK          objectRetentionRecordKey
	MTime       time.Time
	RetainUntil time.Time
}

func (b *Bucket) GetObjectRetention(key, versionID string) (time.Time, error) {
	pk := objectRetentionRecordKey{
		Key:       key,
		VersionID: versionID,
	}

	var record objectRetentionRecord

	if err := b.db.Bolt().View(func(tx *bolt.Tx) error {
		bucket := b.get(tx)

		if err := b.db.GetFromBucket(bucket, pk, &record); err != nil && !errors.Is(err, bolthold.ErrNotFound) {
			return err
		}

		return nil
	}); err != nil {
		return time.Time{}, err
	}

	return record.RetainUntil, nil
}

func (b *Bucket) SetObjectRetention(key, versionID string, until time.Time) error {
	record := objectRetentionRecord{
		PK: objectRetentionRecordKey{
			Key:       key,
			VersionID: versionID,
		},
		MTime:       time.Now(),
		RetainUntil: until,
	}

	return b.db.Bolt().Update(func(tx *bolt.Tx) error {
		bucket := b.get(tx)

		return b.db.UpsertBucket(bucket, record.PK, record)
	})
}

func (b *Bucket) DeleteObjectRetention(key, versionID string) error {
	pk := objectRetentionRecordKey{
		Key:       key,
		VersionID: versionID,
	}

	return b.db.Bolt().Update(func(tx *bolt.Tx) error {
		bucket := b.get(tx)

		if err := b.db.DeleteFromBucket(bucket, pk, objectRetentionRecord{}); err != nil && !errors.Is(err, bolthold.ErrNotFound) {
			return err
		}

		return nil
	})
}
