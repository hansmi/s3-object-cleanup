package state

import (
	"testing"
	"time"
)

func newBucketForTest(t *testing.T) *Bucket {
	t.Helper()

	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	b, err := s.Bucket("test")
	if err != nil {
		t.Fatalf("Bucket() failed: %v", err)
	}

	return b
}

func TestBucketGetObjectRetention(t *testing.T) {
	b := newBucketForTest(t)

	ts, err := b.GetObjectRetention("", "")
	if err != nil {
		t.Errorf("GetObjectRetention() failed: %v", err)
	}

	if !ts.IsZero() {
		t.Errorf("GetObjectRetention() returned non-zero time")
	}
}

func TestBucketSetObjectRetention(t *testing.T) {
	const (
		key     = "key"
		version = "ver123"
	)

	b := newBucketForTest(t)

	want := time.Date(2000, time.January, 1, 0, 1, 2, 3, time.UTC)

	err := b.SetObjectRetention(key, version, want)
	if err != nil {
		t.Errorf("SetObjectRetention() failed: %v", err)
	}

	got, err := b.GetObjectRetention(key, version)
	if err != nil {
		t.Errorf("GetObjectRetention() failed: %v", err)
	}

	if !want.Equal(got) {
		t.Errorf("GetObjectRetention() returned %v, want %v", got, want)
	}
}

func TestBucketDeleteObjectRetention(t *testing.T) {
	const (
		key     = "x"
		version = "y"
	)

	b := newBucketForTest(t)

	if err := b.DeleteObjectRetention("", ""); err != nil {
		t.Errorf("DeleteObjectRetention() failed: %v", err)
	}

	err := b.SetObjectRetention(key, version, time.Now())
	if err != nil {
		t.Errorf("SetObjectRetention() failed: %v", err)
	}

	if _, err := b.GetObjectRetention(key, version); err != nil {
		t.Errorf("GetObjectRetention() failed: %v", err)
	}

	if err := b.DeleteObjectRetention(key, version); err != nil {
		t.Errorf("DeleteObjectRetention() failed: %v", err)
	}

	if got, err := b.GetObjectRetention(key, version); err != nil {
		t.Errorf("GetObjectRetention() failed: %v", err)
	} else if !got.IsZero() {
		t.Errorf("GetObjectRetention() returned non-zero value after delete: %v", got)
	}
}
