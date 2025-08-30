package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/hansmi/s3-object-cleanup/internal/client"
	"github.com/hansmi/s3-object-cleanup/internal/state"
)

// downloadStateFromBucket downloads a compressed state database snapshot from
// an S3 bucket.
func downloadStateFromBucket(ctx context.Context, tmpdir string, c *client.Client, key string) (_ *state.Store, err error) {
	tmpfile, err := state.CreateUnlinkedTemp(tmpdir, "download*")
	if err != nil {
		return nil, err
	}

	defer tmpfile.Close()

	if err := c.DownloadObject(ctx, tmpfile, key); err != nil {
		return nil, fmt.Errorf("object %q download: %w", key, err)
	}

	if _, err := tmpfile.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}

	return state.OpenCompressed(tmpdir, tmpfile)
}

// uploadStateToBucket uploads a compressed state database snapshot to an S3 bucket.
func uploadStateToBucket(ctx context.Context, s *state.Store, tmpdir string, c *client.Client, key string) (err error) {
	f, err := s.WriteCompressed(tmpdir)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, f.Close())
	}()

	return c.UploadObject(ctx, f, key)
}
