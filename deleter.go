package main

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

const batchSize = 250

type batchDeleterClient interface {
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type batchDeleterCheckFunc func(objectVersion) bool

type batchDeleterOptions struct {
	logger *slog.Logger
	stats  *cleanupStats
	client batchDeleterClient
	bucket string
	dryRun bool
}

type batchDeleter struct {
	logger  *slog.Logger
	stats   *cleanupStats
	dryRun  bool
	client  batchDeleterClient
	bucket  string
	workers int
}

func newBatchDeleter(opts batchDeleterOptions) *batchDeleter {
	return &batchDeleter{
		logger:  opts.logger,
		stats:   opts.stats,
		dryRun:  opts.dryRun,
		client:  opts.client,
		bucket:  opts.bucket,
		workers: 4,
	}
}

func (d *batchDeleter) deleteBatch(ctx context.Context, items []objectVersion) error {
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(d.bucket),
		Delete: &types.Delete{},
	}

	for _, i := range items {
		input.Delete.Objects = append(input.Delete.Objects, i.identifier())

		d.logger.InfoContext(ctx, "Delete",
			slog.Bool("dry_run", d.dryRun),
			slog.Any("object", i),
		)

		d.stats.addDelete(i)
	}

	if !d.dryRun {
		output, err := d.client.DeleteObjects(ctx, input)
		if err != nil {
			return err
		}

		d.stats.addDeleteResults(len(output.Deleted), len(output.Errors))

		for _, i := range output.Errors {
			d.logger.ErrorContext(ctx, "Delete failed",
				slog.String("key", aws.ToString(i.Key)),
				slog.String("version", aws.ToString(i.VersionId)),
				slog.String("code", aws.ToString(i.Code)),
				slog.String("msg", aws.ToString(i.Message)),
			)
		}
	}

	return nil
}

func collectDeletes(ch <-chan objectVersion) []objectVersion {
	pending := make([]objectVersion, 0, batchSize)

	for ov := range ch {
		pending = append(pending, ov)

		if len(pending) >= batchSize {
			break
		}
	}

	return pending
}

func (d *batchDeleter) run(ctx context.Context, in <-chan objectVersion) error {
	g, ctx := errgroup.WithContext(ctx)

	ch := make(chan []objectVersion, 8)

	for range max(1, d.workers) {
		g.Go(func() error {
			for items := range ch {
				if err := d.deleteBatch(ctx, items); err != nil {
					d.logger.Error("Batch deletion failed", slog.Any("error", err))
					d.stats.addDeleteResults(0, 1)
					continue
				}
			}

			return nil
		})
	}

	g.Go(func() error {
		defer close(ch)

		for {
			items := collectDeletes(in)

			if len(items) == 0 {
				return nil
			}

			ch <- items
		}
	})

	return g.Wait()
}
