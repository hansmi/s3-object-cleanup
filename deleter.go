package main

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/hansmi/s3-object-cleanup/internal/client"
	"golang.org/x/sync/errgroup"
)

const batchSize = 250

type batchDeleter struct {
	logger     *slog.Logger
	stats      *cleanupStats
	dryRun     bool
	client     *s3.Client
	bucketName string
	workers    int
}

func newBatchDeleter(logger *slog.Logger, stats *cleanupStats, c *client.Client, dryRun bool) *batchDeleter {
	return &batchDeleter{
		logger:     logger,
		stats:      stats,
		dryRun:     dryRun,
		client:     c.S3(),
		bucketName: c.Name(),
		workers:    4,
	}
}

func (d *batchDeleter) deleteBatch(ctx context.Context, items []objectVersion) error {
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(d.bucketName),
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

func collectDeletes(ctx context.Context, ch <-chan objectVersion) ([]objectVersion, error) {
	pending := make([]objectVersion, 0, batchSize)

loop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case obj, ok := <-ch:
			if !ok {
				break loop
			}

			pending = append(pending, obj)

			if len(pending) >= batchSize {
				break loop
			}
		}
	}

	return pending, nil
}

func (d *batchDeleter) run(ctx context.Context, in <-chan objectVersion) error {
	g, ctx := errgroup.WithContext(ctx)

	ch := make(chan []objectVersion)

	for range max(1, d.workers) {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case items, ok := <-ch:
					if !ok {
						return nil
					}

					if err := d.deleteBatch(ctx, items); err != nil {
						return err
					}
				}
			}
		})
	}

	g.Go(func() error {
		defer close(ch)

		for {
			items, err := collectDeletes(ctx, in)
			if err != nil {
				return err
			}

			if len(items) == 0 {
				return nil
			}

			ch <- items
		}
	})

	return g.Wait()
}
