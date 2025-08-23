package main

import (
	"context"
	"log/slog"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

const (
	batchSize           = 250
	maxConcurrentDelete = 10
)

type batchDeleter struct {
	logger     *slog.Logger
	stats      *cleanupStats
	dryRun     bool
	client     *s3.Client
	bucketName string
}

func newBatchDeleter(logger *slog.Logger, stats *cleanupStats, c *client, dryRun bool) *batchDeleter {
	return &batchDeleter{
		logger:     logger,
		stats:      stats,
		dryRun:     dryRun,
		client:     c.client,
		bucketName: c.name,
	}
}

func (d *batchDeleter) deleteBatch(ctx context.Context, logger *slog.Logger, items []objectVersion) error {
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(d.bucketName),
		Delete: &types.Delete{},
	}

	for _, i := range items {
		input.Delete.Objects = append(input.Delete.Objects, i.identifier())

		logger.InfoContext(ctx, "Delete",
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
			logger.ErrorContext(ctx, "Delete failed",
				slog.String("key", aws.ToString(i.Key)),
				slog.String("version", aws.ToString(i.VersionId)),
				slog.String("code", aws.ToString(i.Code)),
				slog.String("msg", aws.ToString(i.Message)),
			)
		}
	}

	return nil
}

func (d *batchDeleter) run(ctx context.Context, ch <-chan objectVersion) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(1 + maxConcurrentDelete)

	var pending []objectVersion
	var batchCount int

	send := func(ctx context.Context, flush bool) error {
		for len(pending) > 0 {
			if len(pending) < batchSize && !flush {
				break
			}

			batch := pending[:min(len(pending), batchSize)]
			pending = slices.Clone(pending[len(batch):])

			logger := d.logger.With("batch", batchCount)

			g.Go(func() error {
				return d.deleteBatch(ctx, logger, batch)
			})

			batchCount++
		}

		return nil
	}

	g.Go(func() (err error) {
	loop:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case obj, ok := <-ch:
				if !ok {
					break loop
				}

				pending = append(pending, obj)

				if err := send(ctx, false); err != nil {
					return err
				}
			}
		}

		return send(ctx, true)
	})

	return g.Wait()
}
