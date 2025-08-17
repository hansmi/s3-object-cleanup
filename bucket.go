package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type versionHandler interface {
	handleDeleteMarker(types.DeleteMarkerEntry) error
	handleVersion(types.ObjectVersion) error
}

type bucket struct {
	client *s3.Client
	name   string
	prefix string
}

func newBucketFromName(cfg aws.Config, input string) (*bucket, error) {
	result := &bucket{
		name: input,
	}

	var config []func(*s3.Options)

	if u, err := url.Parse(input); err == nil && u.IsAbs() {
		switch u.Scheme {
		case "http", "https":
		default:
			return nil, fmt.Errorf("%w: unrecognized scheme %q: %s", os.ErrInvalid, u.Scheme, u.Redacted())
		}

		result.name = strings.TrimLeft(u.Path, "/")

		if before, after, found := strings.Cut(result.name, "/"); found {
			result.name = before
			result.prefix = after
		}

		endpoint := (&url.URL{
			Scheme: u.Scheme,
			Host:   u.Host,
		}).String()

		config = append(config, func(opts *s3.Options) {
			opts.Region = "us-east-1"
			opts.BaseEndpoint = aws.String(endpoint)
			opts.EndpointOptions.DisableHTTPS = u.Scheme == "http"
		})
	}

	if result.name == "" {
		return nil, fmt.Errorf("%w: missing bucket name: %s", os.ErrInvalid, input)
	}

	result.client = s3.NewFromConfig(cfg, config...)

	return result, nil
}

func (b *bucket) listObjectVersions(ctx context.Context, logger *slog.Logger, handler versionHandler) error {
	logger.InfoContext(ctx, "Listing object versions",
		slog.String("prefix", b.prefix),
	)

	paginator := s3.NewListObjectVersionsPaginator(b.client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(b.name),
		Prefix: aws.String(b.prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, i := range page.Versions {
			if err := handler.handleVersion(i); err != nil {
				return fmt.Errorf("key %q version %q: %w", aws.ToString(i.Key), aws.ToString(i.VersionId), err)
			}
		}

		for _, i := range page.DeleteMarkers {
			if err := handler.handleDeleteMarker(i); err != nil {
				return fmt.Errorf("key %q version %q: %w", aws.ToString(i.Key), aws.ToString(i.VersionId), err)
			}
		}
	}

	return nil
}

func (b *bucket) downloadObject(ctx context.Context, w io.WriterAt, key string) error {
	downloader := manager.NewDownloader(b.client)

	_, err := downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	})

	return err
}

func (b *bucket) uploadObject(ctx context.Context, r io.Reader, key string) error {
	uploader := manager.NewUploader(b.client)

	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
		Body:   r,
	}); err != nil {
		return err
	}

	return s3.NewObjectExistsWaiter(b.client).Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	}, time.Minute)
}
