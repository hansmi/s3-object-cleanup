package main

import (
	"context"
	"errors"
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
	"github.com/aws/smithy-go"
)

const errorCodeNoSuchKey = "NoSuchKey"

type versionHandler interface {
	handleDeleteMarker(types.DeleteMarkerEntry) error
	handleVersion(types.ObjectVersion) error
}

type client struct {
	client *s3.Client
	name   string
	prefix string
}

func newClientFromName(cfg aws.Config, input string) (*client, error) {
	result := &client{
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

func (c *client) listObjectVersions(ctx context.Context, logger *slog.Logger, handler versionHandler) error {
	logger.InfoContext(ctx, "Listing object versions",
		slog.String("prefix", c.prefix),
	)

	paginator := s3.NewListObjectVersionsPaginator(c.client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(c.name),
		Prefix: aws.String(c.prefix),
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

func (c *client) downloadObject(ctx context.Context, w io.WriterAt, key string) error {
	downloader := manager.NewDownloader(c.client)

	_, err := downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(c.name),
		Key:    aws.String(key),
	})

	return err
}

func (c *client) uploadObject(ctx context.Context, r io.Reader, key string) error {
	uploader := manager.NewUploader(c.client)

	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.name),
		Key:    aws.String(key),
		Body:   r,
	}); err != nil {
		return err
	}

	return s3.NewObjectExistsWaiter(c.client).Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.name),
		Key:    aws.String(key),
	}, time.Minute)
}

type getObjectRetentionClient interface {
	GetObjectRetention(context.Context, *s3.GetObjectRetentionInput, ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error)
}

func getObjectRetentionImpl(ctx context.Context, c getObjectRetentionClient, bucket, key, versionID string) (_ time.Time, err error) {
	result, err := c.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		var errNoSuchKey *types.NoSuchKey
		var errApi smithy.APIError

		switch {
		case errors.As(err, &errNoSuchKey):
			fallthrough
		case errors.As(err, &errApi) && errApi.ErrorCode() == errorCodeNoSuchKey:
			err = nil
		}

		return time.Time{}, err
	}

	return aws.ToTime(result.Retention.RetainUntilDate), nil
}

func (c *client) getObjectRetention(ctx context.Context, key, versionID string) (_ time.Time, err error) {
	return getObjectRetentionImpl(ctx, c.client, c.name, key, versionID)
}
