package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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

func annotateError(err *error, format string, args ...any) {
	if *err != nil {
		prefix := fmt.Sprintf(format, args...)

		*err = fmt.Errorf("%s: %w", prefix, *err)
	}
}

func isNotExist(err error) bool {
	var errNoSuchKey *types.NoSuchKey
	var errApi smithy.APIError

	switch {
	case errors.As(err, &errNoSuchKey):
		return true
	case errors.As(err, &errApi) && errApi.ErrorCode() == errorCodeNoSuchKey:
		return true
	}

	return false
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

func (c *client) downloadObject(ctx context.Context, w io.WriterAt, key string) (err error) {
	defer annotateError(&err, "key %q", key)

	downloader := manager.NewDownloader(c.client)

	_, err = downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(c.name),
		Key:    aws.String(key),
	})

	return err
}

func (c *client) uploadObject(ctx context.Context, r io.Reader, key string) (err error) {
	defer annotateError(&err, "key %q", key)

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
	defer annotateError(&err, "key %q, version %q", key, versionID)

	result, err := c.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		if isNotExist(err) {
			// Version may have been deleted.
			err = nil
		}

		return time.Time{}, err
	}

	return aws.ToTime(result.Retention.RetainUntilDate), nil
}

func (c *client) getObjectRetention(ctx context.Context, key, versionID string) (time.Time, error) {
	return getObjectRetentionImpl(ctx, c.client, c.name, key, versionID)
}

type putObjectRetentionClient interface {
	PutObjectRetention(context.Context, *s3.PutObjectRetentionInput, ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error)
}

func putObjectRetentionImpl(ctx context.Context, c putObjectRetentionClient, bucket, key, versionID string, until time.Time) (err error) {
	defer annotateError(&err, "key %q, version %q", key, versionID)

	_, err = c.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
		Retention: &types.ObjectLockRetention{
			// TODO: ObjectLockRetentionMode may be required
			RetainUntilDate: aws.Time(until),
		},
	})
	if err != nil {
		if isNotExist(err) {
			// Version may have been deleted.
			err = nil
		}

		return err
	}

	return nil
}

func (c *client) putObjectRetention(ctx context.Context, key, versionID string, until time.Time) (err error) {
	return putObjectRetentionImpl(ctx, c.client, c.name, key, versionID, until)
}
