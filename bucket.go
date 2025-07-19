package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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

func (b *bucket) listObjectVersions(ctx context.Context, handler versionHandler) error {
	slog.InfoContext(ctx, "Listing object versions",
		slog.String("bucket", b.name),
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
