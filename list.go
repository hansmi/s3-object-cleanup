package main

import (
	"context"
	"unique"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

type listHandler struct {
	out chan<- objectVersion
}

func newListHandler(out chan<- objectVersion) *listHandler {
	return &listHandler{
		out: out,
	}
}

// Best-effort string interning. Object keys occur multiple times if there are
// versions.
func (h *listHandler) internString(s *string) string {
	if s == nil {
		return ""
	}

	return unique.Make(*s).Value()
}

func (h *listHandler) handleVersion(ov types.ObjectVersion) {
	h.out <- objectVersion{
		key:          h.internString(ov.Key),
		versionID:    aws.ToString(ov.VersionId),
		lastModified: aws.ToTime(ov.LastModified),
		isLatest:     aws.ToBool(ov.IsLatest),
		size:         aws.ToInt64(ov.Size),
	}
}

func (h *listHandler) handleDeleteMarker(marker types.DeleteMarkerEntry) {
	h.out <- objectVersion{
		key:          h.internString(marker.Key),
		versionID:    aws.ToString(marker.VersionId),
		lastModified: aws.ToTime(marker.LastModified),
		isLatest:     aws.ToBool(marker.IsLatest),
		deleteMarker: true,
	}
}

func listObjectVersions(ctx context.Context, c s3.ListObjectVersionsAPIClient, bucket, prefix string, out chan<- objectVersion) error {
	paginator := s3.NewListObjectVersionsPaginator(c, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	ch := make(chan *s3.ListObjectVersionsOutput, 1)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(ch)

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return err
			}

			ch <- page
		}

		return nil
	})
	g.Go(func() error {
		handler := newListHandler(out)

		for page := range ch {
			for _, i := range page.Versions {
				handler.handleVersion(i)
			}

			for _, i := range page.DeleteMarkers {
				handler.handleDeleteMarker(i)
			}
		}

		return nil
	})

	return g.Wait()
}
