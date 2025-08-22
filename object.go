package main

import (
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type objectVersion struct {
	key          string
	versionID    string
	lastModified time.Time
	isLatest     bool
	deleteMarker bool
	size         int64
	retainUntil  time.Time
}

var _ slog.LogValuer = (*objectVersion)(nil)

func (v objectVersion) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", v.key),
		slog.String("version", v.versionID),
		slog.Time("last_modified", v.lastModified),
		slog.Bool("delete_marker", v.deleteMarker),
		slog.Time("retain_until", v.retainUntil),
	)
}

func (v objectVersion) identifier() types.ObjectIdentifier {
	return types.ObjectIdentifier{
		Key:              aws.String(v.key),
		VersionId:        aws.String(v.versionID),
		LastModifiedTime: aws.Time(v.lastModified),
		Size:             aws.Int64(v.size),
	}
}
