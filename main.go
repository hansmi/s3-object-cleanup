package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go/logging"
	"github.com/hansmi/s3-object-cleanup/internal/client"
	"github.com/hansmi/s3-object-cleanup/internal/env"
	"github.com/hansmi/s3-object-cleanup/internal/state"
)

const minAgeDaysDefault = 32
const defaultMinRetentionDays = 32
const defaultMinRetentionThresholdDays = defaultMinRetentionDays / 4

type program struct {
	dryRun bool
	minAge time.Duration

	persistenceBucket string

	minRetention          time.Duration
	minRetentionThreshold time.Duration
}

func (p *program) registerFlags() {
	flag.BoolVar(&p.dryRun, "dry_run",
		env.MustGetBool("S3_OBJECT_CLEANUP_DRY_RUN", true),
		"Perform a trial run without actually deleting objects. Defaults to $S3_OBJECT_CLEANUP_DRY_RUN.")

	flag.DurationVar(&p.minAge, "min_age",
		env.MustGetDuration("S3_OBJECT_CLEANUP_MIN_AGE", minAgeDaysDefault*24*time.Hour),
		fmt.Sprintf("Minimum object version age before considering for deletion. Defaults to $S3_OBJECT_CLEANUP_MIN_AGE or %d days.",
			minAgeDaysDefault))

	flag.DurationVar(&p.minRetention, "min_retention",
		env.MustGetDuration("S3_OBJECT_CLEANUP_MIN_RETENTION", defaultMinRetentionDays*24*time.Hour),
		fmt.Sprintf("Set or extend the retention of object versions to be at least the given amount of time. Defaults to $S3_OBJECT_CLEANUP_MIN_RETENTION or %d days.",
			defaultMinRetentionDays))

	flag.DurationVar(&p.minRetentionThreshold, "min_retention_threshold",
		env.MustGetDuration("S3_OBJECT_CLEANUP_MIN_RETENTION_THRESHOLD", defaultMinRetentionThresholdDays*24*time.Hour),
		fmt.Sprintf("Object version retention is set when it's missing or the remaining amount of time falls below the given value. Defaults to $S3_OBJECT_CLEANUP_MIN_RETENTION_THRESHOLD or %d days.",
			defaultMinRetentionThresholdDays))

	flag.StringVar(&p.persistenceBucket, "persistence_bucket",
		env.GetWithFallback("S3_OBJECT_CLEANUP_PERSISTENCE_BUCKET", ""),
		`URL to an S3 bucket for storing a information reducing API calls. Defaults to $S3_OBJECT_CLEANUP_PERSISTENCE_BUCKET.`)
}

func (p *program) run(ctx context.Context, bucketNames []string) (err error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithLogger(logging.StandardLogger{
			Logger: slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug),
		}),
		config.WithClientLogMode(
			aws.LogRequest|aws.LogResponse|aws.LogDeprecatedUsage,
		),
	)

	if err != nil {
		return err
	}

	var clients []*client.Client

	for _, i := range bucketNames {
		c, err := client.NewFromName(cfg, i)
		if err != nil {
			return err
		}

		clients = append(clients, c)
	}

	if p.minRetentionThreshold > p.minRetention {
		return fmt.Errorf("min_retention_threshold (%v) may not exceed min_retention (%v)",
			p.minRetentionThreshold.String(), p.minRetention.String())
	}

	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, os.RemoveAll(tmpdir))
	}()

	var s *state.Store
	var persistState func(context.Context) error

	if p.persistenceBucket != "" {
		const key = "state.gz"

		c, err := client.NewFromName(cfg, p.persistenceBucket)
		if err != nil {
			return err
		}

		if s, err = downloadStateFromBucket(ctx, tmpdir, c, key); err != nil {
			slog.Warn("Restoring state failed", slog.Any("error", err))
			s = nil
		}

		persistState = func(ctx context.Context) error {
			return uploadStateToBucket(ctx, s, tmpdir, c, key)
		}
	}

	if s == nil {
		s, err = state.New(tmpdir)
		if err != nil {
			return fmt.Errorf("initializing state: %w", err)
		}
	}

	stats := newCleanupStats()

	defer func() {
		slog.InfoContext(ctx, "Statistics", stats.attrs()...)
	}()

	minModTime := time.Now().Add(-p.minAge).Truncate(time.Minute)

	var bucketErrors []error

	for _, c := range clients {
		logger := slog.With(slog.String("bucket", c.Name()))

		if err := cleanup(ctx, cleanupOptions{
			logger:                logger,
			stats:                 stats,
			state:                 s,
			client:                c,
			dryRun:                p.dryRun,
			minModTime:            minModTime,
			minRetention:          p.minRetention,
			minRetentionThreshold: p.minRetentionThreshold,
		}); err != nil {
			logger.Error("Cleanup failed", slog.Any("error", err))

			bucketErrors = append(bucketErrors, fmt.Errorf("%s: %w", c.Name(), err))
		}
	}

	if persistState != nil {
		if err := persistState(ctx); err != nil {
			bucketErrors = append(bucketErrors, fmt.Errorf("persisting state: %w", err))
		}
	}

	return errors.Join(bucketErrors...)
}

func main() {
	flag.Usage = func() {
		w := flag.CommandLine.Output()

		fmt.Fprintf(w, "Usage: %s [bucket...]\n", os.Args[0])
		fmt.Fprintln(w, `
Remove non-current object versions from S3 buckets. Buckets may be specified as
arguments and via $S3_OBJECT_CLEANUP_BUCKETS (separated by whitespace).

Flags:`)
		flag.PrintDefaults()
	}

	debug := flag.Bool("debug", false, "Enable debug logging.")

	var logLevel slog.LevelVar

	logHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: &logLevel,
	})
	slog.SetDefault(slog.New(logHandler))

	var p program

	p.registerFlags()

	flag.Parse()

	if *debug {
		logLevel.Set(slog.LevelDebug)
	}

	buckets := strings.Fields(os.Getenv("S3_OBJECT_CLEANUP_BUCKETS"))
	buckets = append(buckets, flag.Args()...)

	if err := p.run(context.Background(), buckets); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
