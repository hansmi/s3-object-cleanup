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
	"github.com/hansmi/s3-object-cleanup/internal/state"
)

const minAgeDaysDefault = 32

type program struct {
	dryRun bool
	minAge time.Duration

	persistenceBucket string
}

func (p *program) registerFlags() {
	flag.BoolVar(&p.dryRun, "dry_run",
		mustGetenvBool("S3_OBJECT_CLEANUP_DRY_RUN", true),
		"Perform a trial run without actually deleting objects. Defaults to $S3_OBJECT_CLEANUP_DRY_RUN.")

	flag.DurationVar(&p.minAge, "min_age",
		mustGetenvDuration("S3_OBJECT_CLEANUP_MIN_AGE", minAgeDaysDefault*24*time.Hour),
		fmt.Sprintf("Minimum object version age. Defaults to $S3_OBJECT_CLEANUP_MIN_AGE or %d days.",
			minAgeDaysDefault))

	flag.StringVar(&p.persistenceBucket, "persistence_bucket",
		getenvWithFallback("S3_OBJECT_CLEANUP_PERSISTENCE_BUCKET", ""),
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

	var clients []*client

	for _, i := range bucketNames {
		c, err := newClientFromName(cfg, i)
		if err != nil {
			return err
		}

		clients = append(clients, c)
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

		c, err := newClientFromName(cfg, p.persistenceBucket)
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

	modifiedBefore := time.Now().Add(-p.minAge).Truncate(time.Minute)

	var bucketErrors []error

	for _, c := range clients {
		logger := slog.With(slog.String("bucket", c.name))

		if err := cleanup(ctx, cleanupOptions{
			logger:         logger,
			stats:          stats,
			state:          s,
			client:         c,
			dryRun:         p.dryRun,
			modifiedBefore: modifiedBefore,
		}); err != nil {
			logger.Error("Cleanup failed", slog.Any("error", err))

			bucketErrors = append(bucketErrors, fmt.Errorf("%s: %w", c.name, err))
		}
	}

	if err := persistState(ctx); err != nil {
		bucketErrors = append(bucketErrors, fmt.Errorf("persisting state: %w", err))
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
