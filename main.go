package main

import (
	"context"
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
)

const minAgeDaysDefault = 32

type program struct {
	dryRun bool
	minAge time.Duration
}

func (p *program) registerFlags() {
	flag.BoolVar(&p.dryRun, "dry_run",
		mustGetenvBool("S3_OBJECT_CLEANUP_DRY_RUN", true),
		"Delete objects instead of only checking what would be done. Defaults to $S3_OBJECT_CLEANUP_DRY_RUN.")

	flag.DurationVar(&p.minAge, "min_age",
		mustGetenvDuration("S3_OBJECT_CLEANUP_MIN_AGE", minAgeDaysDefault*24*time.Hour),
		fmt.Sprintf("Minimum object version age. Defaults to $S3_OBJECT_CLEANUP_MIN_AGE or %d days.",
			minAgeDaysDefault))
}

func (p *program) run(ctx context.Context, bucketNames []string) error {
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

	var buckets []*bucket

	for _, i := range bucketNames {
		b, err := newBucketFromName(cfg, i)
		if err != nil {
			return err
		}

		buckets = append(buckets, b)
	}

	s := newCleanupStats()

	defer func() {
		slog.InfoContext(ctx, "Statistics", s.attrs()...)
	}()

	modifiedBefore := time.Now().Add(-p.minAge).Truncate(time.Minute)

	for _, b := range buckets {
		if err := cleanup(ctx, s, b, p.dryRun, modifiedBefore); err != nil {
			return fmt.Errorf("%s: %w", b.name, err)
		}
	}

	return nil
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
