package cli

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/logging"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	stdoutsink "github.com/avinash-gupta-rdz/rdstail/internal/sink/stdout"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// countingSink counts records that pass the upstream filters, for the
// end-of-run summary.
type countingSink struct {
	sink.Sink
	n atomic.Int64
}

func (c *countingSink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	c.n.Add(int64(len(records)))
	return c.Sink.Write(ctx, records)
}

// newDumpCmd is the one-shot incident fetch: pull a time window of logs for
// one or more instances, write NDJSON (gzipped if the path says so), exit.
// No config, no state, no sinks — read-only IAM is enough.
func newDumpCmd(logLevel *string) *cobra.Command {
	var (
		instances  []string
		region     string
		engine     string
		assumeRole string
		format     string
		grep       string
		since      time.Duration
		output     string
	)
	c := &cobra.Command{
		Use:   "dump",
		Short: "One-shot fetch of a time window of RDS logs — for postmortems and tickets.",
		Example: `  rdstail dump -i my-db --since 24h -o incident-4231.ndjson.gz
  rdstail dump -i my-db -i my-db-replica --since 6h --grep 'deadlock' -o dump.ndjson
  rdstail dump -i my-db --since 1h | jq .message`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(instances) == 0 {
				return fmt.Errorf("--instance is required")
			}
			if region == "" {
				region = os.Getenv("AWS_REGION")
			}
			if region == "" {
				return fmt.Errorf("--region is required (or set AWS_REGION)")
			}
			if since <= 0 {
				return fmt.Errorf("--since must be positive (e.g. 24h)")
			}
			var grepRE *regexp.Regexp
			if grep != "" {
				var err error
				if grepRE, err = regexp.Compile(grep); err != nil {
					return fmt.Errorf("--grep: %w", err)
				}
			}
			lg := logging.New(os.Stderr, *logLevel)
			logging.InstallDefault(lg)

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region, AssumeRole: assumeRole})
			if err != nil {
				return err
			}
			client := awsrds.NewFromConfig(awsCfg)
			cutoff := time.Now().UTC().Add(-since)

			// Resolve the output writer: stdout by default or with "-";
			// a path ending in .gz gets gzip-compressed.
			var w io.Writer = os.Stdout
			var closers []io.Closer
			if output != "" && output != "-" {
				f, err := os.Create(output)
				if err != nil {
					return err
				}
				closers = append(closers, f)
				w = f
				if strings.HasSuffix(output, ".gz") {
					gz := gzip.NewWriter(f)
					closers = append(closers, gz)
					w = gz
				}
			}

			out, err := stdoutsink.New("dump", format, w)
			if err != nil {
				return err
			}
			counter := &countingSink{Sink: out}
			var s sink.Sink = counter
			s = sink.WithGrep(s, grepRE)
			s = sink.WithSince(s, cutoff)

			filesDumped := 0
			for _, instance := range instances {
				eng := engine
				if eng == "" {
					if eng, err = detectEngine(ctx, client, instance, region); err != nil {
						return err
					}
					lg.Info("detected engine", "instance", instance, "engine", eng)
				}
				fetcher, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{
					API: client, InstanceID: instance, Engine: eng,
				})
				if err != nil {
					return err
				}
				files, err := fetcher.DiscoverFiles(ctx)
				if err != nil {
					return err
				}
				// Oldest first so the dump reads chronologically across
				// rotated files; files that predate the window are skipped
				// (keep files with no LastWritten — never drop on missing
				// metadata).
				sort.Slice(files, func(i, j int) bool {
					return files[i].LastWrittenMS < files[j].LastWrittenMS
				})
				for _, file := range files {
					if lw := file.LastWritten(); !lw.IsZero() && lw.Before(cutoff) {
						continue
					}
					marker := rdssrc.MarkerBeginning
					for {
						if err := ctx.Err(); err != nil {
							return err
						}
						chunk, err := fetcher.PullPortion(ctx, file.Name, marker)
						if err != nil {
							return err
						}
						for i := range chunk.Records {
							chunk.Records[i].Marker = chunk.NextMarker
							chunk.Records[i].BatchID = chunk.BatchID
						}
						if err := s.Write(ctx, chunk.Records); err != nil {
							return err
						}
						marker = chunk.NextMarker
						if !chunk.AdditionalPending {
							break
						}
					}
					filesDumped++
				}
			}

			// Flush the sink's buffer before the gzip/file closers underneath.
			if err := out.Close(); err != nil {
				return err
			}
			for i := len(closers) - 1; i >= 0; i-- {
				if err := closers[i].Close(); err != nil {
					return err
				}
			}
			dest := output
			if dest == "" || dest == "-" {
				dest = "stdout"
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "dumped %d records from %d file(s) across %d instance(s) (window: since %s) → %s\n",
				counter.n.Load(), filesDumped, len(instances), cutoff.Format(time.RFC3339), dest)
			return nil
		},
	}
	c.Flags().StringArrayVarP(&instances, "instance", "i", nil, "RDS instance identifier (repeatable)")
	c.Flags().StringVar(&region, "region", "", "AWS region (default: $AWS_REGION)")
	c.Flags().StringVar(&engine, "engine", "", "postgres|mysql|mariadb (default: auto-detect per instance)")
	c.Flags().StringVar(&assumeRole, "assume-role", "", "role ARN for cross-account access")
	c.Flags().StringVar(&format, "format", "ndjson", "output format: ndjson|text")
	c.Flags().StringVar(&grep, "grep", "", "only lines matching this regular expression (RE2)")
	c.Flags().DurationVar(&since, "since", 24*time.Hour, "how far back to fetch (e.g. 24h)")
	c.Flags().StringVarP(&output, "output", "o", "", "output path (.gz for gzip; default: stdout)")
	return c
}
