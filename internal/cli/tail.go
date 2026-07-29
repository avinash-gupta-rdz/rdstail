package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/logging"
	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	stdoutsink "github.com/avinash-gupta-rdz/rdstail/internal/sink/stdout"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	statememory "github.com/avinash-gupta-rdz/rdstail/internal/state/memory"
)

// detectEngine resolves an instance's engine via DescribeDBInstances and
// normalises it to postgres|mysql|mariadb. Errors if the instance is missing
// or runs an engine rdstail cannot tail.
func detectEngine(ctx context.Context, client *awsrds.Client, instance, region string) (string, error) {
	resp, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instance),
	})
	if err != nil {
		return "", fmt.Errorf("describe %s: %w", instance, err)
	}
	if len(resp.DBInstances) == 0 {
		return "", fmt.Errorf("instance %q not found in %s", instance, region)
	}
	apiEngine := aws.ToString(resp.DBInstances[0].Engine)
	engine := rdssrc.NormalizeEngine(apiEngine)
	if engine == "" {
		return "", fmt.Errorf("instance %q runs %q, which rdstail cannot tail", instance, apiEngine)
	}
	return engine, nil
}

// newTailCmd is the zero-config one-liner: stream instances' logs to stdout,
// tail -f style. State is in-memory (no resume — that's `run`'s job).
func newTailCmd(logLevel *string) *cobra.Command {
	var (
		instances   []string
		region      string
		engine      string
		assumeRole  string
		format      string
		minSeverity string
		grep        string
		since       time.Duration
		poll        time.Duration
		beginning   bool
	)
	c := &cobra.Command{
		Use:   "tail",
		Short: "Stream RDS instances' logs to stdout — tail -f, no config file.",
		Example: `  rdstail tail -i my-db --region ap-south-1
  rdstail tail -i my-db --region us-east-1 --min-severity ERROR
  rdstail tail -i my-db --since 1h --grep 'deadlock|timeout' --region us-east-1
  rdstail tail -i my-db -i my-db-replica --region eu-west-1 --format ndjson | jq .message`,
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
			if minSeverity != "" && sink.SeverityRank(minSeverity) == 0 {
				return fmt.Errorf("--min-severity: unknown severity %q", minSeverity)
			}
			if since > 0 && beginning {
				return fmt.Errorf("--since and --from-beginning are mutually exclusive")
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

			out, err := stdoutsink.New("tail", format, nil)
			if err != nil {
				return err
			}
			var s sink.Sink = out
			if minSeverity != "" {
				s = sink.WithFilter(s, sink.FilterConfig{MinSeverity: minSeverity})
			}
			s = sink.WithGrep(s, grepRE)

			// --since N: replay the window (files that predate it are skipped,
			// records inside kept files are cut at the boundary), then keep
			// following. Without it, start at the tail unless --from-beginning.
			var cutoff time.Time
			startFrom := config.StartFromEnd
			if since > 0 {
				cutoff = time.Now().UTC().Add(-since)
				s = sink.WithSince(s, cutoff)
				startFrom = config.StartFromBeginning
			} else if beginning {
				startFrom = config.StartFromBeginning
			}

			workers := make([]*pipeline.InstanceWorker, 0, len(instances))
			for _, instance := range instances {
				// Engine drives log-file classification and metadata
				// extraction; auto-detect per instance when not given.
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
				worker, err := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
					Fetcher:      fetcher,
					Store:        statememory.New(),
					Sink:         s,
					Logger:       lg,
					PollInterval: poll,
					StartFrom:    startFrom,
					MinFileTime:  cutoff,
				})
				if err != nil {
					return err
				}
				workers = append(workers, worker)
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "tailing %s in %s — Ctrl-C to stop\n",
				strings.Join(instances, ", "), region)
			var wg sync.WaitGroup
			for _, worker := range workers {
				wg.Add(1)
				go func(w *pipeline.InstanceWorker) {
					defer wg.Done()
					// Run only errors on construction-level problems; poll
					// errors are logged and retried inside the loop.
					if err := w.Run(ctx); err != nil {
						lg.Error("worker stopped", "err", err)
					}
				}(worker)
			}
			wg.Wait()
			return out.Close()
		},
	}
	c.Flags().StringArrayVarP(&instances, "instance", "i", nil, "RDS instance identifier (repeatable to tail several)")
	c.Flags().StringVar(&region, "region", "", "AWS region (default: $AWS_REGION)")
	c.Flags().StringVar(&engine, "engine", "", "postgres|mysql|mariadb (default: auto-detect per instance)")
	c.Flags().StringVar(&assumeRole, "assume-role", "", "role ARN for cross-account access")
	c.Flags().StringVar(&format, "format", "text", "output format: text|ndjson")
	c.Flags().StringVar(&minSeverity, "min-severity", "", "only lines at/above this severity (e.g. ERROR)")
	c.Flags().StringVar(&grep, "grep", "", "only lines matching this regular expression (RE2)")
	c.Flags().DurationVar(&since, "since", 0, "replay the last N (e.g. 1h) before following (implies reading kept files from the beginning)")
	c.Flags().DurationVar(&poll, "poll", 5*time.Second, "poll interval")
	c.Flags().BoolVar(&beginning, "from-beginning", false, "start from the beginning of each log file instead of the tail")
	return c
}
