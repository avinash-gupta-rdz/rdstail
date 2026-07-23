package cli

import (
	"fmt"
	"os"
	"os/signal"
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

// newTailCmd is the zero-config one-liner: stream one instance's logs to
// stdout, tail -f style. State is in-memory (no resume — that's `run`'s job).
func newTailCmd(logLevel *string) *cobra.Command {
	var (
		instance    string
		region      string
		engine      string
		assumeRole  string
		format      string
		minSeverity string
		poll        time.Duration
		beginning   bool
	)
	c := &cobra.Command{
		Use:   "tail",
		Short: "Stream one RDS instance's logs to stdout — tail -f, no config file.",
		Example: `  rdstail tail -i my-db --region ap-south-1
  rdstail tail -i my-db --region us-east-1 --min-severity ERROR
  rdstail tail -i my-db --region eu-west-1 --format ndjson | jq .message`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if instance == "" {
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
			lg := logging.New(os.Stderr, *logLevel)
			logging.InstallDefault(lg)

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region, AssumeRole: assumeRole})
			if err != nil {
				return err
			}
			client := awsrds.NewFromConfig(awsCfg)

			// Engine drives log-file classification and metadata extraction;
			// auto-detect when not given.
			if engine == "" {
				resp, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String(instance),
				})
				if err != nil {
					return fmt.Errorf("describe %s: %w", instance, err)
				}
				if len(resp.DBInstances) == 0 {
					return fmt.Errorf("instance %q not found in %s", instance, region)
				}
				apiEngine := aws.ToString(resp.DBInstances[0].Engine)
				engine = rdssrc.NormalizeEngine(apiEngine)
				if engine == "" {
					return fmt.Errorf("instance %q runs %q, which rdstail cannot tail", instance, apiEngine)
				}
				lg.Info("detected engine", "instance", instance, "engine", engine)
			}

			out, err := stdoutsink.New("tail", format, nil)
			if err != nil {
				return err
			}
			var s sink.Sink = out
			if minSeverity != "" {
				s = sink.WithFilter(s, sink.FilterConfig{MinSeverity: minSeverity})
			}

			fetcher, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{
				API: client, InstanceID: instance, Engine: engine,
			})
			if err != nil {
				return err
			}
			startFrom := config.StartFromEnd
			if beginning {
				startFrom = config.StartFromBeginning
			}
			worker, err := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
				Fetcher:      fetcher,
				Store:        statememory.New(),
				Sink:         s,
				Logger:       lg,
				PollInterval: poll,
				StartFrom:    startFrom,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "tailing %s (%s) in %s — Ctrl-C to stop\n", instance, engine, region)
			if err := worker.Run(ctx); err != nil {
				return err
			}
			return out.Close()
		},
	}
	c.Flags().StringVarP(&instance, "instance", "i", "", "RDS instance identifier (required)")
	c.Flags().StringVar(&region, "region", "", "AWS region (default: $AWS_REGION)")
	c.Flags().StringVar(&engine, "engine", "", "postgres|mysql|mariadb (default: auto-detect)")
	c.Flags().StringVar(&assumeRole, "assume-role", "", "role ARN for cross-account access")
	c.Flags().StringVar(&format, "format", "text", "output format: text|ndjson")
	c.Flags().StringVar(&minSeverity, "min-severity", "", "only lines at/above this severity (e.g. ERROR)")
	c.Flags().DurationVar(&poll, "poll", 5*time.Second, "poll interval")
	c.Flags().BoolVar(&beginning, "from-beginning", false, "start from the beginning of each log file instead of the tail")
	return c
}
