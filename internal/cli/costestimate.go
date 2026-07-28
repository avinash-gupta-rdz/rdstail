package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/costestimate"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

// newCostEstimateCmd projects the fleet's real monthly log volume from
// DescribeDBLogFiles metadata and prices CloudWatch export against rdstail.
// Works from a config file, or with no config at all against a whole region.
func newCostEstimateCmd(cfgPath *string) *cobra.Command {
	var (
		region     string
		instances  []string
		assumeRole string
		jsonOut    bool
		prices     = costestimate.DefaultPrices()
	)
	c := &cobra.Command{
		Use:   "cost-estimate",
		Short: "Project your fleet's monthly log volume and price CloudWatch vs rdstail.",
		Long: `Read each instance's retained log-file sizes via DescribeDBLogFiles (free —
no log data is downloaded), project a month of volume from the retention
window, and print what that volume costs on the CloudWatch export path next
to what it costs on the rdstail path.

With --config, the fleet is the config's sources (explicit instances plus tag
discovery). Without it, every supported instance in --region is estimated —
no config file needed to evaluate rdstail.`,
		Example: `  rdstail cost-estimate --region ap-south-1
  rdstail cost-estimate --region us-east-1 -i prod-db -i prod-db-replica
  rdstail cost-estimate -c rdstail.yaml --json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			var usages []costestimate.InstanceUsage
			var err error
			if *cfgPath != "" {
				if region != "" || len(instances) > 0 || assumeRole != "" {
					return fmt.Errorf("--region/--instance/--assume-role are for no-config mode; with --config the fleet comes from the config file")
				}
				usages, err = usagesFromConfig(ctx, *cfgPath, &prices)
			} else {
				if region == "" {
					region = os.Getenv("AWS_REGION")
				}
				if region == "" {
					return fmt.Errorf("--config or --region is required (or set AWS_REGION)")
				}
				usages, err = usagesFromRegion(ctx, region, assumeRole, instances)
			}
			if err != nil {
				return err
			}
			if len(usages) == 0 {
				return fmt.Errorf("no RDS instances to estimate")
			}
			report := costestimate.Estimate(usages, time.Now().UTC(), prices)
			if jsonOut {
				out, err := json.MarshalIndent(report, "", "  ")
				if err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), string(out))
				return nil
			}
			fmt.Fprint(cmd.OutOrStdout(), report.Text())
			return nil
		},
	}
	c.Flags().StringVar(&region, "region", "", "AWS region for no-config mode (default: $AWS_REGION)")
	c.Flags().StringArrayVarP(&instances, "instance", "i", nil, "instance to estimate in no-config mode (repeatable; default: all supported instances in the region)")
	c.Flags().StringVar(&assumeRole, "assume-role", "", "role ARN for cross-account access in no-config mode")
	c.Flags().BoolVar(&jsonOut, "json", false, "emit the full report as JSON")
	c.Flags().Float64Var(&prices.CloudWatchIngestPerGB, "cw-ingest-per-gb", prices.CloudWatchIngestPerGB, "CloudWatch Logs ingestion price per GB")
	c.Flags().Float64Var(&prices.CloudWatchStoragePerGB, "cw-storage-per-gb", prices.CloudWatchStoragePerGB, "CloudWatch Logs storage price per GB-month")
	c.Flags().Float64Var(&prices.S3PutPerThousand, "s3-put-per-1k", prices.S3PutPerThousand, "S3 PUT price per 1,000 requests")
	c.Flags().Float64Var(&prices.S3StoragePerGB, "s3-storage-per-gb", prices.S3StoragePerGB, "S3 storage price per GB-month")
	c.Flags().Float64Var(&prices.CompressionRatio, "compression-ratio", prices.CompressionRatio, "assumed NDJSON+gzip compression ratio for S3 storage")
	return c
}

// usagesFromConfig resolves the config's fleet (explicit instances plus tag
// discovery, per source) and reads each instance's log-file metadata. The
// config's runtime.max_batch_bytes overrides prices.BatchBytes so the PUT
// count reflects the batching `run` would actually do.
func usagesFromConfig(ctx context.Context, path string, prices *costestimate.Prices) ([]costestimate.InstanceUsage, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	if err := config.Validate(cfg); err != nil {
		return nil, fmt.Errorf("schema validation failed:\n%w", err)
	}
	if cfg.Runtime.MaxBatchBytes > 0 {
		prices.BatchBytes = cfg.Runtime.MaxBatchBytes
	}
	var usages []costestimate.InstanceUsage
	seen := map[string]bool{} // region/id — an instance in two sources counts once
	for i, src := range cfg.Sources {
		awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: src.Region, AssumeRole: src.AssumeRole})
		if err != nil {
			return nil, fmt.Errorf("aws config for %s: %w", src.Region, err)
		}
		client := awsrds.NewFromConfig(awsCfg)

		// Union of explicit instances and discovery, deduplicated — the same
		// fleet `run` would ingest.
		engines := map[string]string{}
		for _, id := range src.Instances {
			engines[id] = src.Engine
		}
		if src.Discover != nil {
			found, err := rdssrc.DiscoverInstances(ctx, client, rdssrc.DiscoverFilter{
				Tags:   src.Discover.Tags,
				Engine: src.Engine,
			})
			if err != nil {
				return nil, fmt.Errorf("sources[%d]: %w", i, err)
			}
			for _, d := range found {
				engines[d.ID] = d.Engine
			}
		}
		for id, engine := range engines {
			if seen[src.Region+"/"+id] {
				continue
			}
			seen[src.Region+"/"+id] = true
			u, err := instanceUsage(ctx, client, id, engine)
			if err != nil {
				return nil, err
			}
			usages = append(usages, u)
		}
	}
	return usages, nil
}

// usagesFromRegion estimates without a config: the named instances, or every
// supported instance in the region when none are named. Engines come from AWS.
func usagesFromRegion(ctx context.Context, region, assumeRole string, instances []string) ([]costestimate.InstanceUsage, error) {
	awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region, AssumeRole: assumeRole})
	if err != nil {
		return nil, err
	}
	client := awsrds.NewFromConfig(awsCfg)

	engines := map[string]string{}
	if len(instances) == 0 {
		found, err := rdssrc.ListInstances(ctx, client, "")
		if err != nil {
			return nil, err
		}
		for _, d := range found {
			engines[d.ID] = d.Engine
		}
	} else {
		for _, id := range instances {
			resp, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(id),
			})
			if err != nil {
				return nil, fmt.Errorf("describe %s: %w", id, err)
			}
			if len(resp.DBInstances) == 0 {
				return nil, fmt.Errorf("instance %q not found in %s", id, region)
			}
			apiEngine := aws.ToString(resp.DBInstances[0].Engine)
			engine := rdssrc.NormalizeEngine(apiEngine)
			if engine == "" {
				return nil, fmt.Errorf("instance %q runs %q, which rdstail cannot tail", id, apiEngine)
			}
			engines[id] = engine
		}
	}
	var usages []costestimate.InstanceUsage
	for id, engine := range engines {
		u, err := instanceUsage(ctx, client, id, engine)
		if err != nil {
			return nil, err
		}
		usages = append(usages, u)
	}
	return usages, nil
}

// instanceUsage reads one instance's eligible log-file metadata, filtered by
// the same engine classifier `run` uses — the estimate covers exactly the
// files rdstail would ship.
func instanceUsage(ctx context.Context, client *awsrds.Client, id, engine string) (costestimate.InstanceUsage, error) {
	fetcher, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: client, InstanceID: id, Engine: engine})
	if err != nil {
		return costestimate.InstanceUsage{}, err
	}
	files, err := fetcher.DiscoverFiles(ctx)
	if err != nil {
		return costestimate.InstanceUsage{}, err
	}
	u := costestimate.InstanceUsage{InstanceID: id, Engine: engine}
	for _, f := range files {
		u.Files = append(u.Files, costestimate.FileStat{Size: f.Size, LastWritten: f.LastWritten()})
	}
	return u, nil
}
