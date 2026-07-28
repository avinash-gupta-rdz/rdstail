package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/initwizard"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

// newInitCmd is the interactive onboarding path: list what's actually in the
// account, ask five questions, write a validated config, print the next
// command. The 5-minute claim made real for people who don't read docs.
func newInitCmd() *cobra.Command {
	var (
		region string
		output string
	)
	c := &cobra.Command{
		Use:   "init",
		Short: "Interactively create a working rdstail.yaml from your live AWS account.",
		Long: `Walk from zero to a validated config: rdstail lists the RDS instances your
ambient AWS credentials can see, you pick instances and a sink (S3 / Kafka /
HTTP webhook / stdout), and a commented rdstail.yaml is written and
validated — then the exact commands to run next are printed.

Listing failures (no credentials, no instances) fall back to manual entry;
init never needs more than RDS/S3 read permissions and works with none.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if region == "" {
				region = os.Getenv("AWS_REGION")
			}
			if region == "" {
				region = os.Getenv("AWS_DEFAULT_REGION")
			}
			res, err := initwizard.Run(cmd.Context(), initwizard.Deps{
				In:            cmd.InOrStdin(),
				Out:           cmd.ErrOrStderr(),
				ListInstances: listInstancesFor,
				ListBuckets:   listBuckets,
				FileExists: func(p string) bool {
					// Unknown stat errors count as "exists" so the overwrite
					// prompt runs rather than silently clobbering.
					_, err := os.Stat(p)
					return !errors.Is(err, fs.ErrNotExist)
				},
			}, region, output)
			if err != nil {
				return err
			}
			// 0600: the config is where users are invited to add auth headers
			// and SASL passwords.
			if err := os.WriteFile(res.Path, []byte(res.YAML), 0o600); err != nil {
				return fmt.Errorf("write %s: %w", res.Path, err)
			}
			// The wizard's contract is validated output — verify through the
			// real loader anyway so a bug here can never ship a broken file.
			cfg, err := config.Load(res.Path)
			if err == nil {
				err = config.Validate(cfg)
			}
			if err != nil {
				return fmt.Errorf("wrote %s but it failed validation (please report this): %w", res.Path, err)
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "\nwrote %s (validated)\n\nnext:\n", res.Path)
			fmt.Fprintf(out, "  rdstail iam-policy -c %s          # least-privilege IAM policy for this config\n", res.Path)
			fmt.Fprintf(out, "  rdstail validate -c %s --deep     # probe AWS + the sink end to end\n", res.Path)
			fmt.Fprintf(out, "  rdstail run -c %s                 # start shipping\n", res.Path)
			return nil
		},
	}
	c.Flags().StringVar(&region, "region", "", "AWS region to list instances in (default: $AWS_REGION, else asked)")
	c.Flags().StringVarP(&output, "output", "o", "rdstail.yaml", "where to write the config")
	return c
}

// listInstancesFor adapts rds discovery to the wizard's Instance type. The
// region comes from the wizard prompt, so the client is built per call.
func listInstancesFor(ctx context.Context, region string) ([]initwizard.Instance, error) {
	awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region})
	if err != nil {
		return nil, err
	}
	found, err := rdssrc.ListInstances(ctx, awsrds.NewFromConfig(awsCfg), "")
	if err != nil {
		return nil, err
	}
	out := make([]initwizard.Instance, 0, len(found))
	for _, d := range found {
		out = append(out, initwizard.Instance{ID: d.ID, Engine: d.Engine, Status: d.Status})
	}
	return out, nil
}

// listBuckets lists the account's buckets from the prompted region's
// partition endpoint.
func listBuckets(ctx context.Context, region string) ([]string, error) {
	awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region})
	if err != nil {
		return nil, err
	}
	resp, err := awss3.NewFromConfig(awsCfg).ListBuckets(ctx, &awss3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.Buckets))
	for _, b := range resp.Buckets {
		out = append(out, aws.ToString(b.Name))
	}
	return out, nil
}
