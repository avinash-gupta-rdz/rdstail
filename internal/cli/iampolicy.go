package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/iampolicy"
)

func newIAMPolicyCmd(cfgPath *string) *cobra.Command {
	var (
		deep      bool
		terraform bool
	)
	c := &cobra.Command{
		Use:   "iam-policy",
		Short: "Print the minimal IAM policy this config needs.",
		Long: `Derive the least-privilege IAM policy for the principal running rdstail,
from the config's exact sources and sinks: RDS log reads scoped to your
instances, S3 writes scoped to bucket+prefix, and KMS / assume-role
statements only when the config uses them.

Policy JSON goes to stdout (pipe into 'aws iam put-role-policy' or a file);
notes about permissions that belong elsewhere — e.g. on an assumed role in
the bucket's account — go to stderr.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if *cfgPath == "" {
				return fmt.Errorf("--config is required")
			}
			cfg, err := config.Load(*cfgPath)
			if err != nil {
				return err
			}
			if err := config.Validate(cfg); err != nil {
				return fmt.Errorf("schema validation failed:\n%w", err)
			}
			pol, notes := iampolicy.Generate(cfg, iampolicy.Options{Deep: deep})
			if terraform {
				fmt.Fprint(cmd.OutOrStdout(), pol.Terraform())
			} else {
				out, err := pol.JSON()
				if err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), out)
			}
			for _, n := range notes {
				fmt.Fprintf(cmd.ErrOrStderr(), "note: %s\n", n)
			}
			return nil
		},
	}
	c.Flags().BoolVar(&deep, "deep", false, "include the extra permissions `validate --deep` probes use (s3:ListBucket)")
	c.Flags().BoolVar(&terraform, "terraform", false, "emit a Terraform aws_iam_policy_document data source instead of JSON")
	return c
}
