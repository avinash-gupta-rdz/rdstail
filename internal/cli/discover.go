package cli

import (
	"fmt"

	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

// newDiscoverCmd previews tag-based instance discovery: it resolves every
// source's discover block against live AWS and prints what `run` would ingest,
// without starting the pipeline.
func newDiscoverCmd(cfgPath, logLevel *string) *cobra.Command {
	return &cobra.Command{
		Use:   "discover",
		Short: "Preview which RDS instances tag-based discovery would ingest.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, _, err := loadAndInstall(*cfgPath, *logLevel)
			if err != nil {
				return err
			}
			any := false
			for i, src := range cfg.Sources {
				if src.Discover == nil {
					continue
				}
				any = true
				awsCfg, err := awsx.NewConfig(cmd.Context(), awsx.Options{Region: src.Region, AssumeRole: src.AssumeRole})
				if err != nil {
					return fmt.Errorf("aws config for %s: %w", src.Region, err)
				}
				found, err := rdssrc.DiscoverInstances(cmd.Context(), awsrds.NewFromConfig(awsCfg), rdssrc.DiscoverFilter{
					Tags:   src.Discover.Tags,
					Engine: src.Engine,
				})
				if err != nil {
					return fmt.Errorf("sources[%d]: %w", i, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "sources[%d] region=%s tags=%v → %d instance(s)\n", i, src.Region, src.Discover.Tags, len(found))
				for _, d := range found {
					fmt.Fprintf(cmd.OutOrStdout(), "  %-40s %-10s %s\n", d.ID, d.Engine, d.Status)
				}
			}
			if !any {
				fmt.Fprintln(cmd.OutOrStdout(), "no sources use discover; nothing to preview")
			}
			return nil
		},
	}
}
