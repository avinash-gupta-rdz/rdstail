// Package cli wires cobra subcommands for the rdstail binary.
package cli

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/app"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/logging"
	"github.com/avinash-gupta-rdz/rdstail/internal/validate"
)

// Filled at build time via -ldflags (see Makefile).
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

// New constructs the root command. Callers invoke cmd.Execute().
func New() *cobra.Command {
	var (
		configPath string
		logLevel   string
	)

	root := &cobra.Command{
		Use:           "rdstail",
		Short:         "Stream AWS RDS logs to S3, Kafka, or HTTP sinks.",
		SilenceErrors: true,
		SilenceUsage:  true,
		Version:       fmt.Sprintf("%s (commit %s, built %s)", version, commit, buildDate),
	}
	root.PersistentFlags().StringVarP(&configPath, "config", "c", "", "path to YAML config (required for run/validate)")
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level: debug|info|warn|error")

	root.AddCommand(newRunCmd(&configPath, &logLevel))
	root.AddCommand(newValidateCmd(&configPath, &logLevel))
	root.AddCommand(newVersionCmd())
	return root
}

func newRunCmd(cfgPath, logLevel *string) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the log shipper.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, lg, err := loadAndInstall(*cfgPath, *logLevel)
			if err != nil {
				return err
			}
			return app.Run(cmd.Context(), cfg, lg)
		},
	}
}

func newValidateCmd(cfgPath, logLevel *string) *cobra.Command {
	var deep bool
	c := &cobra.Command{
		Use:   "validate",
		Short: "Validate a config file (schema only; --deep probes AWS/sinks).",
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
			fmt.Fprintln(cmd.OutOrStdout(), "schema OK")

			if !deep {
				return nil
			}
			results := validate.Deep(cmd.Context(), cfg)
			var failed int
			for _, r := range results {
				if r.OK() {
					fmt.Fprintf(cmd.OutOrStdout(), "  OK   %s\n", r.Name)
					continue
				}
				failed++
				fmt.Fprintf(cmd.ErrOrStderr(), "  FAIL %s: %v\n", r.Name, r.Err)
			}
			if failed > 0 {
				return fmt.Errorf("%d deep check(s) failed", failed)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "deep OK")
			return nil
		},
	}
	c.Flags().BoolVar(&deep, "deep", false, "run network-level probes against AWS and sinks")
	return c
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information.",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "rdstail %s (commit %s, built %s)\n", version, commit, buildDate)
		},
	}
}

// loadAndInstall loads the config and installs a default logger. Used by the run
// command; validate re-implements config loading to keep its error surface clean.
func loadAndInstall(path, level string) (*config.Config, *slog.Logger, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, nil, err
	}
	if err := config.Validate(cfg); err != nil {
		return nil, nil, err
	}
	// Config log level overrides flag default only if flag is the default "info".
	effective := level
	if level == "info" && cfg.Logging.Level != "" {
		effective = cfg.Logging.Level
	}
	lg := logging.New(os.Stderr, effective)
	logging.InstallDefault(lg)
	return cfg, lg, nil
}

