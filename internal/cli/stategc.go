package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// newStateCmd groups checkpoint-store maintenance.
func newStateCmd(cfgPath, logLevel *string) *cobra.Command {
	c := &cobra.Command{
		Use:   "state",
		Short: "Inspect and maintain the checkpoint store.",
	}
	c.AddCommand(newStateGCCmd(cfgPath, logLevel))
	return c
}

func newStateGCCmd(cfgPath, logLevel *string) *cobra.Command {
	var (
		olderThan time.Duration
		dryRun    bool
	)
	c := &cobra.Command{
		Use:   "gc",
		Short: "Prune checkpoints for log files not seen recently (rotated out / departed instances).",
		Long: `Prune checkpoint rows whose last update is older than --older-than.
Active files are re-checkpointed every poll, so only rows for rotated-out log
files and removed instances qualify. Safe while rdstail is running. If a
pruned file ever reappears, runtime.start_from decides where it resumes.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if olderThan < time.Hour {
				return fmt.Errorf("--older-than must be >= 1h (got %s)", olderThan)
			}
			cfg, _, err := loadAndInstall(*cfgPath, *logLevel)
			if err != nil {
				return err
			}
			store, err := state.Open(cmd.Context(), state.Config{Type: cfg.State.Type, Path: cfg.State.Path})
			if err != nil {
				return fmt.Errorf("open state store: %w", err)
			}
			defer store.Close()
			gc, ok := store.(state.GCer)
			if !ok {
				return fmt.Errorf("state store type %q does not support gc (use sqlite)", cfg.State.Type)
			}
			n, err := gc.GCCheckpoints(cmd.Context(), time.Now().Add(-olderThan), dryRun)
			if err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "would prune %d checkpoint(s) older than %s\n", n, olderThan)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "pruned %d checkpoint(s) older than %s\n", n, olderThan)
			}
			return nil
		},
	}
	c.Flags().DurationVar(&olderThan, "older-than", 30*24*time.Hour, "prune checkpoints not updated in this long (>= 1h)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "report what would be pruned without deleting")
	return c
}
