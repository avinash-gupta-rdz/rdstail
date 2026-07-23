package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/replay"
	sinkfactory "github.com/avinash-gupta-rdz/rdstail/internal/sink/factory"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"

	// side-effect registrations (dlq commands open the store without app.Run)
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/sqlite"
)

// newDLQCmd groups the dead-letter-queue lifecycle: list parked batches,
// replay them through real sinks, or purge them.
func newDLQCmd(cfgPath, logLevel *string) *cobra.Command {
	c := &cobra.Command{
		Use:   "dlq",
		Short: "Inspect, replay, or purge dead-lettered batches.",
	}
	c.AddCommand(newDLQListCmd(cfgPath, logLevel))
	c.AddCommand(newDLQReplayCmd(cfgPath, logLevel))
	c.AddCommand(newDLQPurgeCmd(cfgPath, logLevel))
	return c
}

// openDLQ loads the config and opens its state store, asserting DLQ capability.
// Callers own store.Close().
func openDLQ(ctx context.Context, path, level string) (*config.Config, *slog.Logger, state.StateStore, state.DLQ, error) {
	cfg, lg, err := loadAndInstall(path, level)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	store, err := state.Open(ctx, state.Config{Type: cfg.State.Type, Path: cfg.State.Path})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open state store: %w", err)
	}
	dlq, ok := store.(state.DLQ)
	if !ok {
		_ = store.Close()
		return nil, nil, nil, nil, fmt.Errorf("state store type %q has no DLQ support (use sqlite)", cfg.State.Type)
	}
	return cfg, lg, store, dlq, nil
}

func newDLQListCmd(cfgPath, logLevel *string) *cobra.Command {
	var (
		limit    int
		sinkName string
		asJSON   bool
	)
	c := &cobra.Command{
		Use:   "list",
		Short: "List dead-lettered batches (oldest first).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, _, store, dlq, err := openDLQ(cmd.Context(), *cfgPath, *logLevel)
			if err != nil {
				return err
			}
			defer store.Close()

			total, err := dlq.DLQCount(cmd.Context(), sinkName)
			if err != nil {
				return err
			}
			items, err := dlq.DLQList(cmd.Context(), state.DLQQuery{SinkName: sinkName, Limit: limit})
			if err != nil {
				return err
			}

			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				for _, it := range items {
					// Payload is already JSON ([]LogRecord); embed it raw.
					if err := enc.Encode(struct {
						ID        int64           `json:"id"`
						SinkName  string          `json:"sink_name"`
						BatchID   string          `json:"batch_id"`
						Reason    string          `json:"reason"`
						CreatedAt string          `json:"created_at"`
						Records   json.RawMessage `json:"records"`
					}{it.ID, it.SinkName, it.BatchID, it.Reason, it.CreatedAt.Format("2006-01-02T15:04:05Z07:00"), it.Payload}); err != nil {
						return err
					}
				}
				return nil
			}

			if total == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "DLQ is empty")
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%-6s %-20s %-18s %-20s %s\n", "ID", "SINK", "BATCH", "CREATED", "REASON")
			for _, it := range items {
				reason := it.Reason
				if len(reason) > 60 {
					reason = reason[:57] + "..."
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%-6d %-20s %-18s %-20s %s\n",
					it.ID, it.SinkName, it.BatchID, it.CreatedAt.Format("2006-01-02 15:04:05"), reason)
			}
			if int64(len(items)) < total {
				fmt.Fprintf(cmd.OutOrStdout(), "... showing %d of %d (use --limit to see more)\n", len(items), total)
			}
			return nil
		},
	}
	c.Flags().IntVar(&limit, "limit", 50, "maximum items to show")
	c.Flags().StringVar(&sinkName, "sink", "", "only items parked for this sink name")
	c.Flags().BoolVar(&asJSON, "json", false, "emit one JSON object per line (includes full payload)")
	return c
}

func newDLQReplayCmd(cfgPath, logLevel *string) *cobra.Command {
	var (
		sinkName string
		limit    int
		dryRun   bool
	)
	c := &cobra.Command{
		Use:   "replay",
		Short: "Re-deliver dead-lettered batches through their sinks; delete on ACK.",
		Long: `Re-deliver dead-lettered batches through the sinks named on each row.
A row is deleted only after the sink durably ACKs the write; failures leave the
row in place, so replay is always safe to re-run. Delivery is at-least-once —
records keep their original BatchID for downstream dedupe.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, lg, store, dlq, err := openDLQ(cmd.Context(), *cfgPath, *logLevel)
			if err != nil {
				return err
			}
			defer store.Close()

			res, err := replay.Run(cmd.Context(), cfg, dlq, sinkfactory.Build, lg, replay.Options{
				SinkFilter: sinkName,
				Limit:      limit,
				DryRun:     dryRun,
			})
			verb := "replayed"
			if dryRun {
				verb = "would replay"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s %d, failed %d, skipped %d\n", verb, res.Replayed, res.Failed, res.Skipped)
			if err != nil {
				return err
			}
			if res.Failed > 0 {
				return fmt.Errorf("%d batch(es) failed to replay (left in DLQ)", res.Failed)
			}
			return nil
		},
	}
	c.Flags().StringVar(&sinkName, "sink", "", "only replay items parked for this sink name")
	c.Flags().IntVar(&limit, "limit", 0, "stop after this many items (0 = all)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "report what would be replayed without writing")
	return c
}

func newDLQPurgeCmd(cfgPath, logLevel *string) *cobra.Command {
	var (
		sinkName string
		id       int64
		yes      bool
	)
	c := &cobra.Command{
		Use:   "purge",
		Short: "Permanently delete dead-lettered batches WITHOUT replaying them.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				return fmt.Errorf("purge permanently drops log data; re-run with --yes to confirm")
			}
			_, _, store, dlq, err := openDLQ(cmd.Context(), *cfgPath, *logLevel)
			if err != nil {
				return err
			}
			defer store.Close()

			if id > 0 {
				if err := dlq.DLQDelete(cmd.Context(), id); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "purged item %d\n", id)
				return nil
			}
			n, err := dlq.DLQPurge(cmd.Context(), sinkName)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "purged %d item(s)\n", n)
			return nil
		},
	}
	c.Flags().StringVar(&sinkName, "sink", "", "only purge items parked for this sink name (default: all)")
	c.Flags().Int64Var(&id, "id", 0, "purge a single item by ID")
	c.Flags().BoolVar(&yes, "yes", false, "confirm the purge")
	return c
}
