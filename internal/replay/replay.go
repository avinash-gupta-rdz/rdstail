// Package replay drains the sinks_dlq table back through real sinks. It is the
// recovery half of the DLQ contract: the run pipeline parks terminally-failed
// batches, `rdstail dlq replay` re-delivers them once the operator has fixed
// the sink, and rows are deleted only after the sink ACKs durably.
package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// SinkBuilder constructs a bare (undecorated) sink from its config entry.
// Injected so tests can substitute in-memory sinks; production passes
// factory.Build.
type SinkBuilder func(ctx context.Context, cfgSink *config.Sink) (sink.Sink, error)

// Options parameterise a replay run.
type Options struct {
	SinkFilter string // replay only items parked for this sink name ("" == all)
	Limit      int    // stop after processing this many items (0 == all)
	DryRun     bool   // report what would be replayed without writing anything
	PageSize   int    // DLQ read page size (0 == 100)
}

// Result summarises a replay run. Failed and Skipped items remain in the DLQ.
type Result struct {
	Replayed int // written to the sink and deleted from the DLQ
	Failed   int // sink write (or payload decode) failed; left in place
	Skipped  int // sink name no longer in config; left in place
}

// Run replays DLQ items through the configured sinks. Per item: unmarshal the
// stored batch, Write it to the sink named on the row, and DLQDelete only after
// a nil (durable-ACK) return. Failures leave the row untouched — replay is
// idempotent to re-run, and downstream dedupe is covered by the BatchID that
// every record already carries.
//
// Sinks are built lazily (first item that needs one) and wrapped with the
// config's retry policy but deliberately NOT with the DLQ decorator: a replay
// failure must surface, not re-park the batch behind a nil error.
func Run(ctx context.Context, cfg *config.Config, dlq state.DLQ, build SinkBuilder, lg *slog.Logger, opts Options) (Result, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	cfgByName := make(map[string]*config.Sink, len(cfg.Sinks))
	for i := range cfg.Sinks {
		cfgByName[cfg.Sinks[i].Name] = &cfg.Sinks[i]
	}

	sinks := map[string]sink.Sink{}
	defer func() {
		for name, s := range sinks {
			if err := s.Close(); err != nil {
				lg.Warn("replay sink close", "sink", name, "err", err)
			}
		}
	}()
	getSink := func(name string) (sink.Sink, error) {
		if s, ok := sinks[name]; ok {
			return s, nil
		}
		cfgSink, ok := cfgByName[name]
		if !ok {
			return nil, nil
		}
		bare, err := build(ctx, cfgSink)
		if err != nil {
			return nil, fmt.Errorf("build sink %q: %w", name, err)
		}
		wrapped := sink.WithRetry(bare, sink.RetryConfig{
			MaxAttempts: cfgSink.Retry.MaxAttempts,
			InitialWait: cfgSink.Retry.InitialWait,
			MaxWait:     cfgSink.Retry.MaxWait,
			Multiplier:  cfgSink.Retry.Multiplier,
		})
		sinks[name] = wrapped
		return wrapped, nil
	}

	var res Result
	var afterID int64
	processed := 0
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		items, err := dlq.DLQList(ctx, state.DLQQuery{
			SinkName: opts.SinkFilter,
			AfterID:  afterID,
			Limit:    pageSize,
		})
		if err != nil {
			return res, fmt.Errorf("dlq list: %w", err)
		}
		if len(items) == 0 {
			return res, nil
		}
		for _, it := range items {
			afterID = it.ID
			if opts.Limit > 0 && processed >= opts.Limit {
				return res, nil
			}
			processed++

			s, err := getSink(it.SinkName)
			if err != nil {
				return res, err
			}
			if s == nil {
				res.Skipped++
				lg.Warn("dlq item for unknown sink; leaving in place",
					"id", it.ID, "sink", it.SinkName, "batch_id", it.BatchID)
				continue
			}

			var records []logrecord.LogRecord
			if err := json.Unmarshal(it.Payload, &records); err != nil {
				res.Failed++
				lg.Error("dlq payload undecodable; leaving in place",
					"id", it.ID, "sink", it.SinkName, "batch_id", it.BatchID, "err", err)
				continue
			}

			if opts.DryRun {
				res.Replayed++
				lg.Info("dry-run: would replay",
					"id", it.ID, "sink", it.SinkName, "batch_id", it.BatchID, "records", len(records))
				continue
			}

			if err := s.Write(ctx, records); err != nil {
				res.Failed++
				lg.Error("replay write failed; leaving in place",
					"id", it.ID, "sink", it.SinkName, "batch_id", it.BatchID, "err", err)
				continue
			}
			if err := dlq.DLQDelete(ctx, it.ID); err != nil {
				// Delivered but not dequeued: the next replay re-sends it, which
				// at-least-once semantics permit (BatchID enables downstream dedupe).
				return res, fmt.Errorf("dlq delete id=%d after successful write: %w", it.ID, err)
			}
			res.Replayed++
			lg.Info("replayed",
				"id", it.ID, "sink", it.SinkName, "batch_id", it.BatchID, "records", len(records))
		}
	}
}
