package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// InstanceWorker pulls logs for a single RDS instance and ships them through
// the configured sink. It iterates log files serially within a poll cycle to
// preserve per-file ordering and bound per-instance API concurrency.
type InstanceWorker struct {
	fetcher         *rdssrc.Fetcher
	store           state.StateStore
	sink            sink.Sink
	log             *slog.Logger
	pollInterval    time.Duration
	startFrom       string
	lagGauge        *prometheus.GaugeVec
	stateOpsCounter *prometheus.CounterVec
}

// InstanceWorkerOpts configure NewInstanceWorker. LagGauge and StateOpsCounter
// are optional (nil → not recorded).
type InstanceWorkerOpts struct {
	Fetcher         *rdssrc.Fetcher
	Store           state.StateStore
	Sink            sink.Sink
	Logger          *slog.Logger
	PollInterval    time.Duration
	StartFrom       string // config.StartFromBeginning | StartFromEnd
	LagGauge        *prometheus.GaugeVec
	StateOpsCounter *prometheus.CounterVec
}

// NewInstanceWorker constructs a worker. Required fields are validated.
func NewInstanceWorker(opts InstanceWorkerOpts) (*InstanceWorker, error) {
	if opts.Fetcher == nil {
		return nil, errors.New("pipeline: fetcher required")
	}
	if opts.Store == nil {
		return nil, errors.New("pipeline: state store required")
	}
	if opts.Sink == nil {
		return nil, errors.New("pipeline: sink required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 10 * time.Second
	}
	if opts.StartFrom == "" {
		opts.StartFrom = config.StartFromEnd
	}
	return &InstanceWorker{
		fetcher:         opts.Fetcher,
		store:           opts.Store,
		sink:            opts.Sink,
		log:             opts.Logger.With("instance", opts.Fetcher.InstanceID(), "engine", opts.Fetcher.Engine()),
		pollInterval:    opts.PollInterval,
		startFrom:       opts.StartFrom,
		lagGauge:        opts.LagGauge,
		stateOpsCounter: opts.StateOpsCounter,
	}, nil
}

// Run blocks until ctx is cancelled. Each tick it polls once and sleeps. On
// context cancellation mid-poll it drains the current file's in-flight chunk
// before returning.
func (w *InstanceWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	// First poll immediately.
	if err := w.pollOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		w.log.Warn("initial poll error", "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.pollOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				w.log.Warn("poll error", "err", err)
			}
		}
	}
}

// pollOnce is one fetch cycle across all eligible log files.
func (w *InstanceWorker) pollOnce(ctx context.Context) error {
	files, err := w.fetcher.DiscoverFiles(ctx)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	for _, file := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.observeLag(file)
		if err := w.drainFile(ctx, file); err != nil {
			w.log.Warn("drain file failed", "log_file", file.Name, "err", err)
			// continue to next file; don't abort the whole poll
		}
	}
	return nil
}

// observeLag updates the ingestion-lag gauge for this file if configured.
// Lag = now − file.LastWritten. Reflects RDS-server clock, not ours.
func (w *InstanceWorker) observeLag(file rdssrc.FileMeta) {
	if w.lagGauge == nil {
		return
	}
	lw := file.LastWritten()
	if lw.IsZero() {
		return
	}
	lag := time.Since(lw).Seconds()
	if lag < 0 {
		lag = 0
	}
	w.lagGauge.WithLabelValues(w.fetcher.InstanceID(), file.Name).Set(lag)
}

// stateOp bumps the state_store_ops_total counter if configured.
func (w *InstanceWorker) stateOp(op, outcome string) {
	if w.stateOpsCounter == nil {
		return
	}
	w.stateOpsCounter.WithLabelValues(op, outcome).Inc()
}

// drainFile fetches all pending chunks for one file, advancing the checkpoint
// after each successful sink write.
func (w *InstanceWorker) drainFile(ctx context.Context, file rdssrc.FileMeta) error {
	prev, found, err := w.store.Get(ctx, w.fetcher.InstanceID(), file.Name)
	if err != nil {
		w.stateOp("get", "error")
		return fmt.Errorf("state get: %w", err)
	}
	w.stateOp("get", "ok")

	// New file: decide whether to start at beginning or at the current tail.
	if !found {
		switch w.startFrom {
		case config.StartFromEnd:
			tail, err := w.fetcher.SkipToEnd(ctx, file.Name)
			if err != nil {
				return fmt.Errorf("skip to end: %w", err)
			}
			prev = state.Checkpoint{
				Marker:      tail,
				FileSize:    file.Size,
				LastWritten: file.LastWritten(),
			}
			if err := w.store.Set(ctx, w.fetcher.InstanceID(), file.Name, prev); err != nil {
				w.stateOp("set", "error")
				return fmt.Errorf("state set tail: %w", err)
			}
			w.stateOp("set", "ok")
			w.log.Info("new file, skipped to end", "log_file", file.Name, "marker", tail)
			return nil
		case config.StartFromBeginning:
			prev = state.Checkpoint{Marker: rdssrc.MarkerBeginning, FileSize: file.Size}
		}
	} else if file.Size < prev.FileSize {
		// Truncation / rotation-in-place: drain what's still reachable under the
		// old marker and then reset to start. We do one best-effort pull first.
		w.log.Warn("log file truncated; resetting marker",
			"log_file", file.Name, "prev_size", prev.FileSize, "new_size", file.Size)
		prev.Marker = rdssrc.MarkerBeginning
	}

	// Pagination loop: keep pulling while AdditionalDataPending.
	marker := prev.Marker
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		chunk, err := w.fetcher.PullPortion(ctx, file.Name, marker)
		if err != nil {
			return fmt.Errorf("pull: %w", err)
		}
		if len(chunk.Records) > 0 {
			// Stamp batch-id on every record so downstream can dedupe.
			for i := range chunk.Records {
				chunk.Records[i].Marker = chunk.NextMarker
				chunk.Records[i].BatchID = chunk.BatchID
			}
			if err := w.sink.Write(ctx, chunk.Records); err != nil {
				return fmt.Errorf("sink write: %w", err)
			}
		}
		// Advance checkpoint only after sink ack (at-least-once).
		next := state.Checkpoint{
			Marker:       chunk.NextMarker,
			BytesWritten: prev.BytesWritten + chunk.Bytes,
			FileSize:     file.Size,
			LastWritten:  file.LastWritten(),
		}
		if err := w.store.Set(ctx, w.fetcher.InstanceID(), file.Name, next); err != nil {
			w.stateOp("set", "error")
			return fmt.Errorf("state set: %w", err)
		}
		w.stateOp("set", "ok")
		prev = next
		marker = chunk.NextMarker
		if !chunk.AdditionalPending {
			return nil
		}
	}
}
