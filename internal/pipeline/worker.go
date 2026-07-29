package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
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
	pollIntervalMax time.Duration // 0 → fixed-interval polling
	pollMultiplier  float64
	maxBatchBytes   int64         // coalesce chunks up to this many raw bytes per write; 0 → per-chunk
	maxBatchRecords int           // coalesce up to this many records per write; 0 → per-chunk
	drainSem        chan struct{} // global bound on concurrent file drains; nil → serial
	startFrom       string
	minFileTime     time.Time // skip files last written before this; zero → all files
	lagGauge        *prometheus.GaugeVec
	pollGauge       *prometheus.GaugeVec
	stateOpsCounter *prometheus.CounterVec
}

// InstanceWorkerOpts configure NewInstanceWorker. LagGauge, PollGauge and
// StateOpsCounter are optional (nil → not recorded).
type InstanceWorkerOpts struct {
	Fetcher         *rdssrc.Fetcher
	Store           state.StateStore
	Sink            sink.Sink
	Logger          *slog.Logger
	PollInterval    time.Duration
	PollIntervalMax time.Duration // > PollInterval enables adaptive backoff-on-idle
	PollMultiplier  float64       // idle growth factor; <= 1 → default 2.0
	MaxBatchBytes   int64         // cross-chunk batching threshold; 0 → write per chunk
	MaxBatchRecords int           // cross-chunk batching threshold; 0 → write per chunk
	DrainSem        chan struct{} // shared semaphore bounding concurrent file drains across ALL workers; nil → serial per instance
	StartFrom       string        // config.StartFromBeginning | StartFromEnd
	MinFileTime     time.Time     // skip log files whose LastWritten predates this (used by tail --since); zero → no file skipped
	LagGauge        *prometheus.GaugeVec
	PollGauge       *prometheus.GaugeVec
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
	if opts.PollMultiplier <= 1 {
		opts.PollMultiplier = 2.0
	}
	if opts.PollIntervalMax < opts.PollInterval {
		opts.PollIntervalMax = 0 // fixed-interval
	}
	return &InstanceWorker{
		fetcher:         opts.Fetcher,
		store:           opts.Store,
		sink:            opts.Sink,
		log:             opts.Logger.With("instance", opts.Fetcher.InstanceID(), "engine", opts.Fetcher.Engine()),
		pollInterval:    opts.PollInterval,
		pollIntervalMax: opts.PollIntervalMax,
		pollMultiplier:  opts.PollMultiplier,
		maxBatchBytes:   opts.MaxBatchBytes,
		maxBatchRecords: opts.MaxBatchRecords,
		drainSem:        opts.DrainSem,
		startFrom:       opts.StartFrom,
		minFileTime:     opts.MinFileTime,
		lagGauge:        opts.LagGauge,
		pollGauge:       opts.PollGauge,
		stateOpsCounter: opts.StateOpsCounter,
	}, nil
}

// Run blocks until ctx is cancelled. Each tick it polls once and sleeps for the
// current interval — fixed by default, or adaptively backed off while idle when
// pollIntervalMax is set (any shipped record snaps back to the base interval).
// On context cancellation mid-poll it drains the current file's in-flight chunk
// before returning.
func (w *InstanceWorker) Run(ctx context.Context) error {
	interval := w.pollInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()

	poll := func() {
		shipped, err := w.pollOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("poll error", "err", err)
		}
		prev := interval
		interval = w.nextPollDelay(interval, shipped > 0)
		if interval != prev {
			w.log.Debug("adaptive poll interval changed", "from", prev.String(), "to", interval.String())
		}
		if w.pollGauge != nil {
			w.pollGauge.WithLabelValues(w.fetcher.InstanceID()).Set(interval.Seconds())
		}
	}

	// First poll immediately.
	poll()
	if ctx.Err() != nil {
		return nil
	}
	timer.Reset(interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			poll()
			if ctx.Err() != nil {
				return nil
			}
			timer.Reset(interval)
		}
	}
}

// nextPollDelay computes the delay before the next poll. Active polls (records
// shipped) reset to the base interval; idle polls grow it by pollMultiplier up
// to pollIntervalMax. With pollIntervalMax unset the base interval is returned
// unconditionally.
func (w *InstanceWorker) nextPollDelay(current time.Duration, active bool) time.Duration {
	if w.pollIntervalMax <= 0 || active {
		return w.pollInterval
	}
	return min(time.Duration(float64(current)*w.pollMultiplier), w.pollIntervalMax)
}

// pollOnce is one fetch cycle across all eligible log files. It returns the
// number of records shipped, which drives the adaptive-poll decision.
//
// With a DrainSem configured and more than one file, files drain concurrently
// (bounded globally by the semaphore). Per-file ordering is preserved because
// each file is drained by exactly one goroutine and polls never overlap;
// checkpoints are per-file, so there is no cross-file state to race on.
func (w *InstanceWorker) pollOnce(ctx context.Context) (int, error) {
	files, err := w.fetcher.DiscoverFiles(ctx)
	if err != nil {
		return 0, fmt.Errorf("discover: %w", err)
	}
	files = w.eligibleFiles(files)

	if w.drainSem == nil || len(files) < 2 {
		shipped := 0
		for _, file := range files {
			if ctx.Err() != nil {
				return shipped, ctx.Err()
			}
			w.observeLag(file)
			n, err := w.drainFile(ctx, file)
			shipped += n
			if err != nil {
				w.log.Warn("drain file failed", "log_file", file.Name, "err", err)
				// continue to next file; don't abort the whole poll
			}
		}
		return shipped, nil
	}

	var (
		wg      sync.WaitGroup
		shipped atomic.Int64
	)
	for _, file := range files {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(file rdssrc.FileMeta) {
			defer wg.Done()
			select {
			case w.drainSem <- struct{}{}:
				defer func() { <-w.drainSem }()
			case <-ctx.Done():
				return
			}
			w.observeLag(file)
			n, err := w.drainFile(ctx, file)
			shipped.Add(int64(n))
			if err != nil {
				w.log.Warn("drain file failed", "log_file", file.Name, "err", err)
			}
		}(file)
	}
	wg.Wait()
	return int(shipped.Load()), ctx.Err()
}

// eligibleFiles drops files rotated out before minFileTime — they cannot
// receive new writes, so draining them would only ship pre-window data. Files
// without a LastWritten timestamp are kept: never skip data on missing
// metadata.
func (w *InstanceWorker) eligibleFiles(files []rdssrc.FileMeta) []rdssrc.FileMeta {
	if w.minFileTime.IsZero() {
		return files
	}
	kept := files[:0]
	for _, f := range files {
		if lw := f.LastWritten(); !lw.IsZero() && lw.Before(w.minFileTime) {
			continue
		}
		kept = append(kept, f)
	}
	return kept
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
// after each successful sink write. It returns the number of records shipped.
func (w *InstanceWorker) drainFile(ctx context.Context, file rdssrc.FileMeta) (int, error) {
	prev, found, err := w.store.Get(ctx, w.fetcher.InstanceID(), file.Name)
	if err != nil {
		w.stateOp("get", "error")
		return 0, fmt.Errorf("state get: %w", err)
	}
	w.stateOp("get", "ok")

	// New file: decide whether to start at beginning or at the current tail.
	if !found {
		switch w.startFrom {
		case config.StartFromEnd:
			tail, err := w.fetcher.SkipToEnd(ctx, file.Name)
			if err != nil {
				return 0, fmt.Errorf("skip to end: %w", err)
			}
			prev = state.Checkpoint{
				Marker:      tail,
				FileSize:    file.Size,
				LastWritten: file.LastWritten(),
			}
			if err := w.store.Set(ctx, w.fetcher.InstanceID(), file.Name, prev); err != nil {
				w.stateOp("set", "error")
				return 0, fmt.Errorf("state set tail: %w", err)
			}
			w.stateOp("set", "ok")
			w.log.Info("new file, skipped to end", "log_file", file.Name, "marker", tail)
			return 0, nil
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

	// Pagination loop: keep pulling while AdditionalDataPending, coalescing
	// chunks into one sink write until a batch threshold trips or the file has
	// no more pending data. The checkpoint advances only at flush points and
	// only after the sink ACKs (at-least-once) — a crash mid-batch re-pulls
	// every chunk since the last flush.
	marker := prev.Marker
	shipped := 0
	var pending []logrecord.LogRecord
	var pendingBytes int64

	// flush writes the accumulated records (if any) and advances the
	// checkpoint to ckpt. Called with the state as of the last pulled chunk.
	flush := func(ckpt state.Checkpoint) error {
		if len(pending) > 0 {
			if err := w.sink.Write(ctx, pending); err != nil {
				return fmt.Errorf("sink write: %w", err)
			}
			shipped += len(pending)
			pending = nil
			pendingBytes = 0
		}
		if err := w.store.Set(ctx, w.fetcher.InstanceID(), file.Name, ckpt); err != nil {
			w.stateOp("set", "error")
			return fmt.Errorf("state set: %w", err)
		}
		w.stateOp("set", "ok")
		prev = ckpt
		return nil
	}

	for {
		if ctx.Err() != nil {
			return shipped, ctx.Err()
		}
		chunk, err := w.fetcher.PullPortion(ctx, file.Name, marker)
		if err != nil {
			return shipped, fmt.Errorf("pull: %w", err)
		}
		// Stamp batch-id on every record so downstream can dedupe. Records
		// keep their originating chunk's BatchID even when chunks coalesce
		// into one write.
		for i := range chunk.Records {
			chunk.Records[i].Marker = chunk.NextMarker
			chunk.Records[i].BatchID = chunk.BatchID
		}
		pending = append(pending, chunk.Records...)
		pendingBytes += chunk.Bytes
		marker = chunk.NextMarker
		ckpt := state.Checkpoint{
			Marker:       chunk.NextMarker,
			BytesWritten: prev.BytesWritten + pendingBytes,
			FileSize:     file.Size,
			LastWritten:  file.LastWritten(),
		}

		if !chunk.AdditionalPending {
			// End of poll for this file: flush whatever accumulated.
			return shipped, flush(ckpt)
		}
		full := (w.maxBatchBytes > 0 && pendingBytes >= w.maxBatchBytes) ||
			(w.maxBatchRecords > 0 && len(pending) >= w.maxBatchRecords)
		if full || (w.maxBatchBytes == 0 && w.maxBatchRecords == 0) {
			// Threshold hit (or batching disabled): flush mid-pagination.
			if err := flush(ckpt); err != nil {
				return shipped, err
			}
		}
	}
}
