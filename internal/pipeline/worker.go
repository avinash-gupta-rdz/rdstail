package pipeline

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	parallelReads   int           // concurrent byte-range reads per file for large backlogs; <= 1 → off
	drainSem        chan struct{} // global bound on concurrent file drains; nil → serial
	startFrom       string
	minFileTime     time.Time // skip files last written before this; zero → all files
	lagGauge        *prometheus.GaugeVec
	pollGauge       *prometheus.GaugeVec
	stateOpsCounter *prometheus.CounterVec
	anomalyCounter  *prometheus.CounterVec

	// initialFiles holds the files present at this worker's first successful
	// discovery when the instance had no checkpoints yet. Only those files
	// honour start_from; any file that appears later (a new hourly Postgres
	// file, a log type enabled at runtime, files created while rdstail was
	// down) is new data and is read from the beginning. nil until the first
	// successful discovery; read-only afterwards.
	initialFiles map[string]bool
	// initialSize is each initial file's size at first discovery: with
	// start_from=end the tail is anchored there, so lines written after
	// startup ship even if the first skip is delayed (throttling, errors).
	initialSize map[string]int64
	// horizon is the newest LastWritten among the instance's checkpoints at
	// first discovery. An untracked file last written at or before it is old
	// data whose checkpoint was pruned (checkpoint_retention), not a new file,
	// so it keeps start_from instead of being re-read from the beginning.
	horizon time.Time

	throttled atomic.Bool // set when any API call in the current poll was throttled

	stallMu sync.Mutex
	stalls  map[string]int // consecutive no-progress polls per growing file

	errMu        sync.Mutex
	recentErrors map[uint64]time.Time // hashes of mysql-error.log records shipped recently
	errPruned    time.Time

	// Restart/failover recovery. RDS reboots and Multi-AZ failovers lose the
	// last few KB of unflushed log writes (seen live: files come back smaller),
	// and the server then appends new lines from that shorter end. A marker
	// past the new end would skip those lines; resetting to 0 re-ships the
	// whole file. Instead the worker rewinds rewindBytes and drops records it
	// already shipped (per-file ring of recent record hashes).
	shipMu     sync.Mutex
	shipped    map[string]*shippedRing
	restartGen atomic.Int64        // bumped on each detected restart (banner or RDS event)
	restarts   []restartNote       // recent restarts, by generation
	handledGen map[string]int64    // per file: restartGen already recovered from
	started    time.Time           // banners older than this (or horizon) are history
	marks      map[string][]markAt // per file: recent checkpoints, for rewinding to before a restart
}

// restartNote is one detected restart: its generation and approximate time.
type restartNote struct {
	gen int64
	at  time.Time
}

// markAt is a checkpoint marker and when it was durably set.
type markAt struct {
	t      time.Time
	marker string
}

// restartMargin is how far before a reported restart time the rewind point
// is taken, absorbing clock skew and buffered-but-unflushed writes.
const (
	restartMargin      = time.Minute
	restartDedupWindow = 90 * time.Second // one restart's events/banner arrive within seconds
	marksKept          = 256
)

const (
	rewindBytes = 256 << 10
	// The dedup memory must cover every byte a rewind can re-read, or lines
	// older than it ship twice (seen live: a 4096-line ring vs a 512 KB
	// rewind of a busy general log). A restart rewind starts from a position
	// held >= restartMargin before the restart, so a busy file needs the
	// rewind window plus about a minute of writes: keep 2 MB, bounded.
	shipRingBytes   = 2 << 20
	shipRingMaxKeys = 50_000
)

// serverReady matches the lines MySQL and PostgreSQL log when they finish
// starting — after every reboot and Multi-AZ failover.
var serverReady = regexp.MustCompile(`ready for connections|database system is ready to accept connections`)

type shippedRing struct {
	keys  []shipKey // FIFO, oldest first
	bytes int64
	set   map[uint64]int
}

type shipKey struct {
	h uint64
	n int64
}

// recentErrorsTTL bounds how long a shipped mysql-error.log line is remembered
// for de-duplicating mysql-error-running.log (RDS copies every ~5 minutes);
// recentErrorsMax bounds memory during an error storm.
const (
	recentErrorsTTL = time.Hour
	recentErrorsMax = 200_000
)

// stallThreshold is how many consecutive polls a growing file may show no
// marker progress before it is reported as stalled.
const stallThreshold = 3

// throttleMaxDelay caps the poll interval while AWS is throttling us; long
// enough to shed load, short enough that realtime tailing resumes promptly.
const throttleMaxDelay = 2 * time.Minute

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
	ParallelReads   int           // runtime.parallel_reads_per_file; <= 1 → sequential
	DrainSem        chan struct{} // shared semaphore bounding concurrent file drains across ALL workers; nil → serial per instance
	StartFrom       string        // config.StartFromBeginning | StartFromEnd
	MinFileTime     time.Time     // skip log files whose LastWritten predates this (used by tail --since); zero → no file skipped
	LagGauge        *prometheus.GaugeVec
	PollGauge       *prometheus.GaugeVec
	StateOpsCounter *prometheus.CounterVec
	AnomalyCounter  *prometheus.CounterVec // rdstail_read_anomalies_total{instance,kind}
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
		parallelReads:   opts.ParallelReads,
		drainSem:        opts.DrainSem,
		startFrom:       opts.StartFrom,
		minFileTime:     opts.MinFileTime,
		lagGauge:        opts.LagGauge,
		pollGauge:       opts.PollGauge,
		stateOpsCounter: opts.StateOpsCounter,
		anomalyCounter:  opts.AnomalyCounter,
		stalls:          map[string]int{},
		recentErrors:    map[uint64]time.Time{},
		shipped:         map[string]*shippedRing{},
		handledGen:      map[string]int64{},
		marks:           map[string][]markAt{},
		started:         time.Now().UTC(),
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
		w.throttled.Store(false)
		shipped, err := w.pollOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			if rdssrc.IsThrottle(err) {
				w.throttled.Store(true)
			}
			w.log.Warn("poll error", "err", err)
		}
		prev := interval
		throttled := w.throttled.Load()
		interval = w.nextPollDelay(interval, shipped > 0, throttled)
		if throttled && interval != prev {
			w.log.Warn("AWS API throttling; backing off", "next_poll", interval.String())
		}
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
// unconditionally. A throttled poll doubles the delay (up to throttleMaxDelay)
// regardless of activity, so a fleet hitting the RDS API rate limit sheds load
// instead of hammering it; the next unthrottled poll returns to normal pacing.
func (w *InstanceWorker) nextPollDelay(current time.Duration, active, throttled bool) time.Duration {
	if throttled {
		d := min(max(current, w.pollInterval)*2, max(throttleMaxDelay, w.pollIntervalMax))
		// ±20% jitter so a throttled fleet doesn't retry in lockstep.
		return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
	}
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
	if w.initialFiles == nil {
		initial := map[string]bool{}
		w.initialSize = map[string]int64{}
		if w.startFrom != config.StartFromBeginning {
			known, err := w.store.List(ctx, w.fetcher.InstanceID())
			if err != nil {
				return 0, fmt.Errorf("state list: %w", err)
			}
			if len(known) == 0 {
				for _, f := range files {
					initial[f.Name] = true
					w.initialSize[f.Name] = f.Size
				}
			}
			for _, k := range known {
				if k.Checkpoint.LastWritten.After(w.horizon) {
					w.horizon = k.Checkpoint.LastWritten
				}
			}
		}
		w.initialFiles = initial
	}
	files, err = w.planRotation(ctx, files)
	if err != nil {
		return 0, err
	}

	// MySQL error lines land in mysql-error.log first and are copied into
	// mysql-error-running.log every ~5 minutes. Drain the realtime file first,
	// then the running log, which skips lines already shipped (see
	// recentErrors) and so only contributes lines the realtime file lost to
	// truncation.
	first, second := splitErrorLogPhases(files)
	shipped, err := w.drainAll(ctx, first)
	if err != nil || len(second) == 0 {
		return shipped, err
	}
	n, err := w.drainAll(ctx, second)
	return shipped + n, err
}

// drainAll drains files serially, or concurrently bounded by drainSem.
func (w *InstanceWorker) drainAll(ctx context.Context, files []rdssrc.FileMeta) (int, error) {
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
				if rdssrc.IsThrottle(err) {
					w.throttled.Store(true)
				}
				if !errors.Is(err, context.Canceled) {
					w.log.Warn("drain file failed", "log_file", file.Name, "err", err)
				}
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
				if rdssrc.IsThrottle(err) {
					w.throttled.Store(true)
				}
				if !errors.Is(err, context.Canceled) {
					w.log.Warn("drain file failed", "log_file", file.Name, "err", err)
				}
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

// anomaly records a source-side event that may mean missing or delayed data.
func (w *InstanceWorker) anomaly(kind string) {
	if w.anomalyCounter == nil {
		return
	}
	w.anomalyCounter.WithLabelValues(w.fetcher.InstanceID(), kind).Inc()
}

// startFor returns the start_from policy for a file with no checkpoint: the
// configured one for files present at first discovery of a never-seen
// instance (or old files whose checkpoint was pruned), otherwise "beginning"
// — a file written since is new data.
func (w *InstanceWorker) startFor(file rdssrc.FileMeta) string {
	if w.startFrom == config.StartFromBeginning {
		return config.StartFromBeginning
	}
	if w.initialFiles[file.Name] {
		return w.startFrom
	}
	if lw := file.LastWritten(); !w.horizon.IsZero() && !lw.IsZero() && !lw.After(w.horizon) {
		return w.startFrom
	}
	return config.StartFromBeginning
}

type rotatedFile struct {
	meta rdssrc.FileMeta
	id   rdssrc.HourID
}

// planRotation handles RDS MySQL/MariaDB hourly rotation explicitly instead
// of trusting the marker chain, which stalls after two rotations and silently
// mis-reads purged hours (see rds/rotation.go). It returns the files to drain
// this poll and seeds checkpoints so every hour file is read exactly once:
//
//   - The base file's checkpoint marker names the hour file it points into.
//     If that hour has since been rotated out, the checkpoint is handed to the
//     rotated file (read by name, which honours the offset) and the base file
//     restarts at the live file's beginning. Hours rotated after it are read
//     from their beginning.
//   - Rotated hours older than the base checkpoint were already read through
//     the base file and are skipped without any API call.
//   - A base marker for an hour RDS no longer retains is a gap: it is reported
//     (logs + rdstail_read_anomalies_total{kind="gap"}) and every retained
//     hour after it is read.
//   - With no base checkpoint (first run), rotated hours are backfilled when
//     the base file starts from the beginning, and ignored otherwise.
//
// Rotated files whose base file is not listed are left as ordinary files.
func (w *InstanceWorker) planRotation(ctx context.Context, files []rdssrc.FileMeta) ([]rdssrc.FileMeta, error) {
	present := make(map[string]bool, len(files))
	for _, f := range files {
		present[f.Name] = true
	}
	out := make([]rdssrc.FileMeta, 0, len(files))
	byBase := map[string][]rotatedFile{}
	baseMeta := map[string]rdssrc.FileMeta{}
	for _, f := range files {
		if base, id, ok := rdssrc.SplitRotated(f.Name); ok && present[base] {
			byBase[base] = append(byBase[base], rotatedFile{meta: f, id: id})
			continue
		}
		out = append(out, f)
		baseMeta[f.Name] = f
	}
	inst := w.fetcher.InstanceID()
	for base, rotated := range byBase {
		sort.Slice(rotated, func(i, j int) bool { return rotated[i].id.Compare(rotated[j].id) < 0 })
		bcp, bfound, err := w.store.Get(ctx, inst, base)
		if err != nil {
			return nil, fmt.Errorf("state get %s: %w", base, err)
		}
		bid, _, hasID := rdssrc.ParseHourMarker(bcp.Marker)
		hasID = bfound && hasID
		baseRotated, gap := false, false
		if hasID {
			for _, r := range rotated {
				if r.id == bid {
					baseRotated = true
				}
			}
			gap = !baseRotated && bid.Compare(rotated[len(rotated)-1].id) < 0
		}
		if gap {
			w.anomaly("gap")
			w.log.Error("log hours purged by RDS before they were read; data lost",
				"log_file", base, "checkpoint_hour", bid.String(),
				"oldest_retained_hour", rotated[0].id.String())
		}
		backfill := !bfound && w.startFor(baseMeta[base]) == config.StartFromBeginning
		for _, r := range rotated {
			rcp, rfound, err := w.store.Get(ctx, inst, r.meta.Name)
			if err != nil {
				return nil, fmt.Errorf("state get %s: %w", r.meta.Name, err)
			}
			if rfound {
				// Drain until the stored offset reaches the (immutable) file size.
				if _, off, ok := rdssrc.ParseHourMarker(rcp.Marker); !ok || off < r.meta.Size {
					out = append(out, r.meta)
				}
				continue
			}
			var seed string
			switch {
			case !bfound:
				if !backfill {
					continue
				}
				seed = rdssrc.MarkerBeginning
			case !hasID:
				continue // base restarted at the live file; older hours are done
			case r.id == bid:
				seed = bcp.Marker // finish the hour the base file was in
			case r.id.Compare(bid) > 0:
				if !baseRotated && !gap {
					continue // base already reads a newer (live) hour
				}
				seed = rdssrc.MarkerBeginning
			default:
				continue // older hour, already read through the base file
			}
			if err := w.store.Set(ctx, inst, r.meta.Name, state.Checkpoint{Marker: seed}); err != nil {
				w.stateOp("set", "error")
				return nil, fmt.Errorf("state set %s: %w", r.meta.Name, err)
			}
			w.stateOp("set", "ok")
			out = append(out, r.meta)
		}
		if baseRotated || gap {
			if err := w.store.Set(ctx, inst, base, state.Checkpoint{Marker: rdssrc.MarkerBeginning}); err != nil {
				w.stateOp("set", "error")
				return nil, fmt.Errorf("state set %s: %w", base, err)
			}
			w.stateOp("set", "ok")
			w.log.Info("log rotated; checkpoint handed to rotated hour file",
				"log_file", base, "hour", bid.String())
		}
	}
	return out, nil
}

// noteProgress tracks files that keep growing while their marker stays put —
// the signature of a stuck marker — and reports one after stallThreshold polls.
func (w *InstanceWorker) noteProgress(file rdssrc.FileMeta, before, after state.Checkpoint, shipped int) {
	w.stallMu.Lock()
	defer w.stallMu.Unlock()
	grew := file.LastWritten().After(before.LastWritten) && !before.LastWritten.IsZero()
	if shipped > 0 || after.Marker != before.Marker || !grew {
		delete(w.stalls, file.Name)
		return
	}
	w.stalls[file.Name]++
	if w.stalls[file.Name] == stallThreshold {
		w.anomaly("stall")
		w.log.Error("log file keeps growing but its marker does not advance; no data is being read",
			"log_file", file.Name, "marker", after.Marker, "file_size", file.Size)
	}
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
		switch w.startFor(file) {
		case config.StartFromEnd:
			tail, err := w.fetcher.SkipToEnd(ctx, file.Name, w.anchorSize(file))
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
		default:
			prev = state.Checkpoint{Marker: rdssrc.MarkerBeginning, FileSize: file.Size}
		}
	}
	// A size drop means truncation in place (mysql-error.log after RDS copies
	// it into mysql-error-running.log) or a reboot/failover that lost the
	// unflushed tail. Hourly rename-rotation is handled by planRotation; and
	// when a rotation happened mid-drain the RDS marker chain has already
	// moved into the new hour, so an hour-addressed marker whose offset lies
	// within the (smaller) live file is valid (resetting re-read that hour).
	shrank := found && file.Size < prev.FileSize && !markerStillValid(prev.Marker, file.Size)
	restarted, restartAt, restartGen := false, time.Time{}, int64(0)
	if found {
		restarted, restartAt, restartGen = w.pendingRestart(file.Name)
		if restarted && !file.LastWritten().IsZero() && file.LastWritten().Before(restartAt.Add(-restartMargin)) {
			// Not written since before the restart (e.g. an old hourly
			// Postgres file): it can't have lost writes. Nothing to re-verify.
			w.markRestartHandled(file.Name, restartGen)
			restarted = false
		}
	}
	dedupe, dropFirst := false, false
	consumed := map[uint64]int{} // dedupe consumes ring counts: repeats beyond them are new
	dedupeUntil := int64(-1)     // past this offset data is new: stop deduping
	if shrank || restarted {
		from := prev.Marker
		if restarted {
			// Rewind from where we were before the restart, not from where we
			// are now: the event may arrive after we've read past the gap. Never
			// across an hour boundary: a MySQL base marker's hour ID selects the
			// file, so an older hour's offset would re-read that whole hour.
			if m, ok := w.markBefore(file.Name, restartAt.Add(-restartMargin)); ok && sameHourFile(m, prev.Marker) {
				from = m
			}
		}
		if _, off, ok := rdssrc.OffsetMarker(prev.Marker); ok {
			dedupeUntil = off
		}
		var start string
		start, dropFirst = rewindMarker(from, file.Size)
		w.log.Warn("log file shrank or server restarted; re-reading recent data, skipping already-shipped lines",
			"log_file", file.Name, "prev_size", prev.FileSize, "new_size", file.Size,
			"server_restart", restarted, "from", start)
		prev.Marker = start
		prev.SkipContinuation = false
		dedupe = true
	}
	// The restart counts as handled only once the rewound position is
	// durable; a failed drain leaves it pending so the next poll rewinds again.
	commitRestart := func() {
		if restarted {
			w.markRestartHandled(file.Name, restartGen)
			restarted = false
		}
	}
	before := prev

	// Pagination loop: keep pulling while AdditionalDataPending, coalescing
	// chunks into one sink write until a batch threshold trips or the file has
	// no more pending data. The checkpoint advances only at flush points and
	// only after the sink ACKs (at-least-once) — a crash mid-batch re-pulls
	// every chunk since the last flush.
	//
	// Multi-line entries may straddle chunks, so a mid-pagination flush holds
	// back the newest record (it may continue in the next chunk) and
	// checkpoints at the marker of the chunk it began in. A crash then
	// re-pulls that chunk: at most one chunk of duplicates, never a split or
	// lost entry.
	marker := prev.Marker
	shipped := 0
	var pending []logrecord.LogRecord
	var pendingBytes int64
	lastStart := marker // marker of the chunk the newest pending record began in
	// lastStartSkip: that chunk opens with continuation lines that belong to
	// an earlier, already-accounted-for record — persisted with a hold-back
	// checkpoint so a resume drops them instead of shipping a fragment.
	lastStartSkip := false
	skipLeading := prev.SkipContinuation

	if w.parallelReads > 1 && !dedupe && !dropFirst && !skipLeading &&
		!isErrorLog(file.Name) && !isErrorRunningLog(file.Name) {
		n, err := w.parallelDrain(ctx, file, &prev)
		shipped += n
		if err != nil {
			return shipped, err
		}
		marker, lastStart = prev.Marker, prev.Marker
	}

	flush := func(ckpt state.Checkpoint, holdLast bool) error {
		var carry []logrecord.LogRecord
		if holdLast && len(pending) > 0 {
			carry = []logrecord.LogRecord{pending[len(pending)-1]}
			pending = pending[:len(pending)-1]
			ckpt.Marker = lastStart
			ckpt.SkipContinuation = lastStartSkip
		}
		if len(pending) > 0 {
			if err := w.sink.Write(ctx, pending); err != nil {
				pending = append(pending, carry...)
				return fmt.Errorf("sink write: %w", err)
			}
			if isErrorLog(file.Name) {
				w.rememberErrors(pending)
			}
			w.rememberShipped(file.Name, pending)
			shipped += len(pending)
			pendingBytes = 0
		}
		pending = carry
		if ckpt.Marker == prev.Marker && len(carry) > 0 {
			return nil // nothing durable to advance past yet
		}
		if err := w.store.Set(ctx, w.fetcher.InstanceID(), file.Name, ckpt); err != nil {
			w.stateOp("set", "error")
			return fmt.Errorf("state set: %w", err)
		}
		w.stateOp("set", "ok")
		w.recordMark(file.Name, ckpt.Marker)
		commitRestart()
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
		if chunk.TruncatedLines > 0 {
			w.anomaly("truncated_line")
			w.log.Warn("log line exceeds RDS's 1 MB response limit; shipped truncated",
				"log_file", file.Name, "marker", marker)
		}
		// Stamp batch-id on every record so downstream can dedupe. Records
		// keep their originating chunk's BatchID even when chunks coalesce
		// into one write.
		for i := range chunk.Records {
			chunk.Records[i].Marker = chunk.NextMarker
			chunk.Records[i].BatchID = chunk.BatchID
		}
		recs := chunk.Records
		leading := chunk.LeadingContinuation
		if dropFirst && len(recs) > 0 {
			recs, leading = recs[1:], false // rewound mid-file: first line may be partial
			dropFirst = false
		}
		if dedupe {
			if kept := w.dropAlreadyShipped(file.Name, recs, consumed); len(kept) < len(recs) {
				if len(kept) == 0 || kept[0].Message != recs[0].Message {
					leading = false
				}
				recs = kept
			}
		}
		if isErrorRunningLog(file.Name) {
			var firstDropped bool
			recs, firstDropped = w.dropShippedErrors(recs)
			leading = leading && !firstDropped
		}
		leadAbsorbed := false
		if leading && len(recs) > 0 {
			switch {
			case skipLeading && len(pending) == 0:
				recs = recs[1:] // shipped with its entry before the last checkpoint
				leadAbsorbed = true
			case len(pending) > 0:
				if joined, ok := rdssrc.JoinContinuation(pending[len(pending)-1], recs[0]); ok {
					pending[len(pending)-1] = joined
					recs = recs[1:]
					leadAbsorbed = true
				}
			}
		}
		skipLeading = false
		if len(recs) > 0 {
			lastStart = marker
			lastStartSkip = leadAbsorbed
		}
		w.noteServerStart(recs) // only lines not shipped before: a re-read banner isn't a new restart
		pending = append(pending, recs...)
		pendingBytes += chunk.Bytes
		marker = chunk.NextMarker
		if dedupe {
			if _, off, ok := rdssrc.OffsetMarker(marker); ok && dedupeUntil >= 0 && off >= dedupeUntil {
				dedupe = false
			}
		}
		ckpt := state.Checkpoint{
			Marker:       chunk.NextMarker,
			BytesWritten: prev.BytesWritten + pendingBytes,
			FileSize:     file.Size,
			LastWritten:  file.LastWritten(),
		}

		if !chunk.AdditionalPending {
			// End of poll for this file: flush whatever accumulated.
			err := flush(ckpt, false)
			if err == nil {
				w.noteProgress(file, before, prev, shipped)
			}
			return shipped, err
		}
		full := (w.maxBatchBytes > 0 && pendingBytes >= w.maxBatchBytes) ||
			(w.maxBatchRecords > 0 && len(pending) >= w.maxBatchRecords)
		if full || (w.maxBatchBytes == 0 && w.maxBatchRecords == 0) {
			// Threshold hit (or batching disabled): flush mid-pagination.
			if err := flush(ckpt, w.fetcher.GroupsMultiline()); err != nil {
				return shipped, err
			}
		}
	}
}

func isErrorLog(name string) bool { return path.Base(name) == "mysql-error.log" }

func isErrorRunningLog(name string) bool {
	return strings.HasPrefix(path.Base(name), "mysql-error-running.log")
}

// splitErrorLogPhases moves the mysql-error-running.log family into a second
// phase when the instance also has the realtime mysql-error.log.
func splitErrorLogPhases(files []rdssrc.FileMeta) (first, second []rdssrc.FileMeta) {
	hasRealtime := false
	for _, f := range files {
		if isErrorLog(f.Name) {
			hasRealtime = true
		}
	}
	if !hasRealtime {
		return files, nil
	}
	for _, f := range files {
		if isErrorRunningLog(f.Name) {
			second = append(second, f)
		} else {
			first = append(first, f)
		}
	}
	return first, second
}

func errorKey(msg string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(msg))
	return h.Sum64()
}

// rememberErrors records mysql-error.log records the sink has acknowledged.
func (w *InstanceWorker) rememberErrors(recs []logrecord.LogRecord) {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	now := time.Now()
	if len(w.recentErrors)+len(recs) > recentErrorsMax {
		for k, t := range w.recentErrors {
			if now.Sub(t) > recentErrorsTTL/4 {
				delete(w.recentErrors, k)
			}
		}
	}
	for _, r := range recs {
		if len(w.recentErrors) >= recentErrorsMax {
			break
		}
		w.recentErrors[errorKey(r.Message)] = now
	}
}

// dropShippedErrors filters mysql-error-running.log records already shipped
// from mysql-error.log (reporting whether the first record was dropped) and
// expires old entries at most once a minute.
func (w *InstanceWorker) dropShippedErrors(recs []logrecord.LogRecord) ([]logrecord.LogRecord, bool) {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	now := time.Now()
	if now.Sub(w.errPruned) > time.Minute {
		for k, t := range w.recentErrors {
			if now.Sub(t) > recentErrorsTTL {
				delete(w.recentErrors, k)
			}
		}
		w.errPruned = now
	}
	kept := recs[:0]
	firstDropped := false
	for i, r := range recs {
		if _, seen := w.recentErrors[errorKey(r.Message)]; seen {
			firstDropped = firstDropped || i == 0
			continue
		}
		kept = append(kept, r)
	}
	return kept, firstDropped
}

// markerStillValid reports whether an hour-addressed MySQL marker points
// within a file of the given size; such a marker survives a size drop.
func markerStillValid(marker string, size int64) bool {
	_, off, ok := rdssrc.ParseHourMarker(marker)
	return ok && off <= size
}

// anchorSize is where start_from=end begins reading a file: its size when the
// worker first saw it, never beyond its current size (a rotation in between
// leaves a smaller live file).
func (w *InstanceWorker) anchorSize(file rdssrc.FileMeta) int64 {
	if n, ok := w.initialSize[file.Name]; ok && n <= file.Size {
		return n
	}
	return file.Size
}

// rewindMarker returns a marker rewindBytes before min(marker offset, size)
// for "<prefix>:<offset>" markers, and whether the read starts mid-file (so
// its first line may be partial). Other markers rewind to the beginning.
func rewindMarker(marker string, size int64) (string, bool) {
	i := strings.LastIndexByte(marker, ':')
	if i <= 0 {
		return rdssrc.MarkerBeginning, false
	}
	off, err := strconv.ParseInt(marker[i+1:], 10, 64)
	if err != nil {
		return rdssrc.MarkerBeginning, false
	}
	start := min(off, size) - rewindBytes
	if start <= 0 {
		return rdssrc.MarkerBeginning, false
	}
	return marker[:i+1] + strconv.FormatInt(start, 10), true
}

// rememberShipped records hashes of acknowledged records for file, keeping
// at least the last shipRingBytes of shipped text.
func (w *InstanceWorker) rememberShipped(file string, recs []logrecord.LogRecord) {
	w.shipMu.Lock()
	defer w.shipMu.Unlock()
	r := w.shipped[file]
	if r == nil {
		r = &shippedRing{set: map[uint64]int{}}
		w.shipped[file] = r
	}
	for _, rec := range recs {
		k := shipKey{h: errorKey(rec.Message), n: int64(len(rec.Message)) + 1}
		r.keys = append(r.keys, k)
		r.bytes += k.n
		r.set[k.h]++
	}
	drop := 0
	for drop < len(r.keys) && (r.bytes-r.keys[drop].n >= shipRingBytes || len(r.keys)-drop > shipRingMaxKeys) {
		old := r.keys[drop]
		r.bytes -= old.n
		if r.set[old.h]--; r.set[old.h] <= 0 {
			delete(r.set, old.h)
		}
		drop++
	}
	if drop > 0 {
		r.keys = append(r.keys[:0:0], r.keys[drop:]...)
	}
}

// dropAlreadyShipped filters records recently shipped from file. Each
// remembered occurrence absorbs one re-read record (tracked in consumed for
// the drain), so a line legitimately repeated beyond what was shipped stays.
func (w *InstanceWorker) dropAlreadyShipped(file string, recs []logrecord.LogRecord, consumed map[uint64]int) []logrecord.LogRecord {
	w.shipMu.Lock()
	defer w.shipMu.Unlock()
	r := w.shipped[file]
	if r == nil {
		return recs
	}
	kept := recs[:0:0]
	for _, rec := range recs {
		h := errorKey(rec.Message)
		if consumed[h] < r.set[h] {
			consumed[h]++
			continue
		}
		kept = append(kept, rec)
	}
	return kept
}

// noteServerStart flags a server restart (reboot/failover) when a startup
// banner newer than anything already processed is read, so every file of the
// instance is re-verified once on its next drain.
func (w *InstanceWorker) noteServerStart(recs []logrecord.LogRecord) {
	floor := w.started
	if !w.horizon.IsZero() && w.horizon.Before(floor) {
		floor = w.horizon
	}
	for _, r := range recs {
		if r.Timestamp.After(floor) && serverReady.MatchString(r.Message) {
			w.NotifyRestart(r.Timestamp, "server start banner in log")
			return
		}
	}
}

// NotifyRestart tells the worker its instance restarted (reboot or Multi-AZ
// failover) at about `at`; every tracked file is re-verified on its next
// drain. Safe to call from any goroutine; repeated notices for the same
// restart only cost one extra dedup'd re-read.
func (w *InstanceWorker) NotifyRestart(at time.Time, why string) {
	w.shipMu.Lock()
	// One restart surfaces several times (RDS "shutdown" + "restarted"
	// events, the start banner re-read from mysql-error-running.log): fold
	// notices within restartDedupWindow into the first.
	for _, r := range w.restarts {
		if d := at.Sub(r.at); d < restartDedupWindow && d > -restartDedupWindow {
			w.shipMu.Unlock()
			return
		}
	}
	w.anomaly("restart")
	gen := w.restartGen.Add(1)
	w.restarts = append(w.restarts, restartNote{gen: gen, at: at})
	if len(w.restarts) > 32 {
		w.restarts = w.restarts[len(w.restarts)-32:]
	}
	w.shipMu.Unlock()
	w.log.Warn("database restart/failover detected; re-verifying log positions", "at", at.Format(time.RFC3339), "source", why)
}

// pendingRestart reports whether file still has to be re-verified after a
// detected restart, the earliest such restart's time, and the generation to
// pass to markRestartHandled once the rewound position is durable.
func (w *InstanceWorker) pendingRestart(file string) (bool, time.Time, int64) {
	gen := w.restartGen.Load()
	w.shipMu.Lock()
	defer w.shipMu.Unlock()
	handled := w.handledGen[file]
	if handled >= gen {
		return false, time.Time{}, 0
	}
	var at time.Time
	for _, r := range w.restarts {
		if r.gen > handled && (at.IsZero() || r.at.Before(at)) {
			at = r.at
		}
	}
	return true, at, gen
}

func (w *InstanceWorker) markRestartHandled(file string, gen int64) {
	w.shipMu.Lock()
	defer w.shipMu.Unlock()
	if gen > w.handledGen[file] {
		w.handledGen[file] = gen
	}
}

func (w *InstanceWorker) recordMark(file, marker string) {
	w.shipMu.Lock()
	defer w.shipMu.Unlock()
	m := append(w.marks[file], markAt{t: time.Now().UTC(), marker: marker})
	if len(m) > marksKept {
		m = m[len(m)-marksKept:]
	}
	w.marks[file] = m
}

// markBefore returns the newest checkpoint marker for file set at or before t.
func (w *InstanceWorker) markBefore(file string, t time.Time) (string, bool) {
	w.shipMu.Lock()
	defer w.shipMu.Unlock()
	m := w.marks[file]
	for i := len(m) - 1; i >= 0; i-- {
		if !m[i].t.After(t) {
			return m[i].marker, true
		}
	}
	if len(m) > 0 {
		return m[0].marker, true // restart predates our history: rewind as far as we know
	}
	return "", false
}

// InstanceID is the RDS instance this worker tails.
func (w *InstanceWorker) InstanceID() string { return w.fetcher.InstanceID() }

// parallelSegment is the byte span each concurrent range read covers.
const parallelSegment = 2 << 20

// parallelDrain reads a large backlog of file in waves of w.parallelReads
// concurrent byte-range requests (RDS serves one file sequentially at only
// ~0.3–0.9 MB/s). Each range owns the lines that start in it, so the ranges
// tile the file exactly; their text is joined and parsed as one, so records,
// grouping and order are identical to a sequential read. Each wave ships and
// checkpoints at the start of its last record (which may continue into the
// next wave). It returns when the backlog is small, at end of file, or when
// an exact range read isn't possible — the caller continues sequentially
// from *prev in every case.
func (w *InstanceWorker) parallelDrain(ctx context.Context, file rdssrc.FileMeta, prev *state.Checkpoint) (int, error) {
	prefix, off, ok := rdssrc.OffsetMarker(prev.Marker)
	if !ok {
		return 0, nil
	}
	n := int64(w.parallelReads)
	shipped := 0
	for file.Size-off >= n*parallelSegment/2 {
		if ctx.Err() != nil {
			return shipped, ctx.Err()
		}
		last := off+n*parallelSegment >= file.Size
		texts := make([]string, n)
		ends := make([]int64, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := int64(0); i < n; i++ {
			start := off + i*parallelSegment
			if start >= file.Size {
				ends[i] = -1
				continue
			}
			end := start + parallelSegment
			if last && (i == n-1 || end >= file.Size) {
				end = -1 // read to the current end of file
			}
			wg.Add(1)
			go func(i, start, end int64) {
				defer wg.Done()
				texts[i], ends[i], errs[i] = w.fetcher.ReadRange(ctx, file.Name, prefix, start, end, i == 0)
			}(i, start, end)
			if end < 0 {
				for j := i + 1; j < n; j++ {
					ends[j] = -1
				}
				break
			}
		}
		wg.Wait()
		// Verify the ranges tile: each starts exactly where the previous ended.
		var sb strings.Builder
		at := off
		for i := int64(0); i < n; i++ {
			if errs[i] != nil {
				if errors.Is(errs[i], rdssrc.ErrRangeUnsupported) {
					return shipped, nil
				}
				return shipped, fmt.Errorf("parallel read: %w", errs[i])
			}
			if ends[i] < 0 {
				continue
			}
			if ends[i]-int64(len(texts[i])) != at {
				w.log.Debug("parallel ranges did not tile; continuing sequentially", "log_file", file.Name)
				return shipped, nil
			}
			sb.WriteString(texts[i])
			at = ends[i]
		}
		text, waveEnd := sb.String(), at
		recs := w.fetcher.Records(file.Name, text)
		if !last {
			if len(recs) < 2 {
				return shipped, nil // one giant record: let the sequential path handle it
			}
			// Hold back the last record: it may continue in the next wave.
			tail := recs[len(recs)-1].Message + "\n"
			if !strings.HasSuffix(text, tail) {
				return shipped, nil
			}
			waveEnd -= int64(len(tail))
			recs = recs[:len(recs)-1]
		}
		if waveEnd <= off {
			return shipped, nil
		}
		next := prefix + ":" + strconv.FormatInt(waveEnd, 10)
		batch := rdssrc.BatchID(w.fetcher.InstanceID(), file.Name, prev.Marker, next)
		for i := range recs {
			recs[i].Marker, recs[i].BatchID = next, batch
		}
		w.noteServerStart(recs)
		if len(recs) > 0 {
			if err := w.sink.Write(ctx, recs); err != nil {
				return shipped, fmt.Errorf("sink write: %w", err)
			}
			w.rememberShipped(file.Name, recs)
			shipped += len(recs)
		}
		ckpt := state.Checkpoint{
			Marker:       next,
			BytesWritten: prev.BytesWritten + (waveEnd - off),
			FileSize:     file.Size,
			LastWritten:  file.LastWritten(),
		}
		if err := w.store.Set(ctx, w.fetcher.InstanceID(), file.Name, ckpt); err != nil {
			w.stateOp("set", "error")
			return shipped, fmt.Errorf("state set: %w", err)
		}
		w.stateOp("set", "ok")
		w.recordMark(file.Name, next)
		*prev = ckpt
		off = waveEnd
		if last {
			break
		}
	}
	return shipped, nil
}

// sameHourFile reports whether two markers address the same file: always for
// non-hourly markers; for MySQL "YYYY-MM-DD.H:off" markers, the same hour.
func sameHourFile(a, b string) bool {
	ia, _, okA := rdssrc.ParseHourMarker(a)
	ib, _, okB := rdssrc.ParseHourMarker(b)
	if !okA || !okB {
		return okA == okB
	}
	return ia == ib
}
