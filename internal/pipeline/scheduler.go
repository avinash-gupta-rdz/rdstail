// Package pipeline wires fetchers, state, and sinks into a per-instance worker
// pool with graceful shutdown.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// InstanceSpec identifies one RDS instance the pipeline should poll.
type InstanceSpec struct {
	InstanceID string
	Engine     string
	Region     string
	API        rdssrc.RDSAPI
}

// Scheduler owns the lifecycle of per-instance InstanceWorkers.
type Scheduler struct {
	cfg             *config.Config
	instances       []InstanceSpec
	refresh         func(ctx context.Context) ([]InstanceSpec, error)
	refreshInterval time.Duration
	store           state.StateStore
	sink            sink.Sink
	log             *slog.Logger

	lagGauge        *prometheus.GaugeVec
	pollGauge       *prometheus.GaugeVec
	stateOpsCounter *prometheus.CounterVec
	apiCallsCounter *prometheus.CounterVec

	shutdownTimeout time.Duration
}

// SchedulerOpts configure NewScheduler. The prometheus collectors are
// optional; nil means no metric updates. Refresh + RefreshInterval enable
// periodic reconciliation: Refresh returns the full desired instance set and
// the scheduler starts/stops workers to match.
type SchedulerOpts struct {
	Config          *config.Config
	Instances       []InstanceSpec
	Refresh         func(ctx context.Context) ([]InstanceSpec, error)
	RefreshInterval time.Duration
	Store           state.StateStore
	Sink            sink.Sink
	Logger          *slog.Logger
	LagGauge        *prometheus.GaugeVec
	PollGauge       *prometheus.GaugeVec
	StateOpsCounter *prometheus.CounterVec
	APICallsCounter *prometheus.CounterVec
}

// NewScheduler validates inputs and returns a Scheduler.
func NewScheduler(opts SchedulerOpts) (*Scheduler, error) {
	if opts.Config == nil {
		return nil, errors.New("scheduler: config required")
	}
	if opts.Store == nil {
		return nil, errors.New("scheduler: state store required")
	}
	if opts.Sink == nil {
		return nil, errors.New("scheduler: sink required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Scheduler{
		cfg:             opts.Config,
		instances:       opts.Instances,
		refresh:         opts.Refresh,
		refreshInterval: opts.RefreshInterval,
		store:           opts.Store,
		sink:            opts.Sink,
		log:             opts.Logger,
		lagGauge:        opts.LagGauge,
		pollGauge:       opts.PollGauge,
		stateOpsCounter: opts.StateOpsCounter,
		apiCallsCounter: opts.APICallsCounter,
		shutdownTimeout: opts.Config.Runtime.ShutdownTimeout,
	}, nil
}

// maxDynamicWorkers bounds the semaphore when the desired instance set can
// grow at runtime (periodic re-discovery) and no explicit concurrency cap is
// configured. Mirrors the 500-instance cap.
const maxDynamicWorkers = 500

// specKey uniquely identifies a worker slot.
func specKey(inst InstanceSpec) string { return inst.Region + "|" + inst.InstanceID }

// Run spawns one goroutine per instance (bounded by runtime.max_instances_concurrent
// via a semaphore) and blocks until ctx is cancelled. When Refresh is
// configured, the desired instance set is re-resolved on RefreshInterval and
// workers are started/stopped to match; a worker that exited (error or
// removal) is restarted on the next refresh if still desired. On cancel it
// waits up to shutdown_timeout for workers to drain before returning.
func (s *Scheduler) Run(ctx context.Context) error {
	dynamic := s.refresh != nil && s.refreshInterval > 0
	if len(s.instances) == 0 && !dynamic {
		s.log.Warn("scheduler: no instances configured; idling")
		<-ctx.Done()
		return nil
	}

	concurrency := s.cfg.Runtime.MaxInstancesConcurrent
	if dynamic {
		// The set can grow past the boot size; never clamp to it.
		if concurrency <= 0 {
			concurrency = maxDynamicWorkers
		}
	} else if concurrency <= 0 || concurrency > len(s.instances) {
		concurrency = len(s.instances)
	}
	sem := make(chan struct{}, concurrency)

	// Global file-drain pool: runtime.max_workers bounds how many log files
	// are being fetched+written concurrently across ALL instances. <= 1 keeps
	// every instance serial (one file at a time), the historical behaviour.
	var drainSem chan struct{}
	if mw := s.cfg.Runtime.MaxWorkers; mw > 1 {
		drainSem = make(chan struct{}, mw)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex // guards running
		running = map[string]context.CancelFunc{}
	)

	start := func(inst InstanceSpec) error {
		var observer rdssrc.APICallObserver
		if s.apiCallsCounter != nil {
			c := s.apiCallsCounter
			observer = func(op, outcome string) {
				c.WithLabelValues(op, outcome).Inc()
			}
		}
		fetcher, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{
			API:        inst.API,
			InstanceID: inst.InstanceID,
			Engine:     inst.Engine,
			Observer:   observer,
		})
		if err != nil {
			return fmt.Errorf("build fetcher for %s: %w", inst.InstanceID, err)
		}
		worker, err := NewInstanceWorker(InstanceWorkerOpts{
			Fetcher:         fetcher,
			Store:           s.store,
			Sink:            s.sink,
			Logger:          s.log,
			PollInterval:    s.cfg.Runtime.PollInterval,
			PollIntervalMax: s.cfg.Runtime.PollIntervalMax,
			PollMultiplier:  s.cfg.Runtime.PollBackoffMultiplier,
			MaxBatchBytes:   s.cfg.Runtime.MaxBatchBytes,
			MaxBatchRecords: s.cfg.Runtime.MaxBatchRecords,
			DrainSem:        drainSem,
			StartFrom:       s.cfg.Runtime.StartFrom,
			LagGauge:        s.lagGauge,
			PollGauge:       s.pollGauge,
			StateOpsCounter: s.stateOpsCounter,
		})
		if err != nil {
			return fmt.Errorf("build worker for %s: %w", inst.InstanceID, err)
		}
		key := specKey(inst)
		wctx, cancel := context.WithCancel(ctx)
		mu.Lock()
		running[key] = cancel
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				cancel()
				mu.Lock()
				delete(running, key)
				mu.Unlock()
			}()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := worker.Run(wctx); err != nil {
				s.log.Error("worker exited with error", "instance", inst.InstanceID, "err", err)
			}
		}()
		return nil
	}

	for _, inst := range s.instances {
		if err := start(inst); err != nil {
			return err
		}
	}

	if dynamic {
		ticker := time.NewTicker(s.refreshInterval)
		defer ticker.Stop()
	reconcile:
		for {
			select {
			case <-ctx.Done():
				break reconcile
			case <-ticker.C:
				desired, err := s.refresh(ctx)
				if err != nil {
					s.log.Warn("re-discovery failed; keeping current worker set", "err", err)
					continue
				}
				want := map[string]InstanceSpec{}
				for _, inst := range desired {
					want[specKey(inst)] = inst
				}
				// Stop workers whose instance left the desired set.
				mu.Lock()
				for key, cancel := range running {
					if _, ok := want[key]; !ok {
						s.log.Info("instance left fleet; stopping worker", "key", key)
						cancel()
					}
				}
				mu.Unlock()
				// Start workers for new (or previously-exited) instances.
				for key, inst := range want {
					mu.Lock()
					_, alive := running[key]
					mu.Unlock()
					if alive {
						continue
					}
					s.log.Info("starting worker", "instance", inst.InstanceID, "region", inst.Region)
					if err := start(inst); err != nil {
						s.log.Error("start worker failed", "instance", inst.InstanceID, "err", err)
					}
				}
			}
		}
	} else {
		<-ctx.Done()
	}
	s.log.Info("scheduler: shutdown requested; waiting for workers", "timeout", s.shutdownTimeout)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		s.log.Info("scheduler: all workers drained")
	case <-time.After(s.shutdownTimeout):
		s.log.Warn("scheduler: shutdown timeout reached; some workers may still be running")
	}
	return nil
}
