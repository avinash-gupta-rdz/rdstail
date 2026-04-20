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
	cfg       *config.Config
	instances []InstanceSpec
	store     state.StateStore
	sink      sink.Sink
	log       *slog.Logger

	lagGauge        *prometheus.GaugeVec
	stateOpsCounter *prometheus.CounterVec
	apiCallsCounter *prometheus.CounterVec

	shutdownTimeout time.Duration
}

// SchedulerOpts configure NewScheduler. The two prometheus collectors are
// optional; nil means no metric updates.
type SchedulerOpts struct {
	Config          *config.Config
	Instances       []InstanceSpec
	Store           state.StateStore
	Sink            sink.Sink
	Logger          *slog.Logger
	LagGauge        *prometheus.GaugeVec
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
		store:           opts.Store,
		sink:            opts.Sink,
		log:             opts.Logger,
		lagGauge:        opts.LagGauge,
		stateOpsCounter: opts.StateOpsCounter,
		apiCallsCounter: opts.APICallsCounter,
		shutdownTimeout: opts.Config.Runtime.ShutdownTimeout,
	}, nil
}

// Run spawns one goroutine per instance (bounded by runtime.max_instances_concurrent
// via a semaphore) and blocks until ctx is cancelled. On cancel it waits up to
// shutdown_timeout for workers to drain before returning.
func (s *Scheduler) Run(ctx context.Context) error {
	if len(s.instances) == 0 {
		s.log.Warn("scheduler: no instances configured; idling")
		<-ctx.Done()
		return nil
	}

	concurrency := s.cfg.Runtime.MaxInstancesConcurrent
	if concurrency <= 0 || concurrency > len(s.instances) {
		concurrency = len(s.instances)
	}
	sem := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	for _, inst := range s.instances {
		inst := inst
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
			StartFrom:       s.cfg.Runtime.StartFrom,
			LagGauge:        s.lagGauge,
			StateOpsCounter: s.stateOpsCounter,
		})
		if err != nil {
			return fmt.Errorf("build worker for %s: %w", inst.InstanceID, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := worker.Run(ctx); err != nil {
				s.log.Error("worker exited with error", "instance", inst.InstanceID, "err", err)
			}
		}()
	}

	<-ctx.Done()
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
