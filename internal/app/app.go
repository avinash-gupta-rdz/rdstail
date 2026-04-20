// Package app is the top-level runtime orchestrator: it opens the state store,
// builds sinks, builds AWS clients, wires the scheduler, and handles signals.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/metrics"
	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	sinkfactory "github.com/avinash-gupta-rdz/rdstail/internal/sink/factory"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"

	// side-effect registrations
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/sqlite"
)

// Run blocks until ctx is cancelled or a termination signal is received. It
// owns the lifecycle of the state store, sinks, and scheduler.
func Run(ctx context.Context, cfg *config.Config, lg *slog.Logger) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lg.Info("starting rdstail",
		"sources", len(cfg.Sources),
		"sinks", len(cfg.Sinks),
		"poll_interval", cfg.Runtime.PollInterval.String(),
		"max_workers", cfg.Runtime.MaxWorkers,
		"max_instances_concurrent", cfg.Runtime.MaxInstancesConcurrent,
		"state_type", cfg.State.Type,
	)

	store, err := state.Open(ctx, state.Config{Type: cfg.State.Type, Path: cfg.State.Path})
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			lg.Warn("state store close", "err", err)
		}
	}()

	// DLQ: if the configured state store implements state.DLQ (SQLite does),
	// route terminal sink failures there. File-based store doesn't, so DLQ is nil.
	var dlq state.DLQ
	if d, ok := store.(state.DLQ); ok {
		dlq = d
	}

	// Metrics + HTTP exposure.
	mx := metrics.New()
	probe := &sink.PromMetrics{
		LogsProcessedTotal:   mx.LogsProcessedTotal,
		LogsFailedTotal:      mx.LogsFailedTotal,
		BatchBytes:           mx.BatchBytes,
		SinkWriteDurationSec: mx.SinkWriteDurationSec,
	}
	var metricsSrv *metrics.Server
	metricsDone := make(chan struct{})
	if cfg.Metrics.Enabled {
		metricsSrv = metrics.NewServer(cfg.Metrics.Listen, mx, lg)
		go func() {
			defer close(metricsDone)
			if err := metricsSrv.Run(ctx); err != nil {
				lg.Warn("metrics server", "err", err)
			}
		}()
	} else {
		close(metricsDone)
	}

	sinks, err := sinkfactory.BuildAll(ctx, cfg, dlq, probe)
	if err != nil {
		return fmt.Errorf("build sinks: %w", err)
	}
	defer func() {
		for _, s := range sinks {
			if err := s.Close(); err != nil {
				lg.Warn("sink close", "sink", s.Name(), "err", err)
			}
		}
	}()

	var outSink sink.Sink
	if len(sinks) == 1 {
		outSink = sinks[0]
	} else {
		outSink = sink.NewFanout(sinks...)
	}

	instances, err := buildInstanceSpecs(ctx, cfg)
	if err != nil {
		return fmt.Errorf("build instances: %w", err)
	}

	sched, err := pipeline.NewScheduler(pipeline.SchedulerOpts{
		Config:          cfg,
		Instances:       instances,
		Store:           store,
		Sink:            outSink,
		Logger:          lg,
		LagGauge:        mx.IngestionLagSeconds,
		StateOpsCounter: mx.StateStoreOpsTotal,
		APICallsCounter: mx.APICallsTotal,
	})
	if err != nil {
		return fmt.Errorf("new scheduler: %w", err)
	}

	if metricsSrv != nil {
		metricsSrv.MarkReady()
	}
	if err := sched.Run(ctx); err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}
	<-metricsDone
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(ctxErr, context.Canceled) {
		return fmt.Errorf("runtime: %w", ctxErr)
	}
	lg.Info("shutdown complete")
	return nil
}

// buildInstanceSpecs resolves one *awsrds.Client per (region, assume_role) tuple
// and produces one InstanceSpec per configured instance. Clients are shared
// across instances in the same region for efficiency.
func buildInstanceSpecs(ctx context.Context, cfg *config.Config) ([]pipeline.InstanceSpec, error) {
	type clientKey struct {
		region     string
		assumeRole string
	}
	clients := map[clientKey]*awsrds.Client{}

	var out []pipeline.InstanceSpec
	for _, src := range cfg.Sources {
		if src.Type != config.SourceTypeRDS {
			return nil, fmt.Errorf("unsupported source type %q", src.Type)
		}
		key := clientKey{region: src.Region, assumeRole: src.AssumeRole}
		client, ok := clients[key]
		if !ok {
			awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: src.Region, AssumeRole: src.AssumeRole})
			if err != nil {
				return nil, fmt.Errorf("aws config for %s: %w", src.Region, err)
			}
			client = awsrds.NewFromConfig(awsCfg)
			clients[key] = client
		}
		for _, inst := range src.Instances {
			out = append(out, pipeline.InstanceSpec{
				InstanceID: inst,
				Engine:     src.Engine,
				Region:     src.Region,
				API:        client,
			})
		}
	}
	return out, nil
}
