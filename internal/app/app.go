// Package app is the top-level runtime orchestrator: it opens the state store,
// builds sinks, builds AWS clients, wires the scheduler, and handles signals.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
	"time"

	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/metrics"
	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/shard"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	sinkfactory "github.com/avinash-gupta-rdz/rdstail/internal/sink/factory"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
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
		"shard", cfg.Runtime.Shard,
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

	// Automatic checkpoint GC: prune rows for files not seen in
	// checkpoint_retention (rotated out, departed instances). Sweeps every 6h.
	if gc, ok := store.(state.GCer); ok && cfg.Runtime.CheckpointRetention > 0 {
		retention := cfg.Runtime.CheckpointRetention
		go func() {
			sweep := func() {
				n, err := gc.GCCheckpoints(ctx, time.Now().Add(-retention), false)
				switch {
				case err != nil && ctx.Err() == nil:
					lg.Warn("checkpoint gc failed", "err", err)
				case n > 0:
					lg.Info("checkpoint gc pruned stale rows", "count", n, "retention", retention.String())
				}
			}
			sweep()
			ticker := time.NewTicker(6 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sweep()
				}
			}
		}()
	}

	// Export per-sink DLQ depth so operators can alert on parked batches.
	if counter, ok := dlq.(metrics.DLQCounter); ok && cfg.Metrics.Enabled {
		names := make([]string, 0, len(cfg.Sinks))
		for i := range cfg.Sinks {
			names = append(names, cfg.Sinks[i].Name)
		}
		go metrics.WatchDLQDepth(ctx, counter, names, mx.DLQDepth, 30*time.Second, lg)
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

	resolver := newSpecResolver(cfg, lg)
	instances, err := resolver.Resolve(ctx)
	if err != nil {
		return fmt.Errorf("build instances: %w", err)
	}

	// Periodic re-discovery: if any discover block sets refresh_interval, the
	// scheduler reconciles the worker set on the smallest configured cadence.
	var refresh func(context.Context) ([]pipeline.InstanceSpec, error)
	var refreshInterval time.Duration
	for _, src := range cfg.Sources {
		if src.Discover == nil || src.Discover.RefreshInterval <= 0 {
			continue
		}
		if refreshInterval == 0 || src.Discover.RefreshInterval < refreshInterval {
			refreshInterval = src.Discover.RefreshInterval
		}
	}
	if refreshInterval > 0 {
		refresh = resolver.Resolve
		lg.Info("periodic re-discovery enabled", "interval", refreshInterval.String())
	}

	sched, err := pipeline.NewScheduler(pipeline.SchedulerOpts{
		Config:          cfg,
		Instances:       instances,
		Refresh:         refresh,
		RefreshInterval: refreshInterval,
		Store:           store,
		Sink:            outSink,
		Logger:          lg,
		LagGauge:        mx.IngestionLagSeconds,
		PollGauge:       mx.PollIntervalSeconds,
		StateOpsCounter: mx.StateStoreOpsTotal,
		AnomalyCounter:  mx.ReadAnomaliesTotal,
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

// maxInstances mirrors the config-layer default cap; discovery can push the
// total past what static validation could see, so it is re-checked here.
const maxInstances = 500

// specResolver turns the config's sources into InstanceSpecs — explicit
// entries plus tag-discovered ones, deduplicated by (region, instance ID).
// Resolve is safe to call repeatedly (periodic re-discovery); AWS clients are
// built once per (region, assume_role) and cached across calls.
type specResolver struct {
	cfg *config.Config
	lg  *slog.Logger

	mu      sync.Mutex
	clients map[clientKey]*awsrds.Client
	known   map[string]bool // (region|instance) keys already logged as discovered
}

type clientKey struct {
	region     string
	assumeRole string
}

func newSpecResolver(cfg *config.Config, lg *slog.Logger) *specResolver {
	return &specResolver{cfg: cfg, lg: lg, clients: map[clientKey]*awsrds.Client{}, known: map[string]bool{}}
}

func (r *specResolver) client(ctx context.Context, region, assumeRole string) (*awsrds.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := clientKey{region: region, assumeRole: assumeRole}
	if c, ok := r.clients[key]; ok {
		return c, nil
	}
	awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region, AssumeRole: assumeRole})
	if err != nil {
		return nil, fmt.Errorf("aws config for %s: %w", region, err)
	}
	c := awsrds.NewFromConfig(awsCfg)
	r.clients[key] = c
	return c, nil
}

// Resolve returns the current desired instance set.
func (r *specResolver) Resolve(ctx context.Context) ([]pipeline.InstanceSpec, error) {
	sh, err := shard.Parse(r.cfg.Runtime.Shard)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []pipeline.InstanceSpec
	add := func(spec pipeline.InstanceSpec) {
		k := spec.Region + "|" + spec.InstanceID
		if seen[k] || !sh.Owns(k) {
			return
		}
		seen[k] = true
		out = append(out, spec)
	}

	for _, src := range r.cfg.Sources {
		if src.Type != config.SourceTypeRDS {
			return nil, fmt.Errorf("unsupported source type %q", src.Type)
		}
		client, err := r.client(ctx, src.Region, src.AssumeRole)
		if err != nil {
			return nil, err
		}
		// Explicit instances without a configured engine: one
		// DescribeDBInstances listing gives every instance's engine, so a
		// source may mix MySQL, MariaDB and PostgreSQL.
		var engines map[string]string
		if src.Engine == "" && len(src.Instances) > 0 {
			all, err := rdssrc.ListInstances(ctx, client, "")
			if err != nil {
				return nil, fmt.Errorf("detect engines in %s: %w", src.Region, err)
			}
			engines = make(map[string]string, len(all))
			for _, d := range all {
				engines[d.ID] = d.Engine
			}
		}
		for _, inst := range src.Instances {
			engine := src.Engine
			if engine == "" {
				if engine = engines[inst]; engine == "" {
					return nil, fmt.Errorf("instance %q not found in %s (or its engine isn't supported); set engine: explicitly if it is", inst, src.Region)
				}
			}
			add(pipeline.InstanceSpec{
				InstanceID:   inst,
				Engine:       engine,
				Region:       src.Region,
				IncludeAudit: src.IncludeAudit,
				API:          client,
			})
		}
		if src.Discover != nil {
			found, err := rdssrc.DiscoverInstances(ctx, client, rdssrc.DiscoverFilter{
				Tags:        src.Discover.Tags,
				ExcludeTags: src.Discover.ExcludeTags,
				All:         src.Discover.All,
				Engine:      src.Engine,
			})
			if err != nil {
				return nil, fmt.Errorf("discover instances in %s: %w", src.Region, err)
			}
			if len(found) == 0 {
				r.lg.Warn("discovery matched no instances", "region", src.Region, "tags", src.Discover.Tags)
			}
			for _, d := range found {
				k := src.Region + "|" + d.ID
				r.mu.Lock()
				first := !r.known[k]
				r.known[k] = true
				r.mu.Unlock()
				if first {
					r.lg.Info("discovered instance", "instance", d.ID, "engine", d.Engine, "status", d.Status, "region", src.Region)
				}
				add(pipeline.InstanceSpec{
					InstanceID:   d.ID,
					Engine:       d.Engine,
					Region:       src.Region,
					IncludeAudit: src.IncludeAudit,
					API:          client,
				})
			}
		}
	}
	if len(out) > maxInstances {
		return nil, fmt.Errorf("%d instances after discovery exceeds the %d-instance cap (split into multiple rdstail deployments)", len(out), maxInstances)
	}
	return out, nil
}
