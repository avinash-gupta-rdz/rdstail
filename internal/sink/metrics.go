package sink

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// MetricsProbe is the minimal interface the metrics decorator calls on every
// Write. The process-wide *metrics.Metrics satisfies this — we keep it narrow
// so tests can inject fakes without importing prometheus.
type MetricsProbe interface {
	ObserveWrite(sinkType string, duration time.Duration, batchBytes int)
	IncProcessed(instance, engine, logFile, sinkType string, n int)
	IncFailed(instance, sinkType, reason string, n int)
}

// PromMetrics adapts the process-wide Metrics struct to MetricsProbe without
// importing the metrics package here (which would cycle). Instantiate in
// app.Run() and pass in.
type PromMetrics struct {
	LogsProcessedTotal   *prometheus.CounterVec
	LogsFailedTotal      *prometheus.CounterVec
	BatchBytes           *prometheus.HistogramVec
	SinkWriteDurationSec *prometheus.HistogramVec
}

// ObserveWrite records batch bytes and write duration.
func (p *PromMetrics) ObserveWrite(sinkType string, d time.Duration, n int) {
	if p == nil {
		return
	}
	p.SinkWriteDurationSec.WithLabelValues(sinkType).Observe(d.Seconds())
	p.BatchBytes.WithLabelValues(sinkType).Observe(float64(n))
}

// IncProcessed bumps the success counter.
func (p *PromMetrics) IncProcessed(instance, engine, logFile, sinkType string, n int) {
	if p == nil {
		return
	}
	p.LogsProcessedTotal.WithLabelValues(instance, engine, logFile, sinkType).Add(float64(n))
}

// IncFailed bumps the failure counter.
func (p *PromMetrics) IncFailed(instance, sinkType, reason string, n int) {
	if p == nil {
		return
	}
	p.LogsFailedTotal.WithLabelValues(instance, sinkType, reason).Add(float64(n))
}

type metricsDecorator struct {
	inner Sink
	m     MetricsProbe
}

// WithMetrics wraps s so Write calls record prometheus metrics. If m is nil,
// s is returned unchanged.
func WithMetrics(s Sink, m MetricsProbe) Sink {
	if m == nil {
		return s
	}
	return &metricsDecorator{inner: s, m: m}
}

func (d *metricsDecorator) Name() string { return d.inner.Name() }
func (d *metricsDecorator) Type() string { return d.inner.Type() }
func (d *metricsDecorator) Close() error { return d.inner.Close() }

func (d *metricsDecorator) Write(ctx context.Context, records []logrecord.LogRecord) error {
	start := time.Now()
	err := d.inner.Write(ctx, records)
	elapsed := time.Since(start)
	approxBytes := 0
	for i := range records {
		approxBytes += len(records[i].Message)
	}
	d.m.ObserveWrite(d.inner.Type(), elapsed, approxBytes)

	if err == nil {
		if len(records) > 0 {
			r0 := records[0]
			d.m.IncProcessed(r0.InstanceID, r0.Engine, sanitizeLogFile(r0.LogFile), d.inner.Type(), len(records))
		}
		return nil
	}
	reason := "error"
	if isPermanent(err) {
		reason = "permanent"
	}
	instance := ""
	if len(records) > 0 {
		instance = records[0].InstanceID
	}
	d.m.IncFailed(instance, d.inner.Type(), reason, len(records))
	return err
}

// sanitizeLogFile caps cardinality by stripping the directory and truncating.
func sanitizeLogFile(f string) string {
	// Keep just the basename; strip any date suffix after the last '.'.
	start := 0
	for i := len(f) - 1; i >= 0; i-- {
		if f[i] == '/' {
			start = i + 1
			break
		}
	}
	base := f[start:]
	if len(base) > 64 {
		base = base[:64]
	}
	return base
}

func isPermanent(err error) bool {
	for e := err; e != nil; {
		if _, ok := e.(*PermanentError); ok {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := e.(unwrapper)
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}
