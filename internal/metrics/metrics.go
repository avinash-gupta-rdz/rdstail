// Package metrics exposes the prometheus collectors used across the pipeline.
// Every collector is registered in its own private Registry so the HTTP server
// in this package controls exposure order and prefix.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles every collector used by the binary. One instance per process.
type Metrics struct {
	Registry *prometheus.Registry

	LogsProcessedTotal   *prometheus.CounterVec
	LogsFailedTotal      *prometheus.CounterVec
	IngestionLagSeconds  *prometheus.GaugeVec
	APICallsTotal        *prometheus.CounterVec
	PollIntervalSeconds  *prometheus.GaugeVec
	BatchBytes           *prometheus.HistogramVec
	SinkWriteDurationSec *prometheus.HistogramVec
	StateStoreOpsTotal   *prometheus.CounterVec
	DLQDepth             *prometheus.GaugeVec
	ReadAnomaliesTotal   *prometheus.CounterVec
}

// New builds a fresh Metrics with all collectors registered on a private Registry.
func New() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{Registry: r}

	m.LogsProcessedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rdstail_logs_processed_total",
		Help: "Total log records successfully delivered to a sink.",
	}, []string{"instance", "engine", "log_file", "sink_type"})

	m.LogsFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rdstail_logs_failed_total",
		Help: "Total log records that failed delivery (pre-DLQ).",
	}, []string{"instance", "sink_type", "reason"})

	m.IngestionLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rdstail_ingestion_lag_seconds",
		Help: "Seconds between the RDS log file's last_written timestamp and now. Reflects server-side clock; do not alert.",
	}, []string{"instance", "log_file"})

	m.APICallsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rdstail_api_calls_total",
		Help: "AWS API calls, labelled by operation and outcome (ok|error|throttled).",
	}, []string{"operation", "outcome"})

	m.PollIntervalSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rdstail_poll_interval_seconds",
		Help: "Current per-instance poll interval. Rises during idle backoff (adaptive polling), snaps back to base when data flows.",
	}, []string{"instance"})

	m.BatchBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rdstail_batch_bytes",
		Help:    "Serialised batch size per sink write (bytes).",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 14), // 1KB .. 8MB
	}, []string{"sink_type"})

	m.SinkWriteDurationSec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rdstail_sink_write_duration_seconds",
		Help:    "Wall-clock duration of Sink.Write calls.",
		Buckets: prometheus.DefBuckets,
	}, []string{"sink_type"})

	m.StateStoreOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rdstail_state_store_ops_total",
		Help: "State store operations, labelled by op and outcome.",
	}, []string{"op", "outcome"})

	m.DLQDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rdstail_dlq_depth",
		Help: "Dead-lettered batches currently parked, per sink. Nonzero means data is waiting for `rdstail dlq replay` — alert on growth.",
	}, []string{"sink_name"})

	m.ReadAnomaliesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rdstail_read_anomalies_total",
		Help: "Source-side events that mean log data may be missing or delayed: gap (RDS purged log hours before they were read), stall (file grows but its marker does not advance), truncated_line (a single line over RDS's 1 MB response cap was cut). Alert on any increase.",
	}, []string{"instance", "kind"})

	r.MustRegister(
		m.LogsProcessedTotal,
		m.LogsFailedTotal,
		m.IngestionLagSeconds,
		m.APICallsTotal,
		m.PollIntervalSeconds,
		m.BatchBytes,
		m.SinkWriteDurationSec,
		m.StateStoreOpsTotal,
		m.DLQDepth,
		m.ReadAnomaliesTotal,
	)
	return m
}
