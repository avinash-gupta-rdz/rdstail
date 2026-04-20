// Package logrecord defines the canonical log record type used across the pipeline.
// It is the only package exported from pkg/ — embedders of rdstail (custom
// Sink implementations) depend on it.
package logrecord

import "time"

// LogRecord is a single parsed log line pulled from an RDS log file.
//
// Marker is the AWS-opaque pagination token for the chunk that contains this
// record; it is not a byte offset (see plan §5). BatchID is a deterministic
// identifier for the fetch+write batch, suitable for downstream dedupe.
type LogRecord struct {
	InstanceID string    `json:"instance_id"`
	Engine     string    `json:"engine"`
	LogFile    string    `json:"log_file"`
	Timestamp  time.Time `json:"timestamp"`
	Message    string    `json:"message"`
	Marker     string    `json:"marker,omitempty"`
	BatchID    string    `json:"batch_id,omitempty"`
}
