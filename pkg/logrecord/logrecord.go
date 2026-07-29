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
//
// Timestamp is the server-side event time when the engine's log format could
// be parsed from the line (or inherited from the nearest preceding timestamped
// line in the same chunk), else the fetch time. Severity is the engine's
// upper-cased level token (ERROR, WARNING, FATAL, NOTE, ...) when present on
// the line, else empty. Message is always the raw, unmodified line.
type LogRecord struct {
	InstanceID string    `json:"instance_id"`
	Engine     string    `json:"engine"`
	LogFile    string    `json:"log_file"`
	Timestamp  time.Time `json:"timestamp"`
	Severity   string    `json:"severity,omitempty"`
	Message    string    `json:"message"`
	Marker     string    `json:"marker,omitempty"`
	BatchID    string    `json:"batch_id,omitempty"`
	Audit      *Audit    `json:"audit,omitempty"`
}

// Audit carries the fields pgAudit structures into its CSV payload
// (AUDIT: SESSION,1,1,READ,SELECT,TABLE,public.accounts,...). Only what the
// engine already structured is lifted — the statement text stays in Message,
// never interpreted. Nil on non-audit lines.
type Audit struct {
	Type       string `json:"type,omitempty"`        // SESSION | OBJECT
	Class      string `json:"class,omitempty"`       // READ, WRITE, DDL, ROLE, ...
	Command    string `json:"command,omitempty"`     // SELECT, INSERT, CREATE TABLE, ...
	ObjectType string `json:"object_type,omitempty"` // TABLE, VIEW, ... (empty for statement-level classes)
	ObjectName string `json:"object_name,omitempty"` // schema-qualified name
}
