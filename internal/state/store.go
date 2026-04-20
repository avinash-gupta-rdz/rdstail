// Package state defines the checkpoint interface shared by all fetchers and the
// factory that instantiates the configured backend (SQLite by default, JSON file
// as a dev fallback).
//
// Checkpoint durability is the #1 correctness dependency for rdstail —
// changes here must preserve the single-transaction Set semantics and the
// (instance_id, log_file) primary-key invariant.
package state

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Checkpoint captures the resume-state for a single (instance, logfile) pair.
//
// Marker is AWS's opaque pagination token (empty string == "fetch from beginning").
// BytesWritten and FileSize are tracked only for metrics and rotation-detection;
// they are NOT used as a source-of-truth offset (see plan §5).
type Checkpoint struct {
	Marker       string
	BytesWritten int64
	FileSize     int64
	LastWritten  time.Time
}

// FileCheckpoint pairs a log file name with its Checkpoint (used by List).
type FileCheckpoint struct {
	LogFile    string
	Checkpoint Checkpoint
}

// StateStore is the pluggable checkpoint store. Implementations MUST be safe for
// concurrent use from multiple goroutines and MUST persist Set() durably before
// returning nil (fsync or equivalent).
type StateStore interface {
	// Get returns the most-recently-persisted checkpoint for (instance, logfile).
	// found==false with a nil error means the pair has never been written.
	Get(ctx context.Context, instance, logfile string) (c Checkpoint, found bool, err error)

	// Set writes c atomically. Two concurrent Sets for the same key must not
	// interleave or lose data; last-writer-wins is acceptable.
	Set(ctx context.Context, instance, logfile string, c Checkpoint) error

	// List returns all per-logfile checkpoints known for an instance. Order is
	// unspecified.
	List(ctx context.Context, instance string) ([]FileCheckpoint, error)

	// Delete removes a single checkpoint. Returns nil if the row did not exist.
	Delete(ctx context.Context, instance, logfile string) error

	// Close releases any resources (DB handles, file locks).
	Close() error
}

// DLQItem is one record queued for a dead-letter sink after exhausting retries.
type DLQItem struct {
	ID        int64
	SinkName  string
	BatchID   string
	Payload   []byte
	Reason    string
	CreatedAt time.Time
}

// DLQ is an optional capability implemented by stores that persist dead-letter
// records. Callers should type-assert — not every backend implements it.
type DLQ interface {
	DLQPut(ctx context.Context, sinkName, batchID string, payload []byte, reason string) error
	DLQList(ctx context.Context, limit int) ([]DLQItem, error)
	DLQDelete(ctx context.Context, id int64) error
}

// Config selects and parameterises the concrete backend.
type Config struct {
	Type string // "sqlite" | "file"
	Path string
}

// ErrUnsupportedType is returned by Open when Config.Type is unknown.
var ErrUnsupportedType = errors.New("unsupported state store type")

// opener is the factory signature each backend package registers via Register.
type opener func(ctx context.Context, cfg Config) (StateStore, error)

var registry = map[string]opener{}

// Register is called from each backend's init() to expose itself to Open.
func Register(name string, fn opener) {
	registry[name] = fn
}

// Open instantiates the configured backend. Typical call:
//
//	store, err := state.Open(ctx, state.Config{Type: "sqlite", Path: "./state.db"})
func Open(ctx context.Context, cfg Config) (StateStore, error) {
	fn, ok := registry[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("%w: %q (known: %v)", ErrUnsupportedType, cfg.Type, known())
	}
	return fn(ctx, cfg)
}

func known() []string {
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}
