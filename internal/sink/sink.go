// Package sink defines the Sink contract and generic helpers (Fanout, retry,
// metrics, DLQ wrappers) shared by all concrete sink implementations.
package sink

import (
	"context"
	"errors"
	"fmt"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// Sink writes a batch of records to a destination. Implementations MUST only
// return nil after the batch has been durably accepted (S3 2xx, Kafka acks=all,
// HTTP 2xx, etc.) — the pipeline advances its checkpoint based on this.
//
// Implementations SHOULD be safe for concurrent Write calls; the pipeline's
// KeyedRunner serialises writes per (instance, logfile) but different keys
// may dispatch concurrently.
type Sink interface {
	// Name is the operator-assigned sink name (unique per config).
	Name() string
	// Type is one of "s3", "kafka", "http", "memory".
	Type() string
	// Write delivers the batch. Returning nil means the batch is durable.
	Write(ctx context.Context, records []logrecord.LogRecord) error
	// Close releases any resources (TCP, producers, etc.).
	Close() error
}

// Fanout wraps multiple Sinks and delivers each batch to all of them. A Write
// succeeds only if every wrapped sink's Write returned nil; on any failure the
// joined error is returned and the pipeline will NOT advance its checkpoint.
//
// Sinks are written in the order given. If you need parallel writes, use
// FanoutParallel (future).
type Fanout struct {
	sinks []Sink
}

// NewFanout returns a Fanout over the given sinks. Panics if len(sinks)==0.
func NewFanout(sinks ...Sink) *Fanout {
	if len(sinks) == 0 {
		panic("sink.NewFanout: at least one sink required")
	}
	return &Fanout{sinks: sinks}
}

// Name returns a comma-joined list of wrapped sink names.
func (f *Fanout) Name() string {
	out := ""
	for i, s := range f.sinks {
		if i > 0 {
			out += ","
		}
		out += s.Name()
	}
	return out
}

// Type is always "fanout".
func (*Fanout) Type() string { return "fanout" }

// Write fans out to every sink sequentially and joins any errors.
func (f *Fanout) Write(ctx context.Context, records []logrecord.LogRecord) error {
	var errs []error
	for _, s := range f.sinks {
		if err := s.Write(ctx, records); err != nil {
			errs = append(errs, fmt.Errorf("sink %s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Close closes every sink, collecting any errors.
func (f *Fanout) Close() error {
	var errs []error
	for _, s := range f.sinks {
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sink %s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Sinks returns the wrapped sinks (read-only; do not mutate).
func (f *Fanout) Sinks() []Sink { return f.sinks }
