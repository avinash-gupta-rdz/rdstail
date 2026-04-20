// Package memory is an in-memory Sink used by tests. It records every batch it
// receives (optionally failing on demand), and is safe for concurrent writes.
package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// Sink records received batches in memory. Zero value is usable.
type Sink struct {
	mu       sync.Mutex
	name     string
	batches  [][]logrecord.LogRecord
	writeErr error
}

// New returns an in-memory sink with the given name.
func New(name string) *Sink { return &Sink{name: name} }

// Name implements sink.Sink.
func (s *Sink) Name() string {
	if s.name == "" {
		return "memory"
	}
	return s.name
}

// Type implements sink.Sink.
func (*Sink) Type() string { return "memory" }

// Write appends the batch to the internal log.
func (s *Sink) Write(_ context.Context, records []logrecord.LogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}
	batch := make([]logrecord.LogRecord, len(records))
	copy(batch, records)
	s.batches = append(s.batches, batch)
	return nil
}

// Close implements sink.Sink.
func (*Sink) Close() error { return nil }

// Batches returns a copy of all received batches.
func (s *Sink) Batches() [][]logrecord.LogRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]logrecord.LogRecord, len(s.batches))
	copy(out, s.batches)
	return out
}

// RecordCount is the total number of records across all received batches.
func (s *Sink) RecordCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

// FailNext causes the next (and subsequent) Write call(s) to fail with err.
// Pass nil to clear.
func (s *Sink) FailNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeErr = err
}

// ErrForced is a convenient sentinel returned by FailNext(...).
var ErrForced = errors.New("memory sink: forced error")
