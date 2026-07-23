// Package memory is an ephemeral in-memory StateStore. Checkpoints vanish with
// the process — used by `rdstail tail` (which wants tail -f semantics, not
// resume) and by tests. No DLQ capability.
package memory

import (
	"context"
	"sync"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// Register under "memory" so state.Open can build it (path is ignored).
func init() {
	state.Register("memory", func(_ context.Context, _ state.Config) (state.StateStore, error) {
		return New(), nil
	})
}

// Store is a mutex-guarded map. Safe for concurrent use.
type Store struct {
	mu sync.Mutex
	m  map[string]state.Checkpoint
}

// New returns an empty store.
func New() *Store { return &Store{m: map[string]state.Checkpoint{}} }

func key(instance, logfile string) string { return instance + "|" + logfile }

// Get implements state.StateStore.
func (s *Store) Get(_ context.Context, instance, logfile string) (state.Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.m[key(instance, logfile)]
	return c, ok, nil
}

// Set implements state.StateStore.
func (s *Store) Set(_ context.Context, instance, logfile string, c state.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key(instance, logfile)] = c
	return nil
}

// List implements state.StateStore.
func (s *Store) List(_ context.Context, instance string) ([]state.FileCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []state.FileCheckpoint
	prefix := instance + "|"
	for k, c := range s.m {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, state.FileCheckpoint{LogFile: k[len(prefix):], Checkpoint: c})
		}
	}
	return out, nil
}

// Delete implements state.StateStore.
func (s *Store) Delete(_ context.Context, instance, logfile string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key(instance, logfile))
	return nil
}

// Close implements state.StateStore.
func (*Store) Close() error { return nil }
