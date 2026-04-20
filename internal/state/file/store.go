// Package file is a JSON-file StateStore backend intended for dev/testing where
// a SQLite dep is unwelcome. Writes go to a temp file and are renamed atomically.
package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

func init() {
	state.Register("file", func(ctx context.Context, cfg state.Config) (state.StateStore, error) {
		return Open(cfg.Path)
	})
}

// Store persists checkpoints as a single JSON document on disk.
type Store struct {
	mu   sync.Mutex
	path string
	data fileData
}

type entry struct {
	Marker       string `json:"marker"`
	BytesWritten int64  `json:"bytes_written"`
	FileSize     int64  `json:"file_size"`
	LastWrittenMS int64 `json:"last_written_ms"`
}

type fileData struct {
	// Checkpoints keyed by "instance|logfile".
	Checkpoints map[string]entry `json:"checkpoints"`
}

// Open loads (or creates) a store at path.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("file store: path is required")
	}
	s := &Store{path: path, data: fileData{Checkpoints: map[string]entry{}}}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("file store open: %w", err)
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("file store read: %w", err)
	}
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, &s.data); err != nil {
		return fmt.Errorf("file store parse: %w", err)
	}
	if s.data.Checkpoints == nil {
		s.data.Checkpoints = map[string]entry{}
	}
	return nil
}

func (s *Store) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("file store mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("file store tmp: %w", err)
	}
	body, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("file store marshal: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("file store write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("file store fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("file store close tmp: %w", err)
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("file store rename: %w", err)
	}
	return nil
}

func key(instance, logfile string) string { return instance + "|" + logfile }

// Get implements state.StateStore.
func (s *Store) Get(_ context.Context, instance, logfile string) (state.Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data.Checkpoints[key(instance, logfile)]
	if !ok {
		return state.Checkpoint{}, false, nil
	}
	c := state.Checkpoint{
		Marker:       e.Marker,
		BytesWritten: e.BytesWritten,
		FileSize:     e.FileSize,
	}
	if e.LastWrittenMS > 0 {
		c.LastWritten = time.UnixMilli(e.LastWrittenMS).UTC()
	}
	return c, true, nil
}

// Set implements state.StateStore.
func (s *Store) Set(_ context.Context, instance, logfile string, c state.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Checkpoints[key(instance, logfile)] = entry{
		Marker:        c.Marker,
		BytesWritten:  c.BytesWritten,
		FileSize:      c.FileSize,
		LastWrittenMS: c.LastWritten.UnixMilli(),
	}
	return s.persistLocked()
}

// List implements state.StateStore.
func (s *Store) List(_ context.Context, instance string) ([]state.FileCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []state.FileCheckpoint
	prefix := instance + "|"
	for k, e := range s.data.Checkpoints {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		c := state.Checkpoint{Marker: e.Marker, BytesWritten: e.BytesWritten, FileSize: e.FileSize}
		if e.LastWrittenMS > 0 {
			c.LastWritten = time.UnixMilli(e.LastWrittenMS).UTC()
		}
		out = append(out, state.FileCheckpoint{LogFile: k[len(prefix):], Checkpoint: c})
	}
	return out, nil
}

// Delete implements state.StateStore.
func (s *Store) Delete(_ context.Context, instance, logfile string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Checkpoints, key(instance, logfile))
	return s.persistLocked()
}

// Close flushes any pending state and releases resources.
func (s *Store) Close() error { return nil }
