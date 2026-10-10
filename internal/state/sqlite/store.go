// Package sqlite is the default StateStore backend, built on modernc.org/sqlite
// (pure-Go — no CGO, keeping the release binary statically linkable).
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// Register the backend under the name "sqlite" so state.Open can find it.
func init() {
	state.Register("sqlite", func(ctx context.Context, cfg state.Config) (state.StateStore, error) {
		return Open(ctx, cfg.Path)
	})
}

// Store is the SQLite-backed checkpoint store.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite DB at path and applies pending migrations.
// WAL journaling is enabled for concurrent-reader safety.
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlite: path is required")
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize writes; SQLite is single-writer anyway

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// schemaVersion bumps whenever migrations change.
const schemaVersion = 2

func (s *Store) migrate(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS checkpoints (
    instance_id   TEXT NOT NULL,
    log_file      TEXT NOT NULL,
    marker        TEXT NOT NULL,
    bytes_written INTEGER NOT NULL DEFAULT 0,
    file_size     INTEGER NOT NULL DEFAULT 0,
    last_written  INTEGER NOT NULL DEFAULT 0,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (instance_id, log_file)
);
CREATE INDEX IF NOT EXISTS idx_ckpt_updated ON checkpoints(updated_at);
CREATE TABLE IF NOT EXISTS sinks_dlq (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sink_name  TEXT NOT NULL,
    batch_id   TEXT NOT NULL,
    payload    BLOB NOT NULL,
    reason     TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dlq_created ON sinks_dlq(created_at);
`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("sqlite migrate ddl: %w", err)
	}

	var current int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current)
	if err != nil {
		return fmt.Errorf("sqlite migrate read version: %w", err)
	}
	if current >= schemaVersion {
		return nil
	}
	// v2: skip_continuation (see state.Checkpoint.SkipContinuation). A fresh DB
	// reports version 0 and gets the column here too.
	var hasCol int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('checkpoints') WHERE name = 'skip_continuation'`).Scan(&hasCol); err != nil {
		return fmt.Errorf("sqlite migrate v2 probe: %w", err)
	}
	if hasCol == 0 {
		if _, err := s.db.ExecContext(ctx,
			`ALTER TABLE checkpoints ADD COLUMN skip_continuation INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("sqlite migrate v2: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_version(version) VALUES (?)`, schemaVersion); err != nil {
		return fmt.Errorf("sqlite migrate bump version: %w", err)
	}
	return nil
}

// DB exposes the underlying handle for tests and maintenance tooling. Regular
// callers must use the StateStore/DLQ/GCer interfaces.
func (s *Store) DB() *sql.DB { return s.db }

// Close closes the DB. Subsequent calls return nil.
func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// Get implements state.StateStore.
func (s *Store) Get(ctx context.Context, instance, logfile string) (state.Checkpoint, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT marker, bytes_written, file_size, last_written, skip_continuation
FROM checkpoints WHERE instance_id = ? AND log_file = ?`, instance, logfile)
	var c state.Checkpoint
	var lastMS int64
	err := row.Scan(&c.Marker, &c.BytesWritten, &c.FileSize, &lastMS, &c.SkipContinuation)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return state.Checkpoint{}, false, nil
	case err != nil:
		return state.Checkpoint{}, false, fmt.Errorf("sqlite get: %w", err)
	}
	if lastMS > 0 {
		c.LastWritten = time.UnixMilli(lastMS).UTC()
	}
	return c, true, nil
}

// Set implements state.StateStore.
func (s *Store) Set(ctx context.Context, instance, logfile string, c state.Checkpoint) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO checkpoints(instance_id, log_file, marker, bytes_written, file_size, last_written, skip_continuation, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(instance_id, log_file) DO UPDATE SET
    marker            = excluded.marker,
    bytes_written     = excluded.bytes_written,
    file_size         = excluded.file_size,
    last_written      = excluded.last_written,
    skip_continuation = excluded.skip_continuation,
    updated_at        = excluded.updated_at`,
		instance, logfile, c.Marker, c.BytesWritten, c.FileSize,
		c.LastWritten.UnixMilli(), c.SkipContinuation, time.Now().UTC().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("sqlite set: %w", err)
	}
	return nil
}

// List implements state.StateStore.
func (s *Store) List(ctx context.Context, instance string) ([]state.FileCheckpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT log_file, marker, bytes_written, file_size, last_written, skip_continuation
FROM checkpoints WHERE instance_id = ?`, instance)
	if err != nil {
		return nil, fmt.Errorf("sqlite list: %w", err)
	}
	defer rows.Close()

	var out []state.FileCheckpoint
	for rows.Next() {
		var fc state.FileCheckpoint
		var lastMS int64
		if err := rows.Scan(&fc.LogFile, &fc.Checkpoint.Marker, &fc.Checkpoint.BytesWritten, &fc.Checkpoint.FileSize, &lastMS, &fc.Checkpoint.SkipContinuation); err != nil {
			return nil, fmt.Errorf("sqlite list scan: %w", err)
		}
		if lastMS > 0 {
			fc.Checkpoint.LastWritten = time.UnixMilli(lastMS).UTC()
		}
		out = append(out, fc)
	}
	return out, rows.Err()
}

// Delete implements state.StateStore.
func (s *Store) Delete(ctx context.Context, instance, logfile string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checkpoints WHERE instance_id = ? AND log_file = ?`, instance, logfile)
	if err != nil {
		return fmt.Errorf("sqlite delete: %w", err)
	}
	return nil
}

// GCCheckpoints implements state.GCer: prunes checkpoint rows whose updated_at
// is before olderThan. Active files are Set on every poll, so only rows for
// rotated-out / departed files qualify.
func (s *Store) GCCheckpoints(ctx context.Context, olderThan time.Time, dryRun bool) (int64, error) {
	cutoff := olderThan.UTC().UnixMilli()
	if dryRun {
		var n int64
		err := s.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM checkpoints WHERE updated_at < ?`, cutoff).Scan(&n)
		if err != nil {
			return 0, fmt.Errorf("sqlite gc count: %w", err)
		}
		return n, nil
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM checkpoints WHERE updated_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("sqlite gc: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite gc rows: %w", err)
	}
	return n, nil
}

// --- DLQ (state.DLQ interface) ---

// DLQPut enqueues a dead-letter record.
func (s *Store) DLQPut(ctx context.Context, sinkName, batchID string, payload []byte, reason string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO sinks_dlq(sink_name, batch_id, payload, reason, created_at)
VALUES (?, ?, ?, ?, ?)`, sinkName, batchID, payload, reason, time.Now().UTC().UnixMilli())
	if err != nil {
		return fmt.Errorf("sqlite dlq put: %w", err)
	}
	return nil
}

// DLQList returns items matching q, oldest-first (ascending ID).
func (s *Store) DLQList(ctx context.Context, q state.DLQQuery) ([]state.DLQItem, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, sink_name, batch_id, payload, reason, created_at
FROM sinks_dlq
WHERE id > ? AND (? = '' OR sink_name = ?)
ORDER BY id ASC LIMIT ?`, q.AfterID, q.SinkName, q.SinkName, limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite dlq list: %w", err)
	}
	defer rows.Close()

	var out []state.DLQItem
	for rows.Next() {
		var it state.DLQItem
		var createdMS int64
		if err := rows.Scan(&it.ID, &it.SinkName, &it.BatchID, &it.Payload, &it.Reason, &createdMS); err != nil {
			return nil, fmt.Errorf("sqlite dlq list scan: %w", err)
		}
		it.CreatedAt = time.UnixMilli(createdMS).UTC()
		out = append(out, it)
	}
	return out, rows.Err()
}

// DLQDelete removes a DLQ entry (typically after successful replay).
func (s *Store) DLQDelete(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sinks_dlq WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("sqlite dlq delete: %w", err)
	}
	return nil
}

// DLQCount returns the number of parked items for sinkName ("" == all).
func (s *Store) DLQCount(ctx context.Context, sinkName string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sinks_dlq WHERE (? = '' OR sink_name = ?)`, sinkName, sinkName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sqlite dlq count: %w", err)
	}
	return n, nil
}

// DLQPurge deletes all items for sinkName ("" == all) and returns the count.
func (s *Store) DLQPurge(ctx context.Context, sinkName string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
DELETE FROM sinks_dlq WHERE (? = '' OR sink_name = ?)`, sinkName, sinkName)
	if err != nil {
		return 0, fmt.Errorf("sqlite dlq purge: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite dlq purge rows: %w", err)
	}
	return n, nil
}
