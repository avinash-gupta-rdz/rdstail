package sqlite_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	sqlitestore "github.com/avinash-gupta-rdz/rdstail/internal/state/sqlite"
)

func newStore(t *testing.T) *sqlitestore.Store {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.db")
	s, err := sqlitestore.Open(context.Background(), p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGet_MissingReturnsFoundFalse(t *testing.T) {
	s := newStore(t)
	_, found, err := s.Get(context.Background(), "db-1", "postgresql.log.2025-01-01")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if found {
		t.Fatal("expected found=false for missing key")
	}
}

func TestSetThenGet_RoundTrip(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	want := state.Checkpoint{
		Marker:       "0:abc",
		BytesWritten: 1024,
		FileSize:     2048,
		LastWritten:  time.UnixMilli(1_700_000_000_000).UTC(),
	}
	if err := s.Set(ctx, "db-1", "pg.log", want); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, found, err := s.Get(ctx, "db-1", "pg.log")
	if err != nil || !found {
		t.Fatalf("get: err=%v found=%v", err, found)
	}
	if got != want {
		t.Fatalf("mismatch:\nwant %+v\n got %+v", want, got)
	}
}

func TestSet_Upsert(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_ = s.Set(ctx, "db-1", "pg.log", state.Checkpoint{Marker: "m1", BytesWritten: 100})
	_ = s.Set(ctx, "db-1", "pg.log", state.Checkpoint{Marker: "m2", BytesWritten: 200})
	got, _, _ := s.Get(ctx, "db-1", "pg.log")
	if got.Marker != "m2" || got.BytesWritten != 200 {
		t.Fatalf("upsert failed: %+v", got)
	}
}

func TestList_ByInstance(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_ = s.Set(ctx, "db-1", "a.log", state.Checkpoint{Marker: "1"})
	_ = s.Set(ctx, "db-1", "b.log", state.Checkpoint{Marker: "2"})
	_ = s.Set(ctx, "db-2", "c.log", state.Checkpoint{Marker: "3"})

	got, err := s.List(ctx, "db-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows for db-1, got %d", len(got))
	}
}

func TestDelete_Idempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if err := s.Delete(ctx, "db-x", "missing"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	_ = s.Set(ctx, "db-1", "pg.log", state.Checkpoint{Marker: "m"})
	_ = s.Delete(ctx, "db-1", "pg.log")
	_, found, _ := s.Get(ctx, "db-1", "pg.log")
	if found {
		t.Fatal("expected row gone after delete")
	}
}

func TestConcurrentSet_NoRace(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Set(ctx, "db-1", "pg.log", state.Checkpoint{Marker: "m", BytesWritten: int64(i)})
		}(i)
	}
	wg.Wait()
	_, found, err := s.Get(ctx, "db-1", "pg.log")
	if err != nil || !found {
		t.Fatalf("get after concurrent writes: err=%v found=%v", err, found)
	}
}

func TestDLQ_PutListDelete(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if err := s.DLQPut(ctx, "s3-primary", "b1", []byte("payload"), "timeout"); err != nil {
		t.Fatalf("put: %v", err)
	}
	items, err := s.DLQList(ctx, state.DLQQuery{Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("list: err=%v len=%d", err, len(items))
	}
	if items[0].SinkName != "s3-primary" || items[0].Reason != "timeout" {
		t.Fatalf("bad item: %+v", items[0])
	}
	if err := s.DLQDelete(ctx, items[0].ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	items, _ = s.DLQList(ctx, state.DLQQuery{Limit: 10})
	if len(items) != 0 {
		t.Fatalf("expected 0 after delete, got %d", len(items))
	}
}

func TestDLQ_FilterPaginateCountPurge(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := s.DLQPut(ctx, "s3-primary", "b", []byte("p"), "r"); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := s.DLQPut(ctx, "kafka-hot", "b", []byte("p"), "r"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Filter by sink.
	items, err := s.DLQList(ctx, state.DLQQuery{SinkName: "s3-primary"})
	if err != nil || len(items) != 3 {
		t.Fatalf("filtered list: err=%v len=%d", err, len(items))
	}

	// Paginate with AfterID: page size 2 then resume from cursor.
	page1, err := s.DLQList(ctx, state.DLQQuery{SinkName: "s3-primary", Limit: 2})
	if err != nil || len(page1) != 2 {
		t.Fatalf("page1: err=%v len=%d", err, len(page1))
	}
	page2, err := s.DLQList(ctx, state.DLQQuery{SinkName: "s3-primary", AfterID: page1[1].ID, Limit: 2})
	if err != nil || len(page2) != 1 {
		t.Fatalf("page2: err=%v len=%d", err, len(page2))
	}
	if page2[0].ID <= page1[1].ID {
		t.Fatalf("pagination not monotonic: %d <= %d", page2[0].ID, page1[1].ID)
	}

	// Counts.
	if n, err := s.DLQCount(ctx, ""); err != nil || n != 4 {
		t.Fatalf("count all: err=%v n=%d", err, n)
	}
	if n, err := s.DLQCount(ctx, "kafka-hot"); err != nil || n != 1 {
		t.Fatalf("count kafka: err=%v n=%d", err, n)
	}

	// Purge one sink, then the rest.
	if n, err := s.DLQPurge(ctx, "s3-primary"); err != nil || n != 3 {
		t.Fatalf("purge s3: err=%v n=%d", err, n)
	}
	if n, err := s.DLQPurge(ctx, ""); err != nil || n != 1 {
		t.Fatalf("purge all: err=%v n=%d", err, n)
	}
	if n, _ := s.DLQCount(ctx, ""); n != 0 {
		t.Fatalf("expected empty DLQ, got %d", n)
	}
}

func TestReopen_RetainsData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p := filepath.Join(dir, "state.db")
	s, err := sqlitestore.Open(ctx, p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = s.Set(ctx, "db-1", "pg.log", state.Checkpoint{Marker: "durable"})
	_ = s.Close()

	s2, err := sqlitestore.Open(ctx, p)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, found, _ := s2.Get(ctx, "db-1", "pg.log")
	if !found || got.Marker != "durable" {
		t.Fatalf("expected durable=marker after reopen, got %+v found=%v", got, found)
	}
}

func TestGCCheckpoints_PrunesOnlyStaleRows(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	// Two rows: one fresh, one that we'll age by rewriting updated_at directly.
	_ = s.Set(ctx, "db-1", "postgresql.log", state.Checkpoint{Marker: "fresh"})
	_ = s.Set(ctx, "db-1", "postgresql.log.2025-01-01-00", state.Checkpoint{Marker: "stale"})
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE checkpoints SET updated_at = ? WHERE log_file = ?`,
		time.Now().Add(-40*24*time.Hour).UnixMilli(), "postgresql.log.2025-01-01-00"); err != nil {
		t.Fatal(err)
	}

	// Dry run counts without deleting.
	n, err := s.GCCheckpoints(ctx, time.Now().Add(-30*24*time.Hour), true)
	if err != nil || n != 1 {
		t.Fatalf("dry-run: n=%d err=%v", n, err)
	}
	if _, found, _ := s.Get(ctx, "db-1", "postgresql.log.2025-01-01-00"); !found {
		t.Fatal("dry-run must not delete")
	}

	// Real run deletes the stale row only.
	n, err = s.GCCheckpoints(ctx, time.Now().Add(-30*24*time.Hour), false)
	if err != nil || n != 1 {
		t.Fatalf("gc: n=%d err=%v", n, err)
	}
	if _, found, _ := s.Get(ctx, "db-1", "postgresql.log.2025-01-01-00"); found {
		t.Fatal("stale row must be gone")
	}
	if _, found, _ := s.Get(ctx, "db-1", "postgresql.log"); !found {
		t.Fatal("fresh row must survive")
	}
}

// A state.db written by schema v1 (no skip_continuation column) must open,
// keep its checkpoints, and round-trip the new field.
func TestMigrate_V1ToV2_KeepsCheckpoints(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "state.db")
	s, err := sqlitestore.Open(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	db := s.DB()
	for _, q := range []string{
		`DROP TABLE checkpoints`,
		`CREATE TABLE checkpoints (instance_id TEXT NOT NULL, log_file TEXT NOT NULL, marker TEXT NOT NULL,
		 bytes_written INTEGER NOT NULL DEFAULT 0, file_size INTEGER NOT NULL DEFAULT 0,
		 last_written INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL, PRIMARY KEY (instance_id, log_file))`,
		`INSERT INTO checkpoints VALUES ('db-1','general/mysql-general.log','2026-10-10.7:185849',0,185849,0,1)`,
		`DELETE FROM schema_version`,
		`INSERT INTO schema_version(version) VALUES (1)`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_ = s.Close()

	s, err = sqlitestore.Open(ctx, p)
	if err != nil {
		t.Fatalf("reopen v1 db: %v", err)
	}
	defer func() { _ = s.Close() }()
	got, ok, err := s.Get(ctx, "db-1", "general/mysql-general.log")
	if err != nil || !ok || got.Marker != "2026-10-10.7:185849" || got.SkipContinuation {
		t.Fatalf("v1 checkpoint not preserved: %+v ok=%v err=%v", got, ok, err)
	}
	got.SkipContinuation = true
	if err := s.Set(ctx, "db-1", "general/mysql-general.log", got); err != nil {
		t.Fatal(err)
	}
	got, _, _ = s.Get(ctx, "db-1", "general/mysql-general.log")
	if !got.SkipContinuation {
		t.Fatal("skip_continuation not persisted")
	}
}
