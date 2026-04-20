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
	items, err := s.DLQList(ctx, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("list: err=%v len=%d", err, len(items))
	}
	if items[0].SinkName != "s3-primary" || items[0].Reason != "timeout" {
		t.Fatalf("bad item: %+v", items[0])
	}
	if err := s.DLQDelete(ctx, items[0].ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	items, _ = s.DLQList(ctx, 10)
	if len(items) != 0 {
		t.Fatalf("expected 0 after delete, got %d", len(items))
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
