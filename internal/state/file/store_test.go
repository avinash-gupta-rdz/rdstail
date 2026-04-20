package file_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	filestore "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
)

func newStore(t *testing.T) (*filestore.Store, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.json")
	s, err := filestore.Open(p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, p
}

func TestRoundTrip(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	want := state.Checkpoint{Marker: "m1", BytesWritten: 10, FileSize: 20, LastWritten: time.UnixMilli(1000).UTC()}
	if err := s.Set(ctx, "a", "f", want); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Get(ctx, "a", "f")
	if err != nil || !found {
		t.Fatalf("get: err=%v found=%v", err, found)
	}
	if got != want {
		t.Fatalf("mismatch: %+v vs %+v", got, want)
	}
}

func TestReload_AfterAtomicWrite(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "state.json")
	s, err := filestore.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Set(ctx, "a", "f", state.Checkpoint{Marker: "persisted"})
	_ = s.Close()

	s2, err := filestore.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	got, found, _ := s2.Get(ctx, "a", "f")
	if !found || got.Marker != "persisted" {
		t.Fatalf("expected reload: %+v found=%v", got, found)
	}
}

func TestListByInstance(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	_ = s.Set(ctx, "i1", "a", state.Checkpoint{Marker: "m1"})
	_ = s.Set(ctx, "i1", "b", state.Checkpoint{Marker: "m2"})
	_ = s.Set(ctx, "i2", "c", state.Checkpoint{Marker: "m3"})

	got, err := s.List(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestConcurrentSet(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Set(ctx, "a", "f", state.Checkpoint{Marker: "m", BytesWritten: int64(i)})
		}(i)
	}
	wg.Wait()
	_, found, _ := s.Get(ctx, "a", "f")
	if !found {
		t.Fatal("expected record after concurrent writes")
	}
}

func TestDelete(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	_ = s.Set(ctx, "a", "f", state.Checkpoint{Marker: "m"})
	_ = s.Delete(ctx, "a", "f")
	_, found, _ := s.Get(ctx, "a", "f")
	if found {
		t.Fatal("expected gone after delete")
	}
}
