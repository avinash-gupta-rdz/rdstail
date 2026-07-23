package memory_test

import (
	"context"
	"testing"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	"github.com/avinash-gupta-rdz/rdstail/internal/state/memory"
)

func TestMemoryStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	defer s.Close()

	if _, found, err := s.Get(ctx, "db", "f"); err != nil || found {
		t.Fatalf("empty get: found=%v err=%v", found, err)
	}
	if err := s.Set(ctx, "db", "f", state.Checkpoint{Marker: "m1"}); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Get(ctx, "db", "f")
	if err != nil || !found || got.Marker != "m1" {
		t.Fatalf("get after set: %+v found=%v err=%v", got, found, err)
	}
	_ = s.Set(ctx, "db", "g", state.Checkpoint{Marker: "m2"})
	list, err := s.List(ctx, "db")
	if err != nil || len(list) != 2 {
		t.Fatalf("list: len=%d err=%v", len(list), err)
	}
	if err := s.Delete(ctx, "db", "f"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.Get(ctx, "db", "f"); found {
		t.Fatal("expected deleted")
	}
}

func TestMemoryStore_RegisteredWithFactory(t *testing.T) {
	s, err := state.Open(context.Background(), state.Config{Type: "memory"})
	if err != nil {
		t.Fatalf("open via factory: %v", err)
	}
	_ = s.Close()
}
