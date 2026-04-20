package state_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"

	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/sqlite"
)

func TestOpen_RoutesByType(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s1, err := state.Open(ctx, state.Config{Type: "sqlite", Path: filepath.Join(dir, "s.db")})
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	_ = s1.Close()

	s2, err := state.Open(ctx, state.Config{Type: "file", Path: filepath.Join(dir, "s.json")})
	if err != nil {
		t.Fatalf("file: %v", err)
	}
	_ = s2.Close()
}

func TestOpen_UnsupportedType(t *testing.T) {
	_, err := state.Open(context.Background(), state.Config{Type: "redis", Path: "/tmp/x"})
	if !errors.Is(err, state.ErrUnsupportedType) {
		t.Fatalf("expected ErrUnsupportedType, got %v", err)
	}
}
