package sink_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

func TestWithGrep_KeepsOnlyMatchingMessages(t *testing.T) {
	mem := memory.New("m")
	g := sink.WithGrep(mem, regexp.MustCompile(`deadlock|timeout`))

	in := []logrecord.LogRecord{
		{Message: "ERROR: deadlock detected"},
		{Message: "LOG: checkpoint complete"},
		{Message: "ERROR: canceling statement due to statement timeout"},
	}
	if err := g.Write(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if mem.RecordCount() != 2 {
		t.Fatalf("expected 2 kept, got %d: %v", mem.RecordCount(), mem.Batches())
	}
	// A fully-filtered batch ACKs without reaching the inner sink.
	if err := g.Write(context.Background(), []logrecord.LogRecord{{Message: "LOG: noise"}}); err != nil {
		t.Fatal(err)
	}
	if len(mem.Batches()) != 1 {
		t.Fatalf("filtered-out batch should not reach inner sink, got %d batches", len(mem.Batches()))
	}
}

func TestWithGrep_NilRegexpIsPassthrough(t *testing.T) {
	mem := memory.New("m")
	if got := sink.WithGrep(mem, nil); got != sink.Sink(mem) {
		t.Fatal("nil regexp should return the sink unchanged")
	}
}

func TestWithSince_CutsAtBoundary(t *testing.T) {
	mem := memory.New("m")
	cutoff := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	s := sink.WithSince(mem, cutoff)

	in := []logrecord.LogRecord{
		{Message: "old", Timestamp: cutoff.Add(-time.Second)},
		{Message: "at-cutoff", Timestamp: cutoff},
		{Message: "new", Timestamp: cutoff.Add(time.Second)},
	}
	if err := s.Write(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if mem.RecordCount() != 2 {
		t.Fatalf("expected at-cutoff+new kept, got %v", mem.Batches())
	}
	got := mem.Batches()[0]
	if got[0].Message != "at-cutoff" || got[1].Message != "new" {
		t.Fatalf("wrong records kept: %+v", got)
	}
}

func TestWithSince_ZeroCutoffIsPassthrough(t *testing.T) {
	mem := memory.New("m")
	if got := sink.WithSince(mem, time.Time{}); got != sink.Sink(mem) {
		t.Fatal("zero cutoff should return the sink unchanged")
	}
}
