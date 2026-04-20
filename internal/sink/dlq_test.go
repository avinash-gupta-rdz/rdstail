package sink_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

type mockDLQ struct {
	mu    sync.Mutex
	items []state.DLQItem
}

func (m *mockDLQ) DLQPut(_ context.Context, sinkName, batchID string, payload []byte, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, state.DLQItem{SinkName: sinkName, BatchID: batchID, Payload: payload, Reason: reason})
	return nil
}

func (m *mockDLQ) DLQList(_ context.Context, _ int) ([]state.DLQItem, error) { return m.items, nil }
func (m *mockDLQ) DLQDelete(_ context.Context, _ int64) error                 { return nil }

type alwaysFail struct{ err error }

func (a *alwaysFail) Name() string                                          { return "fail" }
func (a *alwaysFail) Type() string                                          { return "test" }
func (a *alwaysFail) Close() error                                          { return nil }
func (a *alwaysFail) Write(_ context.Context, _ []logrecord.LogRecord) error { return a.err }

func TestDLQ_ParksPermanentFailures(t *testing.T) {
	dlq := &mockDLQ{}
	inner := &alwaysFail{err: errors.New("4xx")}
	s := sink.WithDLQ(inner, dlq)

	recs := []logrecord.LogRecord{{InstanceID: "db", LogFile: "f", Message: "m", BatchID: "b1"}}
	if err := s.Write(context.Background(), recs); err != nil {
		t.Fatalf("DLQ wrap should swallow err, got %v", err)
	}
	if len(dlq.items) != 1 {
		t.Fatalf("expected 1 dlq item, got %d", len(dlq.items))
	}
	it := dlq.items[0]
	if it.BatchID != "b1" || it.SinkName != "fail" || it.Reason != "4xx" {
		t.Fatalf("bad dlq item: %+v", it)
	}
	var parsed []logrecord.LogRecord
	if err := json.Unmarshal(it.Payload, &parsed); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if parsed[0].Message != "m" {
		t.Fatalf("bad payload: %+v", parsed)
	}
}

func TestDLQ_PassthroughOnSuccess(t *testing.T) {
	dlq := &mockDLQ{}
	inner := &alwaysFail{err: nil}
	s := sink.WithDLQ(inner, dlq)
	if err := s.Write(context.Background(), []logrecord.LogRecord{{BatchID: "x"}}); err != nil {
		t.Fatal(err)
	}
	if len(dlq.items) != 0 {
		t.Fatalf("expected 0 dlq items on success, got %d", len(dlq.items))
	}
}

func TestDLQ_DoesNotParkContextCancel(t *testing.T) {
	dlq := &mockDLQ{}
	inner := &alwaysFail{err: context.Canceled}
	s := sink.WithDLQ(inner, dlq)
	err := s.Write(context.Background(), []logrecord.LogRecord{{BatchID: "x"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled to propagate, got %v", err)
	}
	if len(dlq.items) != 0 {
		t.Fatalf("expected no DLQ on cancel, got %d", len(dlq.items))
	}
}

func TestDLQ_NilDLQ_IsPassthrough(t *testing.T) {
	inner := &alwaysFail{err: errors.New("nope")}
	s := sink.WithDLQ(inner, nil)
	if err := s.Write(context.Background(), []logrecord.LogRecord{{BatchID: "x"}}); err == nil {
		t.Fatal("expected err to propagate when DLQ is nil")
	}
}
