package sink_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

type flakySink struct {
	failUntil int
	calls     atomic.Int32
	errToReturn error
}

func (f *flakySink) Name() string { return "flaky" }
func (f *flakySink) Type() string { return "test" }
func (f *flakySink) Close() error { return nil }
func (f *flakySink) Write(_ context.Context, _ []logrecord.LogRecord) error {
	n := f.calls.Add(1)
	if int(n) <= f.failUntil {
		return f.errToReturn
	}
	return nil
}

func TestRetry_SuccessAfterTransientFailures(t *testing.T) {
	f := &flakySink{failUntil: 3, errToReturn: errors.New("boom")}
	s := sink.WithRetry(f, sink.RetryConfig{MaxAttempts: 5, InitialWait: 1 * time.Millisecond, MaxWait: 5 * time.Millisecond})
	if err := s.Write(context.Background(), nil); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if f.calls.Load() != 4 {
		t.Fatalf("expected 4 attempts (3 fail + 1 success), got %d", f.calls.Load())
	}
}

func TestRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	f := &flakySink{failUntil: 999, errToReturn: errors.New("always")}
	s := sink.WithRetry(f, sink.RetryConfig{MaxAttempts: 3, InitialWait: 1 * time.Millisecond, MaxWait: 2 * time.Millisecond})
	err := s.Write(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error after max attempts")
	}
	if f.calls.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", f.calls.Load())
	}
}

func TestRetry_PermanentErrorShortCircuits(t *testing.T) {
	f := &flakySink{failUntil: 999, errToReturn: sink.NewPermanentError(errors.New("4xx"))}
	s := sink.WithRetry(f, sink.RetryConfig{MaxAttempts: 10, InitialWait: 1 * time.Millisecond})
	err := s.Write(context.Background(), nil)
	if err == nil || !errors.Is(err, f.errToReturn) {
		t.Fatalf("expected the wrapped cause, got %v", err)
	}
	if f.calls.Load() != 1 {
		t.Fatalf("expected 1 attempt (permanent), got %d", f.calls.Load())
	}
}

func TestRetry_RespectsContextCancel(t *testing.T) {
	f := &flakySink{failUntil: 999, errToReturn: errors.New("boom")}
	s := sink.WithRetry(f, sink.RetryConfig{MaxAttempts: 100, InitialWait: 50 * time.Millisecond, MaxWait: 50 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := s.Write(ctx, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}
