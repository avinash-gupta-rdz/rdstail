package replay_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/replay"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	sqlitestore "github.com/avinash-gupta-rdz/rdstail/internal/state/sqlite"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

func newStore(t *testing.T) *sqlitestore.Store {
	t.Helper()
	s, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fastRetry keeps replay's retry wrapper from slowing failure-path tests.
var fastRetry = config.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond, Multiplier: 2}

func testConfig(names ...string) *config.Config {
	cfg := &config.Config{}
	for _, n := range names {
		cfg.Sinks = append(cfg.Sinks, config.Sink{Name: n, Type: "memory", Retry: fastRetry})
	}
	return cfg
}

func put(t *testing.T, dlq state.DLQ, sinkName, batchID string, msgs ...string) {
	t.Helper()
	recs := make([]logrecord.LogRecord, 0, len(msgs))
	for _, m := range msgs {
		recs = append(recs, logrecord.LogRecord{InstanceID: "db-1", LogFile: "pg.log", Message: m, BatchID: batchID})
	}
	payload, err := json.Marshal(recs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := dlq.DLQPut(context.Background(), sinkName, batchID, payload, "test"); err != nil {
		t.Fatalf("dlq put: %v", err)
	}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func builderFor(sinks map[string]*memory.Sink) replay.SinkBuilder {
	return func(_ context.Context, cfgSink *config.Sink) (sink.Sink, error) {
		s, ok := sinks[cfgSink.Name]
		if !ok {
			return nil, errors.New("no memory sink registered: " + cfgSink.Name)
		}
		return s, nil
	}
}

func TestReplay_DrainsAndDeletes(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	put(t, store, "s3-primary", "b1", "line-1", "line-2")
	put(t, store, "s3-primary", "b2", "line-3")

	mem := memory.New("s3-primary")
	res, err := replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(), replay.Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Replayed != 2 || res.Failed != 0 || res.Skipped != 0 {
		t.Fatalf("bad result: %+v", res)
	}
	if mem.RecordCount() != 3 {
		t.Fatalf("expected 3 records delivered, got %d", mem.RecordCount())
	}
	if n, _ := store.DLQCount(ctx, ""); n != 0 {
		t.Fatalf("expected empty DLQ after replay, got %d", n)
	}
}

func TestReplay_FailureLeavesRow(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	put(t, store, "s3-primary", "b1", "line-1")

	mem := memory.New("s3-primary")
	mem.FailNext(memory.ErrForced)
	res, err := replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(), replay.Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Replayed != 0 || res.Failed != 1 {
		t.Fatalf("bad result: %+v", res)
	}
	if n, _ := store.DLQCount(ctx, ""); n != 1 {
		t.Fatalf("row must survive a failed replay, got count %d", n)
	}

	// Fix the sink; a second run drains it.
	mem.FailNext(nil)
	res, err = replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(), replay.Options{})
	if err != nil || res.Replayed != 1 {
		t.Fatalf("second run: err=%v res=%+v", err, res)
	}
	if n, _ := store.DLQCount(ctx, ""); n != 0 {
		t.Fatalf("expected drained DLQ, got %d", n)
	}
}

func TestReplay_SinkFilter(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	put(t, store, "s3-primary", "b1", "s3-line")
	put(t, store, "kafka-hot", "b2", "kafka-line")

	memS3 := memory.New("s3-primary")
	res, err := replay.Run(ctx, testConfig("s3-primary", "kafka-hot"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": memS3}), discard(),
		replay.Options{SinkFilter: "s3-primary"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Replayed != 1 {
		t.Fatalf("bad result: %+v", res)
	}
	if n, _ := store.DLQCount(ctx, "kafka-hot"); n != 1 {
		t.Fatalf("kafka item must be untouched, got count %d", n)
	}
}

func TestReplay_UnknownSinkSkipped(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	put(t, store, "removed-sink", "b1", "orphan")

	res, err := replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{}), discard(), replay.Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Skipped != 1 || res.Replayed != 0 {
		t.Fatalf("bad result: %+v", res)
	}
	if n, _ := store.DLQCount(ctx, ""); n != 1 {
		t.Fatalf("orphan row must remain, got count %d", n)
	}
}

func TestReplay_DryRun(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	put(t, store, "s3-primary", "b1", "line-1")

	mem := memory.New("s3-primary")
	res, err := replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(),
		replay.Options{DryRun: true})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Replayed != 1 {
		t.Fatalf("bad result: %+v", res)
	}
	if mem.RecordCount() != 0 {
		t.Fatalf("dry-run must not write, got %d records", mem.RecordCount())
	}
	if n, _ := store.DLQCount(ctx, ""); n != 1 {
		t.Fatalf("dry-run must not delete, got count %d", n)
	}
}

func TestReplay_LimitAndPagination(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	for i := 0; i < 5; i++ {
		put(t, store, "s3-primary", "b", "line")
	}

	mem := memory.New("s3-primary")
	// PageSize 2 forces multiple DLQList pages; Limit 3 stops mid-drain.
	res, err := replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(),
		replay.Options{Limit: 3, PageSize: 2})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Replayed != 3 {
		t.Fatalf("bad result: %+v", res)
	}
	if n, _ := store.DLQCount(ctx, ""); n != 2 {
		t.Fatalf("expected 2 remaining, got %d", n)
	}

	// A second unbounded run drains the rest.
	res, err = replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(),
		replay.Options{PageSize: 2})
	if err != nil || res.Replayed != 2 {
		t.Fatalf("second run: err=%v res=%+v", err, res)
	}
}

func TestReplay_UndecodablePayloadFails(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	if err := store.DLQPut(ctx, "s3-primary", "b1", []byte("not-json"), "test"); err != nil {
		t.Fatalf("put: %v", err)
	}

	mem := memory.New("s3-primary")
	res, err := replay.Run(ctx, testConfig("s3-primary"), store,
		builderFor(map[string]*memory.Sink{"s3-primary": mem}), discard(), replay.Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Failed != 1 || res.Replayed != 0 {
		t.Fatalf("bad result: %+v", res)
	}
	if n, _ := store.DLQCount(ctx, ""); n != 1 {
		t.Fatalf("undecodable row must remain for forensics, got %d", n)
	}
}
