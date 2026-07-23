package sink_test

import (
	"context"
	"testing"

	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

func recs(sevs ...string) []logrecord.LogRecord {
	out := make([]logrecord.LogRecord, len(sevs))
	for i, s := range sevs {
		out[i] = logrecord.LogRecord{Severity: s, Message: "m-" + s, BatchID: "b"}
	}
	return out
}

func TestFilter_MinSeverity(t *testing.T) {
	mem := memory.New("m")
	f := sink.WithFilter(mem, sink.FilterConfig{MinSeverity: "ERROR"})

	// ERROR, FATAL, PANIC pass; WARNING, LOG drop; empty/continuation drop.
	in := recs("LOG", "WARNING", "ERROR", "FATAL", "PANIC", "", "STATEMENT")
	if err := f.Write(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if mem.RecordCount() != 3 {
		t.Fatalf("expected 3 kept (ERROR/FATAL/PANIC), got %d: %v", mem.RecordCount(), mem.Batches())
	}
	got := mem.Batches()[0]
	if got[0].Severity != "ERROR" || got[1].Severity != "FATAL" || got[2].Severity != "PANIC" {
		t.Fatalf("wrong records kept: %+v", got)
	}
}

func TestFilter_MinSeverity_CrossEngineTokens(t *testing.T) {
	mem := memory.New("m")
	f := sink.WithFilter(mem, sink.FilterConfig{MinSeverity: "WARNING"})
	// MySQL's NOTE/SYSTEM rank below WARNING; WARN counts as WARNING.
	if err := f.Write(context.Background(), recs("NOTE", "SYSTEM", "WARN", "ERROR")); err != nil {
		t.Fatal(err)
	}
	if mem.RecordCount() != 2 {
		t.Fatalf("expected WARN+ERROR kept, got %v", mem.Batches())
	}
}

func TestFilter_ExactSeverities_CaseInsensitive(t *testing.T) {
	mem := memory.New("m")
	f := sink.WithFilter(mem, sink.FilterConfig{Severities: []string{"fatal", "Error"}})
	if err := f.Write(context.Background(), recs("ERROR", "WARNING", "FATAL", "LOG")); err != nil {
		t.Fatal(err)
	}
	if mem.RecordCount() != 2 {
		t.Fatalf("expected ERROR+FATAL, got %v", mem.Batches())
	}
}

func TestFilter_FullyFilteredBatchAcksWithoutWrite(t *testing.T) {
	mem := memory.New("m")
	mem.FailNext(memory.ErrForced) // would fail if Write were called
	f := sink.WithFilter(mem, sink.FilterConfig{MinSeverity: "FATAL"})
	if err := f.Write(context.Background(), recs("LOG", "WARNING")); err != nil {
		t.Fatalf("fully-filtered batch must ACK, got %v", err)
	}
	if len(mem.Batches()) != 0 {
		t.Fatal("inner sink must not be written")
	}
}

func TestFilter_EmptyConfigIsPassthrough(t *testing.T) {
	mem := memory.New("m")
	if s := sink.WithFilter(mem, sink.FilterConfig{}); s != sink.Sink(mem) {
		t.Fatal("empty filter config must return the sink unchanged")
	}
}

func TestSeverityRank_Ordering(t *testing.T) {
	order := []string{"DEBUG", "LOG", "WARNING", "ERROR", "FATAL", "PANIC"}
	for i := 1; i < len(order); i++ {
		if sink.SeverityRank(order[i-1]) >= sink.SeverityRank(order[i]) {
			t.Fatalf("rank(%s) must be < rank(%s)", order[i-1], order[i])
		}
	}
	if sink.SeverityRank("STATEMENT") != 0 || sink.SeverityRank("") != 0 || sink.SeverityRank("bogus") != 0 {
		t.Fatal("unrankable tokens must rank 0")
	}
}
