package metrics_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/avinash-gupta-rdz/rdstail/internal/metrics"
)

func TestCollectors_AreScrapable(t *testing.T) {
	m := metrics.New()
	m.LogsProcessedTotal.WithLabelValues("db-1", "postgres", "pg.log", "s3").Add(5)
	m.IngestionLagSeconds.WithLabelValues("db-1", "pg.log").Set(12.5)
	m.APICallsTotal.WithLabelValues("DescribeDBLogFiles", "ok").Inc()

	srv := httptest.NewServer(promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	wants := []string{
		`rdstail_logs_processed_total{`,
		`instance="db-1"`,
		`sink_type="s3"`,
		`rdstail_ingestion_lag_seconds{`,
		`rdstail_api_calls_total{`,
	}
	for _, w := range wants {
		if !strings.Contains(s, w) {
			t.Errorf("expected %q in scrape:\n%s", w, s)
		}
	}
}

type fakeDLQCounter struct {
	mu     sync.Mutex
	counts map[string]int64
	calls  int
}

func (f *fakeDLQCounter) DLQCount(_ context.Context, sink string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.counts[sink], nil
}

func TestWatchDLQDepth_UpdatesGauge(t *testing.T) {
	m := metrics.New()
	fake := &fakeDLQCounter{counts: map[string]int64{"s3-primary": 3, "siem": 0}}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		metrics.WatchDLQDepth(ctx, fake, []string{"s3-primary", "siem"}, m.DLQDepth, 5*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	// The first update is synchronous-ish; give a couple ticks then stop.
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-done

	if got := testutil.ToFloat64(m.DLQDepth.WithLabelValues("s3-primary")); got != 3 {
		t.Fatalf("expected depth 3 for s3-primary, got %v", got)
	}
	if got := testutil.ToFloat64(m.DLQDepth.WithLabelValues("siem")); got != 0 {
		t.Fatalf("expected depth 0 for siem, got %v", got)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.calls < 4 {
		t.Fatalf("expected multiple update passes, got %d calls", fake.calls)
	}
}
