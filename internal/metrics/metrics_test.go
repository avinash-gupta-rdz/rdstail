package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"

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
