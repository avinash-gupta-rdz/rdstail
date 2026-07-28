package costestimate

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

// threeDayFleet retains 3 GiB over exactly 3 days → 1 GiB/day, 30 GiB/month.
func threeDayFleet() []InstanceUsage {
	return []InstanceUsage{{
		InstanceID: "db-1", Engine: "postgres",
		Files: []FileStat{
			{Size: 1 << 30, LastWritten: now.Add(-72 * time.Hour)},
			{Size: 1 << 30, LastWritten: now.Add(-36 * time.Hour)},
			{Size: 1 << 30, LastWritten: now},
		},
	}}
}

func TestEstimateProjectsDailyRate(t *testing.T) {
	r := Estimate(threeDayFleet(), now, DefaultPrices())
	if len(r.Instances) != 1 {
		t.Fatalf("instances = %d", len(r.Instances))
	}
	e := r.Instances[0]
	if e.Rough {
		t.Error("3 days of history should not be rough")
	}
	if e.SpanHours != 72 {
		t.Errorf("span = %v h, want 72", e.SpanHours)
	}
	if e.DailyBytes != 1<<30 {
		t.Errorf("daily = %d, want %d", e.DailyBytes, 1<<30)
	}
	if want := int64(30 << 30); e.MonthlyBytes != want {
		t.Errorf("monthly = %d, want %d", e.MonthlyBytes, want)
	}
	if r.TotalMonthlyBytes != e.MonthlyBytes {
		t.Errorf("total %d != instance %d", r.TotalMonthlyBytes, e.MonthlyBytes)
	}
}

func TestEstimateShortHistoryIsRoughAndClamped(t *testing.T) {
	u := []InstanceUsage{{
		InstanceID: "db-new", Engine: "mysql",
		Files: []FileStat{{Size: 600 << 20, LastWritten: now.Add(-10 * time.Minute)}},
	}}
	r := Estimate(u, now, DefaultPrices())
	e := r.Instances[0]
	if !e.Rough {
		t.Error("10 minutes of history must be rough")
	}
	if e.SpanHours != 1 {
		t.Errorf("span should clamp to 1h, got %v", e.SpanHours)
	}
	if e.DailyBytes != int64(600<<20)*24 {
		t.Errorf("daily = %d", e.DailyBytes)
	}
	if !hasNote(r, "db-new") {
		t.Errorf("expected a rough-history note, got %v", r.Notes)
	}
}

func TestEstimateNoTimestampsAssumesOneDay(t *testing.T) {
	u := []InstanceUsage{{
		InstanceID: "db-x", Engine: "mariadb",
		Files: []FileStat{{Size: 100 << 20}},
	}}
	r := Estimate(u, now, DefaultPrices())
	e := r.Instances[0]
	if e.DailyBytes != 100<<20 {
		t.Errorf("daily = %d, want the retained bytes as one day", e.DailyBytes)
	}
	if !e.Rough {
		t.Error("timestampless estimate must be rough")
	}
}

func TestEstimateFutureTimestampsFallBackToOneDay(t *testing.T) {
	// Clock skew: oldest LastWritten is in the future. Treat like the
	// no-timestamp case instead of clamping to 1h and multiplying by 24.
	u := []InstanceUsage{{
		InstanceID: "db-skew", Engine: "postgres",
		Files: []FileStat{{Size: 100 << 20, LastWritten: now.Add(10 * time.Minute)}},
	}}
	r := Estimate(u, now, DefaultPrices())
	e := r.Instances[0]
	if e.DailyBytes != 100<<20 || !e.Rough {
		t.Errorf("future timestamps should assume a one-day window, got %+v", e)
	}
}

func TestEstimateEmptyInstance(t *testing.T) {
	r := Estimate([]InstanceUsage{{InstanceID: "db-idle", Engine: "postgres"}}, now, DefaultPrices())
	e := r.Instances[0]
	if e.DailyBytes != 0 || e.MonthlyBytes != 0 || e.Rough {
		t.Errorf("empty instance should project zero, got %+v", e)
	}
	if !hasNote(r, "db-idle") {
		t.Errorf("expected a no-files note, got %v", r.Notes)
	}
}

// TestPriceMatchesREADMEExample pins the cost model to the README's worked
// example: 100 GB/mo → CloudWatch ≈ $53, rdstail ≈ $0.33 (PUTs + storage).
func TestPriceMatchesREADMEExample(t *testing.T) {
	c := price(100e9, DefaultPrices())
	if !approx(c.CloudWatchTotalUSD, 53.00, 0.01) {
		t.Errorf("cloudwatch = %v, want ≈53.00", c.CloudWatchTotalUSD)
	}
	// 100e9 / 5 MiB ≈ 19074 PUTs → $0.095; 10 GB × $0.023 = $0.23
	if !approx(c.S3PutUSD, 0.095, 0.005) {
		t.Errorf("puts = %v", c.S3PutUSD)
	}
	if !approx(c.S3StorageUSD, 0.23, 0.001) {
		t.Errorf("storage = %v", c.S3StorageUSD)
	}
	if c.SavingsPercent < 99 {
		t.Errorf("savings%% = %v, want ≥99", c.SavingsPercent)
	}
}

func TestPriceZeroVolume(t *testing.T) {
	c := price(0, DefaultPrices())
	if c.CloudWatchTotalUSD != 0 || c.RdstailTotalUSD != 0 || c.S3PutCount != 0 || c.SavingsPercent != 0 {
		t.Errorf("zero volume should price to zero: %+v", c)
	}
}

func TestPriceZeroCompressionRatioDoesNotDivide(t *testing.T) {
	p := DefaultPrices()
	p.CompressionRatio = 0
	c := price(10e9, p)
	if !approx(c.S3CompressedGB, 10, 0.001) {
		t.Errorf("ratio 0 should mean uncompressed, got %v", c.S3CompressedGB)
	}
}

func TestReportJSONRoundTrips(t *testing.T) {
	r := Estimate(threeDayFleet(), now, DefaultPrices())
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var parsed Report
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.TotalMonthlyBytes != r.TotalMonthlyBytes {
		t.Errorf("round trip lost totals: %d != %d", parsed.TotalMonthlyBytes, r.TotalMonthlyBytes)
	}
}

func TestTextRendering(t *testing.T) {
	r := Estimate(threeDayFleet(), now, DefaultPrices())
	txt := r.Text()
	for _, frag := range []string{
		"db-1", "postgres", "fleet total", "30.0 GiB",
		"CloudWatch export path", "rdstail path", "rdstail saves",
		"note:",
	} {
		if !strings.Contains(txt, frag) {
			t.Errorf("text output missing %q:\n%s", frag, txt)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"}, {512, "512 B"}, {1 << 10, "1.0 KiB"},
		{5 << 20, "5.0 MiB"}, {30 << 30, "30.0 GiB"}, {3 << 40, "3.0 TiB"},
	}
	for _, tc := range cases {
		if got := humanBytes(tc.n); got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func hasNote(r Report, frag string) bool {
	for _, n := range r.Notes {
		if strings.Contains(n, frag) {
			return true
		}
	}
	return false
}

func approx(got, want, tol float64) bool {
	return math.Abs(got-want) <= tol
}
