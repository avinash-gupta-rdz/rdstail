// Package costestimate projects a fleet's monthly log volume from
// DescribeDBLogFiles metadata (sizes are free to read; nothing is downloaded)
// and prices the CloudWatch-export path against the rdstail path. Pure
// computation — AWS calls happen in the caller.
package costestimate

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	billableGB = 1e9 // AWS bills per decimal GB
	monthDays  = 30
	minSpan    = time.Hour
	roughSpan  = 24 * time.Hour
)

// FileStat is the per-file metadata the projection needs.
type FileStat struct {
	Size        int64
	LastWritten time.Time // zero when AWS reported none
}

// InstanceUsage is one instance's retained log files at observation time.
type InstanceUsage struct {
	InstanceID string
	Engine     string
	Files      []FileStat
}

// Prices are the unit prices the cost model uses. Defaults are us-east-1 list
// prices (mid-2026) — the same numbers as the README's worked example.
type Prices struct {
	CloudWatchIngestPerGB  float64 `json:"cloudwatch_ingest_per_gb"`
	CloudWatchStoragePerGB float64 `json:"cloudwatch_storage_per_gb"`
	S3PutPerThousand       float64 `json:"s3_put_per_1k"`
	S3StoragePerGB         float64 `json:"s3_storage_per_gb"`
	// CompressionRatio is the assumed NDJSON+gzip ratio for S3 storage (README
	// uses ~10:1 for text logs).
	CompressionRatio float64 `json:"compression_ratio"`
	// BatchBytes is the S3 object size used to count PUT requests
	// (runtime.max_batch_bytes; 5 MiB default).
	BatchBytes int64 `json:"batch_bytes"`
}

// DefaultPrices returns the us-east-1 list prices used in the README.
func DefaultPrices() Prices {
	return Prices{
		CloudWatchIngestPerGB:  0.50,
		CloudWatchStoragePerGB: 0.03,
		S3PutPerThousand:       0.005,
		S3StoragePerGB:         0.023,
		CompressionRatio:       10,
		BatchBytes:             5 << 20,
	}
}

// InstanceEstimate is one instance's projected volume.
type InstanceEstimate struct {
	InstanceID    string  `json:"instance_id"`
	Engine        string  `json:"engine"`
	FileCount     int     `json:"file_count"`
	RetainedBytes int64   `json:"retained_bytes"`
	SpanHours     float64 `json:"span_hours"`
	DailyBytes    int64   `json:"daily_bytes"`
	MonthlyBytes  int64   `json:"monthly_bytes"`
	// Rough marks projections extrapolated from under 24h of retained history.
	Rough bool `json:"rough_estimate,omitempty"`
}

// Costs is the priced comparison for the fleet's projected monthly volume.
type Costs struct {
	MonthlyGB            float64 `json:"monthly_gb"`
	CloudWatchIngestUSD  float64 `json:"cloudwatch_ingest_usd"`
	CloudWatchStorageUSD float64 `json:"cloudwatch_storage_usd"`
	CloudWatchTotalUSD   float64 `json:"cloudwatch_total_usd"`
	S3PutCount           int64   `json:"s3_put_count"`
	S3PutUSD             float64 `json:"s3_put_usd"`
	S3CompressedGB       float64 `json:"s3_compressed_gb"`
	S3StorageUSD         float64 `json:"s3_storage_usd"`
	RdstailTotalUSD      float64 `json:"rdstail_total_usd"`
	MonthlySavingsUSD    float64 `json:"monthly_savings_usd"`
	SavingsPercent       float64 `json:"savings_percent"`
}

// Report is the full estimate: per-instance projections, fleet totals, and the
// priced comparison.
type Report struct {
	Instances         []InstanceEstimate `json:"instances"`
	TotalDailyBytes   int64              `json:"total_daily_bytes"`
	TotalMonthlyBytes int64              `json:"total_monthly_bytes"`
	Prices            Prices             `json:"prices"`
	Costs             Costs              `json:"costs"`
	Notes             []string           `json:"notes,omitempty"`
}

// Estimate projects monthly volume and cost from the observed usages. now is
// the observation time; retained files are assumed to span from the oldest
// LastWritten to now, which slightly overstates the write rate (the oldest
// file's content began before its LastWritten) — a deliberately conservative
// bias, since overstated volume overstates the CloudWatch bill more than the
// rdstail one.
func Estimate(usages []InstanceUsage, now time.Time, p Prices) Report {
	sorted := make([]InstanceUsage, len(usages))
	copy(sorted, usages)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].InstanceID < sorted[j].InstanceID })

	r := Report{Prices: p}
	for _, u := range sorted {
		est := estimateInstance(u, now)
		r.Instances = append(r.Instances, est)
		r.TotalDailyBytes += est.DailyBytes
		r.TotalMonthlyBytes += est.MonthlyBytes
		if est.RetainedBytes == 0 {
			r.Notes = append(r.Notes, fmt.Sprintf("%s: no log data retained — is logging enabled on the instance?", u.InstanceID))
		} else if est.Rough {
			r.Notes = append(r.Notes, fmt.Sprintf("%s: limited log history (%.1fh window) — its projection is rough", u.InstanceID, est.SpanHours))
		}
	}
	r.Costs = price(r.TotalMonthlyBytes, p)
	r.Notes = append(r.Notes,
		"volume read from DescribeDBLogFiles metadata only — free, no log data downloaded",
		"compute cost excluded: rdstail runs on an existing node (a dedicated t4g.micro adds ~$6/mo)",
		"prices are us-east-1 list prices unless overridden — check your region's CloudWatch and S3 pricing pages",
	)
	return r
}

func estimateInstance(u InstanceUsage, now time.Time) InstanceEstimate {
	est := InstanceEstimate{
		InstanceID: u.InstanceID,
		Engine:     u.Engine,
		FileCount:  len(u.Files),
	}
	var oldest time.Time
	for _, f := range u.Files {
		est.RetainedBytes += f.Size
		if !f.LastWritten.IsZero() && (oldest.IsZero() || f.LastWritten.Before(oldest)) {
			oldest = f.LastWritten
		}
	}
	if est.RetainedBytes == 0 {
		return est
	}
	// Default: no usable window (no timestamps, or clock skew putting the
	// oldest write in the future) — assume the retained bytes represent a day.
	span := roughSpan
	est.Rough = true
	if !oldest.IsZero() && now.After(oldest) {
		span = max(now.Sub(oldest), minSpan)
		est.Rough = span < roughSpan
	}
	est.SpanHours = span.Hours()
	daily := float64(est.RetainedBytes) / span.Hours() * 24
	est.DailyBytes = int64(daily)
	est.MonthlyBytes = int64(daily * monthDays)
	return est
}

func price(monthlyBytes int64, p Prices) Costs {
	c := Costs{MonthlyGB: float64(monthlyBytes) / billableGB}
	c.CloudWatchIngestUSD = c.MonthlyGB * p.CloudWatchIngestPerGB
	c.CloudWatchStorageUSD = c.MonthlyGB * p.CloudWatchStoragePerGB
	c.CloudWatchTotalUSD = c.CloudWatchIngestUSD + c.CloudWatchStorageUSD

	if p.BatchBytes > 0 && monthlyBytes > 0 {
		c.S3PutCount = int64(math.Ceil(float64(monthlyBytes) / float64(p.BatchBytes)))
	}
	c.S3PutUSD = float64(c.S3PutCount) / 1000 * p.S3PutPerThousand
	if p.CompressionRatio > 0 {
		c.S3CompressedGB = c.MonthlyGB / p.CompressionRatio
	} else {
		c.S3CompressedGB = c.MonthlyGB
	}
	c.S3StorageUSD = c.S3CompressedGB * p.S3StoragePerGB
	c.RdstailTotalUSD = c.S3PutUSD + c.S3StorageUSD

	c.MonthlySavingsUSD = c.CloudWatchTotalUSD - c.RdstailTotalUSD
	if c.CloudWatchTotalUSD > 0 {
		c.SavingsPercent = c.MonthlySavingsUSD / c.CloudWatchTotalUSD * 100
	}
	return c
}

// Text renders the report as the human-readable comparison — the screenshot
// people paste into Slack.
func (r Report) Text() string {
	var b strings.Builder
	b.WriteString("Fleet log volume (from DescribeDBLogFiles metadata — free, nothing downloaded):\n\n")
	b.WriteString(fmt.Sprintf("  %-32s %-9s %5s  %10s  %8s  %11s  %13s\n",
		"INSTANCE", "ENGINE", "FILES", "RETAINED", "WINDOW", "EST. DAILY", "EST. MONTHLY"))
	for _, e := range r.Instances {
		window := "-"
		if e.SpanHours > 0 {
			window = fmt.Sprintf("%.1fd", e.SpanHours/24)
		}
		rough := ""
		if e.Rough {
			rough = " ~"
		}
		b.WriteString(fmt.Sprintf("  %-32s %-9s %5d  %10s  %8s  %11s  %12s%s\n",
			e.InstanceID, e.Engine, e.FileCount,
			humanBytes(e.RetainedBytes), window,
			humanBytes(e.DailyBytes), humanBytes(e.MonthlyBytes), rough))
	}
	b.WriteString(fmt.Sprintf("  %-32s %-9s %5s  %10s  %8s  %11s  %13s\n",
		"fleet total", "", "", "", "",
		humanBytes(r.TotalDailyBytes), humanBytes(r.TotalMonthlyBytes)))

	c, p := r.Costs, r.Prices
	b.WriteString(fmt.Sprintf("\nProjected monthly cost at %s/mo (%.2f GB billable):\n\n", humanBytes(r.TotalMonthlyBytes), c.MonthlyGB))
	line := func(label string, amount float64) {
		b.WriteString(fmt.Sprintf("  %-46s %9s\n", label, usd(amount)))
	}
	line("CloudWatch export path", c.CloudWatchTotalUSD)
	line(fmt.Sprintf("  ingestion   %.2f GB x $%.2f/GB", c.MonthlyGB, p.CloudWatchIngestPerGB), c.CloudWatchIngestUSD)
	line(fmt.Sprintf("  storage     %.2f GB x $%.2f/GB-mo", c.MonthlyGB, p.CloudWatchStoragePerGB), c.CloudWatchStorageUSD)
	line("rdstail path", c.RdstailTotalUSD)
	line(fmt.Sprintf("  S3 PUTs     %d x $%.3f/1k", c.S3PutCount, p.S3PutPerThousand), c.S3PutUSD)
	line(fmt.Sprintf("  S3 storage  %.2f GB gzip'd x $%.3f/GB-mo", c.S3CompressedGB, p.S3StoragePerGB), c.S3StorageUSD)
	b.WriteString(fmt.Sprintf("\n  -> rdstail saves ~%s/mo (%.0f%%)\n", usd(c.MonthlySavingsUSD), c.SavingsPercent))

	if len(r.Notes) > 0 {
		b.WriteString("\n")
		for _, n := range r.Notes {
			b.WriteString("  note: " + n + "\n")
		}
	}
	return b.String()
}

func usd(v float64) string {
	return fmt.Sprintf("$%.2f", v)
}

// humanBytes formats a byte count in binary units, one decimal.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
