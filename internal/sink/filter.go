package sink

import (
	"context"
	"strings"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// severityRank orders engine severity tokens for min_severity comparisons.
// Postgres and MySQL/MariaDB tokens share one scale. Unknown/empty severities
// rank 0 (unrankable) and never pass a min_severity filter.
var severityRank = map[string]int{
	"DEBUG5": 10, "DEBUG4": 10, "DEBUG3": 10, "DEBUG2": 10, "DEBUG1": 10, "DEBUG": 10,
	"LOG": 20, "INFO": 20, "NOTICE": 20, "NOTE": 20, "SYSTEM": 20,
	"WARNING": 30, "WARN": 30,
	"ERROR": 40,
	"FATAL": 50,
	"PANIC": 60,
}

// SeverityRank returns the ordering rank for a severity token (case
// insensitive), or 0 when the token is unknown or empty. Exported so config
// validation can reject unrankable min_severity values.
func SeverityRank(sev string) int {
	return severityRank[strings.ToUpper(sev)]
}

// FilterConfig selects which records a sink receives. Zero value = no
// filtering. Set at most one of MinSeverity / Severities.
type FilterConfig struct {
	// MinSeverity keeps records whose severity ranks at or above this token
	// (e.g. "ERROR" keeps ERROR, FATAL, PANIC). Records with no or unrankable
	// severity (continuation lines, unparsed formats) are dropped — a
	// min_severity sink is an alert route, not an archive.
	MinSeverity string
	// Severities keeps records whose severity exactly matches one of these
	// tokens (case insensitive).
	Severities []string
}

func (c FilterConfig) enabled() bool { return c.MinSeverity != "" || len(c.Severities) > 0 }

type filterDecorator struct {
	inner Sink
	min   int
	exact map[string]bool // nil when MinSeverity mode
}

// WithFilter wraps s so only records passing cfg reach it. Returns s unchanged
// when cfg is empty. Filtering happens before any retry/metrics/DLQ decorators
// on s, and a fully-filtered batch ACKs immediately — the pipeline's
// checkpoint advances as if the sink accepted it.
func WithFilter(s Sink, cfg FilterConfig) Sink {
	if !cfg.enabled() {
		return s
	}
	d := &filterDecorator{inner: s}
	if cfg.MinSeverity != "" {
		d.min = SeverityRank(cfg.MinSeverity)
	} else {
		d.exact = make(map[string]bool, len(cfg.Severities))
		for _, sev := range cfg.Severities {
			d.exact[strings.ToUpper(sev)] = true
		}
	}
	return d
}

func (d *filterDecorator) Name() string { return d.inner.Name() }
func (d *filterDecorator) Type() string { return d.inner.Type() }
func (d *filterDecorator) Close() error { return d.inner.Close() }

func (d *filterDecorator) keep(r *logrecord.LogRecord) bool {
	if d.exact != nil {
		return d.exact[strings.ToUpper(r.Severity)]
	}
	rank := SeverityRank(r.Severity)
	return rank > 0 && rank >= d.min
}

func (d *filterDecorator) Write(ctx context.Context, records []logrecord.LogRecord) error {
	var kept []logrecord.LogRecord
	for i := range records {
		if d.keep(&records[i]) {
			kept = append(kept, records[i])
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return d.inner.Write(ctx, kept)
}
