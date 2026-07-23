// Package parse extracts server-side metadata — event timestamp and severity —
// from raw RDS log lines, per engine. Extraction is strictly best-effort: a
// line that doesn't match the engine's format simply yields no metadata, and
// the caller falls back to fetch-time. The raw line is never modified.
//
// This is deliberately NOT log parsing/structuring (queries, fields, params
// stay opaque); it lifts just the two attributes downstream consumers need for
// alerting ("severity=FATAL") and time-accurate correlation.
package parse

import (
	"regexp"
	"strings"
	"time"
)

// Meta is the metadata extracted from one log line. Zero-value fields mean
// "not present on this line".
type Meta struct {
	Timestamp time.Time // server-side event time (UTC); zero if not found
	Severity  string    // canonical upper-case token (ERROR, WARNING, ...); "" if not found
}

// Parser extracts Meta from a single raw log line.
type Parser interface {
	Parse(line string) Meta
}

// ForEngine returns the line parser for an engine name, or nil when the engine
// has no parser (callers must treat nil as "no metadata extraction").
func ForEngine(engine string) Parser {
	switch engine {
	case "postgres":
		return postgresParser{}
	case "mysql", "mariadb":
		return mysqlParser{}
	default:
		return nil
	}
}

// --- PostgreSQL ---
//
// RDS pins log_line_prefix to `%t:%r:%u@%d:[%p]:` (not user-changeable), so
// lines look like:
//
//	2026-07-21 10:00:00 UTC:10.0.0.1(5432):app@mydb:[12345]:ERROR:  relation "x" does not exist
//
// %r/%u/%d may be empty for system processes:
//
//	2026-07-21 10:00:00 UTC::@:[389]:LOG:  checkpoint starting: time
//
// Continuation lines (STATEMENT/DETAIL/HINT/CONTEXT) carry the same prefix, so
// they parse like any other line. %m (millisecond) timestamps are accepted too.

var pgLine = regexp.MustCompile(
	`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(\.\d+)? ([A-Z]{2,5}):.*?:\[\d+\]:([A-Z]+\d?):`)

type postgresParser struct{}

func (postgresParser) Parse(line string) Meta {
	m := pgLine.FindStringSubmatch(line)
	if m == nil {
		return Meta{}
	}
	var meta Meta
	meta.Severity = m[4]
	// Fast path for the overwhelmingly common case: RDS logs in UTC. Other
	// zone abbreviations are ambiguous; leave the timestamp unset rather than
	// guess an offset.
	if m[3] == "UTC" {
		if ts, err := time.Parse("2006-01-02 15:04:05", m[1]); err == nil {
			meta.Timestamp = ts.UTC()
		}
	}
	return meta
}

// --- MySQL / MariaDB ---
//
// MySQL 5.7/8.0 error log:
//
//	2026-07-21T10:00:00.123456Z 8 [Warning] [MY-010055] [Server] message
//
// MariaDB 10.x error log:
//
//	2026-07-21 10:00:00 0 [Note] message
//
// Slow-query log timestamp header (severity-less):
//
//	# Time: 2026-07-21T10:00:00.123456Z

var (
	mysqlLine = regexp.MustCompile(
		`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})) +\d+ +\[([A-Za-z]+)\]`)
	mariadbLine = regexp.MustCompile(
		`^(\d{4}-\d{2}-\d{2}) +(\d{1,2}:\d{2}:\d{2}) +\d+ +\[([A-Za-z]+)\]`)
	slowTimeLine = regexp.MustCompile(
		`^# Time: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))`)
)

type mysqlParser struct{}

func (mysqlParser) Parse(line string) Meta {
	if m := mysqlLine.FindStringSubmatch(line); m != nil {
		var meta Meta
		meta.Severity = strings.ToUpper(m[2])
		if ts, err := time.Parse(time.RFC3339Nano, m[1]); err == nil {
			meta.Timestamp = ts.UTC()
		}
		return meta
	}
	if m := mariadbLine.FindStringSubmatch(line); m != nil {
		var meta Meta
		meta.Severity = strings.ToUpper(m[3])
		// MariaDB logs local server time; RDS instances run UTC.
		if ts, err := time.Parse("2006-01-02 15:04:05", m[1]+" "+m[2]); err == nil {
			meta.Timestamp = ts.UTC()
		}
		return meta
	}
	if m := slowTimeLine.FindStringSubmatch(line); m != nil {
		var meta Meta
		if ts, err := time.Parse(time.RFC3339Nano, m[1]); err == nil {
			meta.Timestamp = ts.UTC()
		}
		return meta
	}
	return Meta{}
}
