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
	"encoding/csv"
	"regexp"
	"strings"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// Meta is the metadata extracted from one log line. Zero-value fields mean
// "not present on this line".
type Meta struct {
	Timestamp time.Time        // server-side event time (UTC); zero if not found
	Severity  string           // canonical upper-case token (ERROR, WARNING, ...); "" if not found
	Audit     *logrecord.Audit // pgAudit fields when the line is an AUDIT: entry; nil otherwise
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
	loc := pgLine.FindStringSubmatchIndex(line)
	if loc == nil {
		return Meta{}
	}
	group := func(i int) string {
		if loc[2*i] < 0 {
			return ""
		}
		return line[loc[2*i]:loc[2*i+1]]
	}
	var meta Meta
	meta.Severity = group(4)
	// Fast path for the overwhelmingly common case: RDS logs in UTC. Other
	// zone abbreviations are ambiguous; leave the timestamp unset rather than
	// guess an offset.
	if group(3) == "UTC" {
		if ts, err := time.Parse("2006-01-02 15:04:05", group(1)); err == nil {
			meta.Timestamp = ts.UTC()
		}
	}
	// pgAudit entries put a CSV payload right after the prefix:
	//   ...:[123]:LOG:  AUDIT: SESSION,1,1,READ,SELECT,TABLE,public.accounts,SELECT ...,<not logged>
	// Anchoring on the prefix end (not a substring search) keeps statements
	// that merely contain "AUDIT: " from being misread.
	rest := strings.TrimLeft(line[loc[1]:], " ")
	if body, ok := strings.CutPrefix(rest, "AUDIT: "); ok {
		meta.Audit = parsePGAudit(body)
	}
	return meta
}

// parsePGAudit lifts the classification fields from a pgAudit CSV payload:
// AUDIT_TYPE,STATEMENT_ID,SUBSTATEMENT_ID,CLASS,COMMAND,OBJECT_TYPE,OBJECT_NAME,STATEMENT,PARAMETER.
// The statement itself is deliberately not extracted — it stays verbatim in
// Message. Returns nil when the payload doesn't parse as pgAudit CSV.
func parsePGAudit(body string) *logrecord.Audit {
	r := csv.NewReader(strings.NewReader(body))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	fields, err := r.Read()
	if err != nil || len(fields) < 5 {
		return nil
	}
	a := &logrecord.Audit{
		Type:    fields[0],
		Class:   fields[3],
		Command: fields[4],
	}
	if a.Type != "SESSION" && a.Type != "OBJECT" {
		return nil
	}
	if len(fields) >= 7 {
		a.ObjectType = fields[5]
		a.ObjectName = fields[6]
	}
	return a
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
//
// General query log (severity-less, tab-separated):
//
//	2026-07-21T10:00:00.123456Z	    7 Query	SELECT 1

var (
	mysqlLine = regexp.MustCompile(
		`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})) +\d+ +\[([A-Za-z]+)\]`)
	mariadbLine = regexp.MustCompile(
		`^(\d{4}-\d{2}-\d{2}) +(\d{1,2}:\d{2}:\d{2}) +\d+ +\[([A-Za-z]+)\]`)
	slowTimeLine = regexp.MustCompile(
		`^# Time: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))`)
	// General query log — tab-separated, severity-less:
	//   2026-10-10T06:10:05.902340Z\t    7 Query\tSELECT 1
	generalLine = regexp.MustCompile(
		`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))\t +\d+ [A-Z]`)
	// MariaDB audit plugin (server_audit.log, ingested behind include_audit):
	//   20260729 06:00:00,ip-10-0-0-1,app,10.0.0.9,64,1234,QUERY,mydb,'SELECT 1',0
	// Only the timestamp is lifted; the CSV payload stays verbatim in Message.
	serverAuditLine = regexp.MustCompile(`^(\d{8}) +(\d{2}:\d{2}:\d{2}),`)
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
	if m := generalLine.FindStringSubmatch(line); m != nil {
		var meta Meta
		if ts, err := time.Parse(time.RFC3339Nano, m[1]); err == nil {
			meta.Timestamp = ts.UTC()
		}
		return meta
	}
	if m := serverAuditLine.FindStringSubmatch(line); m != nil {
		var meta Meta
		// server_audit logs server-local time; RDS instances run UTC.
		if ts, err := time.Parse("20060102 15:04:05", m[1]+" "+m[2]); err == nil {
			meta.Timestamp = ts.UTC()
		}
		return meta
	}
	return Meta{}
}
