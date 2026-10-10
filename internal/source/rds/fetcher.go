package rds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go"

	"github.com/avinash-gupta-rdz/rdstail/internal/parse"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// MarkerBeginning is the opaque marker value for "start at the beginning of the file".
// AWS accepts "0" in all engines tested.
const MarkerBeginning = "0"

// truncationNotice is what RDS appends when a DownloadDBLogFilePortion
// response hits its 1 MB cap mid-line. The line before it is cut short and
// the returned marker skips the rest of that line — so a truncated response
// silently loses data unless it is re-fetched with fewer lines.
const truncationNotice = "[Your log message was truncated]"

// maxRecordBytes caps how large one grouped multi-line record may grow before
// it is split, so a run of unparseable lines can't build an unbounded record.
const maxRecordBytes = 1 << 20

// maxLinesPerCall bounds NumberOfLines on truncation re-fetches; RDS's
// documented per-call default is 10,000 lines.
const maxLinesPerCall = 10000

// FileMeta describes one RDS log file discovered via DescribeDBLogFiles.
type FileMeta struct {
	Name          string
	Size          int64
	LastWrittenMS int64
}

// LastWritten returns the file's last-written timestamp in UTC.
func (f FileMeta) LastWritten() time.Time {
	if f.LastWrittenMS == 0 {
		return time.Time{}
	}
	return time.UnixMilli(f.LastWrittenMS).UTC()
}

// Chunk is one portion of log data fetched in a single DownloadDBLogFilePortion call.
type Chunk struct {
	LogFile           string
	Records           []logrecord.LogRecord
	PrevMarker        string
	NextMarker        string
	Bytes             int64
	AdditionalPending bool
	BatchID           string
	// LeadingContinuation reports that the chunk's first record began with a
	// continuation line (no timestamp/severity), i.e. it continues the last
	// record of the previous chunk.
	LeadingContinuation bool
	// TruncatedLines counts lines RDS cut at its 1 MB response cap that could
	// not be recovered by re-fetching (a single line larger than 1 MB).
	TruncatedLines int
}

// APICallObserver is called once per AWS API call with (operation, outcome).
// outcome is "ok" on success, "error" on generic failure, "throttled" on throttling.
// Implementations should be cheap and thread-safe.
type APICallObserver func(operation, outcome string)

// Fetcher pulls logs for a single RDS instance. Safe for concurrent use across
// different log files but per-file ordering is the caller's responsibility.
type Fetcher struct {
	api        RDSAPI
	instanceID string
	engine     string
	classifier LogFileClassifier
	parser     parse.Parser // nil → no metadata extraction
	clock      func() time.Time
	observe    APICallObserver
}

// FetcherOpts configure NewFetcher. Clock and Observer are optional.
type FetcherOpts struct {
	API          RDSAPI
	InstanceID   string
	Engine       string
	IncludeAudit bool // ingest MySQL/MariaDB audit-plugin files (audit/server_audit.log*)
	Clock        func() time.Time
	Observer     APICallObserver
}

// NewFetcher constructs a Fetcher. API and InstanceID are required.
func NewFetcher(opts FetcherOpts) (*Fetcher, error) {
	if opts.API == nil {
		return nil, errors.New("rds: api is required")
	}
	if opts.InstanceID == "" {
		return nil, errors.New("rds: instance id is required")
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Fetcher{
		api:        opts.API,
		instanceID: opts.InstanceID,
		engine:     opts.Engine,
		classifier: NewClassifier(opts.Engine, opts.IncludeAudit),
		parser:     parse.ForEngine(opts.Engine),
		clock:      clock,
		observe:    opts.Observer,
	}, nil
}

func (f *Fetcher) record(op string, err error) {
	if f.observe == nil {
		return
	}
	if err == nil {
		f.observe(op, "ok")
		return
	}
	if IsThrottle(err) {
		f.observe(op, "throttled")
		return
	}
	f.observe(op, "error")
}

// IsThrottle reports whether err is an AWS API rate-limit rejection — either
// the service's throttling error or the SDK's client-side "retry quota
// exceeded", which the SDK raises after repeated throttles (seen live under a
// 40-process fleet; treating it as a generic error kept those processes
// polling at full rate and starved the rest).
func IsThrottle(err error) bool {
	// The SDK returns QuotaExceededError by value.
	var qe ratelimit.QuotaExceededError
	if errors.As(err, &qe) {
		return true
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.ErrorCode() {
	case "Throttling", "ThrottlingException", "RequestLimitExceeded",
		"TooManyRequestsException", "RequestThrottled", "RequestThrottledException":
		return true
	}
	return false
}

// InstanceID returns the instance this fetcher targets (useful for logging).
func (f *Fetcher) InstanceID() string { return f.instanceID }

// GroupsMultiline reports whether records may span several lines (the engine
// has a parser), so an entry can straddle a chunk boundary.
func (f *Fetcher) GroupsMultiline() bool { return f.parser != nil }

// Engine returns the engine label.
func (f *Fetcher) Engine() string { return f.engine }

// DiscoverFiles returns eligible log files for this instance, filtered via the
// engine-specific LogFileClassifier. Paginates until AWS returns no more files.
func (f *Fetcher) DiscoverFiles(ctx context.Context) ([]FileMeta, error) {
	var out []FileMeta
	var marker *string
	for {
		in := &awsrds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: aws.String(f.instanceID),
			Marker:               marker,
		}
		if s := f.classifier.FilenameContains(); s != "" {
			in.FilenameContains = aws.String(s)
		}
		resp, err := f.api.DescribeDBLogFiles(ctx, in)
		f.record("DescribeDBLogFiles", err)
		if err != nil {
			return nil, fmt.Errorf("describe log files %q: %w", f.instanceID, err)
		}
		for _, d := range resp.DescribeDBLogFiles {
			name := aws.ToString(d.LogFileName)
			if !f.classifier.Accepts(name) {
				continue
			}
			out = append(out, FileMeta{
				Name:          name,
				Size:          aws.ToInt64(d.Size),
				LastWrittenMS: aws.ToInt64(d.LastWritten),
			})
		}
		if resp.Marker == nil || aws.ToString(resp.Marker) == "" {
			break
		}
		marker = resp.Marker
	}
	return out, nil
}

// PullPortion fetches a single chunk of log data for logFile starting at marker.
// If marker is empty it is normalised to MarkerBeginning.
//
// The returned chunk's NextMarker is the opaque cursor to persist before the
// next call; never interpret it as a byte offset.
//
// When RDS truncates the response at its 1 MB cap, the chunk is re-fetched
// from the same marker limited to the lines that arrived complete, so the cut
// line is fetched whole by the next call instead of being lost. Only a single
// line larger than 1 MB cannot be recovered; it is shipped cut, flagged
// Truncated, and counted in TruncatedLines.
func (f *Fetcher) PullPortion(ctx context.Context, logFile, marker string) (*Chunk, error) {
	if logFile == "" {
		return nil, errors.New("rds: log file is required")
	}
	actual := marker
	if actual == "" {
		actual = MarkerBeginning
	}
	raw, err := f.pullRaw(ctx, logFile, actual)
	if err != nil {
		return nil, err
	}
	records, leadingCont := f.parseRecords(logFile, raw.data)
	if raw.truncated > 0 && len(records) > 0 {
		records[len(records)-1].Truncated = true
	}
	return &Chunk{
		LogFile:             logFile,
		Records:             records,
		PrevMarker:          marker,
		NextMarker:          raw.next,
		Bytes:               int64(len(raw.data)),
		AdditionalPending:   raw.pending,
		BatchID:             BatchID(f.instanceID, logFile, marker, raw.next),
		LeadingContinuation: leadingCont,
		TruncatedLines:      raw.truncated,
	}, nil
}

type rawPortion struct {
	data      string // RDS truncation notice removed
	next      string
	pending   bool
	truncated int // lines cut at the 1 MB cap that re-fetching couldn't recover
}

// pullRaw is one DownloadDBLogFilePortion with truncation recovery: a
// response cut at the 1 MB cap is re-fetched limited to its whole lines.
func (f *Fetcher) pullRaw(ctx context.Context, logFile, marker string) (rawPortion, error) {
	var lines *int32
	var resp *awsrds.DownloadDBLogFilePortionOutput
	for attempt := 0; ; attempt++ {
		var err error
		resp, err = f.download(ctx, logFile, marker, lines)
		if err != nil {
			return rawPortion{}, fmt.Errorf("download portion %q/%q: %w", f.instanceID, logFile, err)
		}
		data := aws.ToString(resp.LogFileData)
		if !isTruncated(data) || attempt == 4 {
			break
		}
		n := completeLines(data)
		if n == 0 {
			break // the first line alone exceeds 1 MB: unrecoverable
		}
		lines = aws.Int32(int32(min(n, maxLinesPerCall)))
	}
	out := rawPortion{
		data:    aws.ToString(resp.LogFileData),
		next:    aws.ToString(resp.Marker),
		pending: aws.ToBool(resp.AdditionalDataPending),
	}
	if isTruncated(out.data) {
		out.data = stripTruncationNotice(out.data)
		out.truncated = 1
	}
	return out, nil
}

// OffsetMarker splits a "<prefix>:<byte offset>" marker (RDS MySQL and
// PostgreSQL) into its parts.
func OffsetMarker(marker string) (prefix string, off int64, ok bool) {
	i := strings.LastIndexByte(marker, ':')
	if i <= 0 {
		return "", 0, false
	}
	off, err := strconv.ParseInt(marker[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return marker[:i], off, true
}

// ErrRangeUnsupported means a byte-range read can't be done exactly (an
// unrecoverable >1 MB line, or RDS bytes that don't match marker offsets);
// callers fall back to sequential reading.
var ErrRangeUnsupported = errors.New("rds: exact byte-range read not possible")

// ReadRange returns the raw text of the lines that START in [start, end) of
// logFile, plus the offset where that text ends (the first line start >= end,
// or the end of the last complete line at EOF). With lineStart, start is known
// to be a line start; otherwise the read begins one byte early and discards
// through the first newline, so adjacent ranges tile the file exactly. end < 0
// reads to the current end of file. prefix is the marker prefix (see
// OffsetMarker).
func (f *Fetcher) ReadRange(ctx context.Context, logFile, prefix string, start, end int64, lineStart bool) (string, int64, error) {
	pos := start
	if !lineStart {
		pos = start - 1
	}
	bufStart := pos
	var buf strings.Builder
	for {
		raw, err := f.pullRaw(ctx, logFile, prefix+":"+strconv.FormatInt(pos, 10))
		if err != nil {
			return "", 0, err
		}
		_, next, ok := OffsetMarker(raw.next)
		if raw.truncated > 0 || !ok || (next != pos+int64(len(raw.data)) && (raw.pending || len(raw.data) > 0)) {
			return "", 0, ErrRangeUnsupported
		}
		buf.WriteString(raw.data)
		pos += int64(len(raw.data))
		text := buf.String()
		if end >= 0 {
			// Done once a newline at or after end-1 shows where the next range begins.
			from := max(end-1-bufStart, 0)
			if from < int64(len(text)) {
				if i := strings.IndexByte(text[from:], '\n'); i >= 0 {
					return cutRange(text, bufStart, start, lineStart, from+int64(i)+1)
				}
			}
		}
		if !raw.pending || len(raw.data) == 0 {
			// EOF: keep complete lines only.
			last := strings.LastIndexByte(text, '\n')
			return cutRange(text, bufStart, start, lineStart, int64(last+1))
		}
	}
}

// cutRange trims buf (which begins at file offset bufStart) to the owned
// lines and returns them with the absolute offset where they end.
func cutRange(text string, bufStart, start int64, lineStart bool, endIdx int64) (string, int64, error) {
	from := int64(0)
	if !lineStart {
		i := strings.IndexByte(text, '\n')
		if i < 0 {
			return "", bufStart + endIdx, nil // no line starts in range
		}
		from = int64(i + 1)
	}
	if endIdx < from {
		return "", bufStart + from, nil
	}
	return text[from:endIdx], bufStart + endIdx, nil
}

// Records parses raw log text into records exactly as PullPortion does.
func (f *Fetcher) Records(logFile, text string) []logrecord.LogRecord {
	recs, _ := f.parseRecords(logFile, text)
	return recs
}

func isTruncated(data string) bool {
	return strings.HasSuffix(strings.TrimRight(data, "\n"), truncationNotice)
}

// stripTruncationNotice removes RDS's trailing truncation notice line.
func stripTruncationNotice(data string) string {
	body := strings.TrimRight(data, "\n")
	if i := strings.LastIndexByte(body, '\n'); i >= 0 {
		return body[:i+1]
	}
	return ""
}

// completeLines counts the lines in a truncated response that arrived whole:
// everything before the cut line (which directly precedes the notice).
func completeLines(data string) int {
	body := strings.TrimSuffix(stripTruncationNotice(data), "\n")
	return strings.Count(body, "\n")
}

// SkipToEnd returns a marker at byte offset size of logFile (normally the
// file's size when rdstail first saw it), so reading resumes there. Used when
// a file is seen for the first time and the runtime start_from setting is "end".
//
// RDS MySQL and PostgreSQL markers are "<prefix>:<byte offset>", so the marker
// is built from size and confirmed with one small call instead of downloading
// the whole file (a busy hour file is hundreds of MB). Any other marker shape,
// or a built marker RDS doesn't honour, falls back to paging to the current end.
func (f *Fetcher) SkipToEnd(ctx context.Context, logFile string, size int64) (string, error) {
	one := aws.Int32(1)
	probe, err := f.download(ctx, logFile, MarkerBeginning, one)
	if err != nil {
		return "", fmt.Errorf("skip-to-end %q/%q: %w", f.instanceID, logFile, err)
	}
	marker := aws.ToString(probe.Marker)
	if !aws.ToBool(probe.AdditionalDataPending) {
		return marker, nil
	}
	if i := strings.LastIndexByte(marker, ':'); i > 0 && size > 0 {
		built := marker[:i+1] + strconv.FormatInt(size, 10)
		check, err := f.download(ctx, logFile, built, one)
		if err != nil {
			return "", fmt.Errorf("skip-to-end %q/%q: %w", f.instanceID, logFile, err)
		}
		next := aws.ToString(check.Marker)
		if j := strings.LastIndexByte(next, ':'); j == i && next[:j] == marker[:i] {
			if off, err := strconv.ParseInt(next[j+1:], 10, 64); err == nil && off >= size {
				return built, nil // honoured: reading continues from size
			}
		}
	}
	for {
		resp, err := f.download(ctx, logFile, marker, nil)
		if err != nil {
			return "", fmt.Errorf("skip-to-end %q/%q: %w", f.instanceID, logFile, err)
		}
		marker = aws.ToString(resp.Marker)
		if !aws.ToBool(resp.AdditionalDataPending) {
			return marker, nil
		}
	}
}

func (f *Fetcher) download(ctx context.Context, logFile, marker string, lines *int32) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	resp, err := f.api.DownloadDBLogFilePortion(ctx, &awsrds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(f.instanceID),
		LogFileName:          aws.String(logFile),
		Marker:               aws.String(marker),
		NumberOfLines:        lines,
	})
	f.record("DownloadDBLogFilePortion", err)
	return resp, err
}

// parseRecords splits the raw log payload into LogRecords. When the engine
// parser recognises a line (timestamp or severity), that line starts a new
// record; lines it doesn't recognise — slow-query entry bodies, multi-line SQL,
// stack traces — are continuation lines appended (newline-joined) to the
// record before them, so one logical entry ships as one record. The second
// return value reports that the chunk began with continuation lines, which
// the caller may join onto the previous chunk's last record.
//
// Records whose header carried no timestamp inherit the nearest preceding
// timestamped record's time within the chunk, else fetch time. Unknown
// engines (no parser) keep one record per line. Message is the verbatim text.
func (f *Fetcher) parseRecords(logFile, data string) ([]logrecord.LogRecord, bool) {
	if data == "" {
		return nil, false
	}
	now := f.clock().UTC()
	lines := strings.Split(data, "\n")
	out := make([]logrecord.LogRecord, 0, len(lines))
	var lastTS time.Time
	leadingCont := false
	for _, line := range lines {
		if line == "" {
			continue
		}
		var meta parse.Meta
		start := true
		if f.parser != nil {
			meta = f.parser.Parse(line)
			start = meta.Severity != "" || !meta.Timestamp.IsZero() || meta.Audit != nil
		}
		if !start {
			if len(out) == 0 {
				leadingCont = true
			} else if last := &out[len(out)-1]; len(last.Message)+1+len(line) <= maxRecordBytes {
				last.Message += "\n" + line
				continue
			}
		}
		rec := logrecord.LogRecord{
			InstanceID: f.instanceID,
			Engine:     f.engine,
			LogFile:    logFile,
			Timestamp:  now,
			Message:    line,
			Severity:   meta.Severity,
			Audit:      meta.Audit,
		}
		switch {
		case !meta.Timestamp.IsZero():
			rec.Timestamp = meta.Timestamp
			lastTS = meta.Timestamp
		case !lastTS.IsZero():
			rec.Timestamp = lastTS
		}
		out = append(out, rec)
	}
	return out, leadingCont
}

// BatchID is the deterministic batch identifier for (instance, file, prev, next).
// Exported so downstream consumers can match against the record's BatchID for dedupe.
func BatchID(instance, logFile, prevMarker, nextMarker string) string {
	h := sha256.Sum256([]byte(instance + "|" + logFile + "|" + prevMarker + "|" + nextMarker))
	return hex.EncodeToString(h[:8])
}

// JoinContinuation appends a chunk's leading continuation record onto the
// previous chunk's last record, rejoining a multi-line entry that straddled a
// chunk boundary. It reports false (and leaves prev unchanged) when the joined
// record would exceed maxRecordBytes; the caller then keeps next as its own
// record.
func JoinContinuation(prev, next logrecord.LogRecord) (logrecord.LogRecord, bool) {
	if len(prev.Message)+1+len(next.Message) > maxRecordBytes {
		return prev, false
	}
	prev.Message += "\n" + next.Message
	prev.Truncated = prev.Truncated || next.Truncated
	return prev, true
}
