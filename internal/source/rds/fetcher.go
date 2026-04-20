package rds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// MarkerBeginning is the opaque marker value for "start at the beginning of the file".
// AWS accepts "0" in all engines tested.
const MarkerBeginning = "0"

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
	clock      func() time.Time
	observe    APICallObserver
}

// FetcherOpts configure NewFetcher. Clock and Observer are optional.
type FetcherOpts struct {
	API        RDSAPI
	InstanceID string
	Engine     string
	Clock      func() time.Time
	Observer   APICallObserver
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
		classifier: NewClassifier(opts.Engine),
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
	// The SDK v2 doesn't expose stable error-type tags here; classifying as
	// "throttled" requires inspecting error codes, which we defer until we have
	// signal demand. For now "error" captures everything non-ok.
	f.observe(op, "error")
}

// InstanceID returns the instance this fetcher targets (useful for logging).
func (f *Fetcher) InstanceID() string { return f.instanceID }

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
func (f *Fetcher) PullPortion(ctx context.Context, logFile, marker string) (*Chunk, error) {
	if logFile == "" {
		return nil, errors.New("rds: log file is required")
	}
	actual := marker
	if actual == "" {
		actual = MarkerBeginning
	}
	in := &awsrds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(f.instanceID),
		LogFileName:          aws.String(logFile),
		Marker:               aws.String(actual),
	}
	resp, err := f.api.DownloadDBLogFilePortion(ctx, in)
	f.record("DownloadDBLogFilePortion", err)
	if err != nil {
		return nil, fmt.Errorf("download portion %q/%q: %w", f.instanceID, logFile, err)
	}
	data := aws.ToString(resp.LogFileData)
	nextMarker := aws.ToString(resp.Marker)
	pending := aws.ToBool(resp.AdditionalDataPending)

	records := f.parseRecords(logFile, data)
	return &Chunk{
		LogFile:           logFile,
		Records:           records,
		PrevMarker:        marker,
		NextMarker:        nextMarker,
		Bytes:             int64(len(data)),
		AdditionalPending: pending,
		BatchID:           BatchID(f.instanceID, logFile, marker, nextMarker),
	}, nil
}

// SkipToEnd pulls and discards data until the tail is reached, returning the
// tail marker. Used when a file is seen for the first time and the runtime
// start_from setting is "end".
func (f *Fetcher) SkipToEnd(ctx context.Context, logFile string) (string, error) {
	marker := MarkerBeginning
	for {
		in := &awsrds.DownloadDBLogFilePortionInput{
			DBInstanceIdentifier: aws.String(f.instanceID),
			LogFileName:          aws.String(logFile),
			Marker:               aws.String(marker),
		}
		resp, err := f.api.DownloadDBLogFilePortion(ctx, in)
		f.record("DownloadDBLogFilePortion", err)
		if err != nil {
			return "", fmt.Errorf("skip-to-end %q/%q: %w", f.instanceID, logFile, err)
		}
		marker = aws.ToString(resp.Marker)
		if !aws.ToBool(resp.AdditionalDataPending) {
			return marker, nil
		}
	}
}

// parseRecords splits the raw log payload on newlines and wraps each non-empty
// line as a LogRecord. Timestamp is the fetch time — RDS returns raw log lines
// whose internal timestamps vary by engine; per PRD we do not parse them.
func (f *Fetcher) parseRecords(logFile, data string) []logrecord.LogRecord {
	if data == "" {
		return nil
	}
	now := f.clock().UTC()
	lines := strings.Split(data, "\n")
	out := make([]logrecord.LogRecord, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		out = append(out, logrecord.LogRecord{
			InstanceID: f.instanceID,
			Engine:     f.engine,
			LogFile:    logFile,
			Timestamp:  now,
			Message:    line,
		})
	}
	return out
}

// BatchID is the deterministic batch identifier for (instance, file, prev, next).
// Exported so downstream consumers can match against the record's BatchID for dedupe.
func BatchID(instance, logFile, prevMarker, nextMarker string) string {
	h := sha256.Sum256([]byte(instance + "|" + logFile + "|" + prevMarker + "|" + nextMarker))
	return hex.EncodeToString(h[:8])
}
