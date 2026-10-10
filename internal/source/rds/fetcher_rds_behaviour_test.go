package rds_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go"

	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

// truncatingAPI mimics DownloadDBLogFilePortion's 1 MB cap as observed on RDS
// MySQL 8.4: when the requested lines exceed the cap, the last line is cut,
// " [Your log message was truncated]" is appended, and the marker skips past
// the whole cut line. NumberOfLines limits the response to that many lines.
type truncatingAPI struct {
	lines []string // file content, one entry per line (no newline)
	cap   int
	calls []*awsrds.DownloadDBLogFilePortionInput
}

func (a *truncatingAPI) DescribeDBLogFiles(context.Context, *awsrds.DescribeDBLogFilesInput, ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	return &awsrds.DescribeDBLogFilesOutput{}, nil
}

func (a *truncatingAPI) DownloadDBLogFilePortion(_ context.Context, in *awsrds.DownloadDBLogFilePortionInput, _ ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	a.calls = append(a.calls, in)
	start := 0
	if m := aws.ToString(in.Marker); m != "0" {
		start = int(m[0] - 'a') // markers "a", "b", ...: line index
	}
	limit := len(a.lines) - start
	if in.NumberOfLines != nil && int(*in.NumberOfLines) < limit {
		limit = int(*in.NumberOfLines)
	}
	var b strings.Builder
	end := start
	for end < start+limit {
		line := a.lines[end] + "\n"
		if b.Len()+len(line) > a.cap {
			b.WriteString(line[:a.cap-b.Len()])
			b.WriteString("\n [Your log message was truncated]\n")
			end++ // marker skips the rest of the cut line
			break
		}
		b.WriteString(line)
		end++
	}
	return &awsrds.DownloadDBLogFilePortionOutput{
		LogFileData:           aws.String(b.String()),
		Marker:                aws.String(string(rune('a' + end))),
		AdditionalDataPending: aws.Bool(end < len(a.lines)),
	}, nil
}

func TestPullPortion_TruncatedResponseRefetchedWithoutLoss(t *testing.T) {
	long := "2026-10-10T06:30:27.262138Z\t   36 Prepare\tINSERT INTO `iocs` VALUES " + strings.Repeat("(?,?),", 40)
	api := &truncatingAPI{cap: 400, lines: []string{
		"2026-10-10T06:30:27.1Z\t   36 Query\tSELECT 1",
		"2026-10-10T06:30:27.2Z\t   36 Query\tSELECT 2",
		long,
		"2026-10-10T06:30:27.3Z\t   36 Query\tSELECT 3",
	}}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	var got []string
	marker := "0"
	for {
		c, err := f.PullPortion(context.Background(), "general/mysql-general.log", marker)
		if err != nil {
			t.Fatal(err)
		}
		if c.TruncatedLines != 0 {
			t.Fatalf("recoverable truncation reported as lost: %+v", c)
		}
		for _, r := range c.Records {
			got = append(got, r.Message)
		}
		marker = c.NextMarker
		if !c.AdditionalPending {
			break
		}
	}
	if strings.Join(got, "\n") != strings.Join(api.lines, "\n") {
		t.Fatalf("content mismatch:\n got %q\nwant %q", got, api.lines)
	}
}

func TestPullPortion_SingleLineOverCap_FlaggedTruncated(t *testing.T) {
	api := &truncatingAPI{cap: 100, lines: []string{
		"2026-10-10T06:30:27.1Z\t   36 Query\tSELECT '" + strings.Repeat("x", 500) + "'",
	}}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	c, err := f.PullPortion(context.Background(), "general/mysql-general.log", "0")
	if err != nil {
		t.Fatal(err)
	}
	if c.TruncatedLines != 1 || len(c.Records) != 1 || !c.Records[0].Truncated {
		t.Fatalf("expected one truncated record, got %+v", c)
	}
	if strings.Contains(c.Records[0].Message, "Your log message was truncated") {
		t.Fatalf("RDS notice leaked into Message: %q", c.Records[0].Message)
	}
}

// Real slow-query entry captured from RDS MySQL 8.4: one entry, one record.
func TestPullPortion_SlowQueryEntryIsOneRecord(t *testing.T) {
	data := "# Time: 2026-10-10T06:07:15.377745Z\n" +
		"# User@Host: rdsadmin[rdsadmin] @ localhost []  Id:    10\n" +
		"# Query_time: 0.165977  Lock_time: 0.001766 Rows_sent: 1  Rows_examined: 1\n" +
		"SET timestamp=1791612435;\n" +
		"SELECT value FROM mysql.rds_heartbeat2;\n" +
		"# Time: 2026-10-10T06:07:30.009521Z\n" +
		"# User@Host: rdsadmin[rdsadmin] @ localhost []  Id:    10\n" +
		"# Query_time: 0.001181  Lock_time: 0.000005 Rows_sent: 1  Rows_examined: 1\n" +
		"SET timestamp=1791612450;\n" +
		"SELECT value FROM mysql.rds_heartbeat2;\n"
	m := &mockAPI{downloadResponses: map[string]*awsrds.DownloadDBLogFilePortionOutput{
		"slowquery/mysql-slowquery.log|0": {LogFileData: aws.String(data), Marker: aws.String("m")},
	}}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db-1", Engine: "mysql"})
	c, err := f.PullPortion(context.Background(), "slowquery/mysql-slowquery.log", "0")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Records) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(c.Records), c.Records)
	}
	r := c.Records[0]
	if !strings.Contains(r.Message, "Query_time: 0.165977") || !strings.HasSuffix(r.Message, "SELECT value FROM mysql.rds_heartbeat2;") {
		t.Fatalf("entry not grouped: %q", r.Message)
	}
	if r.Timestamp.Format("15:04:05.000000") != "06:07:15.377745" {
		t.Fatalf("timestamp %v", r.Timestamp)
	}
}

// Real general-log lines captured from RDS MySQL 8.4 (tab-separated).
func TestPullPortion_GeneralLogTimestampsParsed(t *testing.T) {
	data := "2026-10-10T06:10:05.902340Z\t    7 Prepare\tSELECT * FROM `honey_tokens` WHERE (mine_type = ?)\n" +
		"2026-10-10T06:10:06.000001Z\t    7 Quit\t\n"
	m := &mockAPI{downloadResponses: map[string]*awsrds.DownloadDBLogFilePortionOutput{
		"general/mysql-general.log|0": {LogFileData: aws.String(data), Marker: aws.String("m")},
	}}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db-1", Engine: "mysql"})
	c, _ := f.PullPortion(context.Background(), "general/mysql-general.log", "0")
	if len(c.Records) != 2 || c.Records[0].Timestamp.Format("15:04:05.000000") != "06:10:05.902340" ||
		c.Records[1].Timestamp.Format("15:04:05.000000") != "06:10:06.000001" {
		t.Fatalf("general-log timestamps not parsed: %+v", c.Records)
	}
}

func TestIsThrottle(t *testing.T) {
	if !rdssrc.IsThrottle(&smithy.GenericAPIError{Code: "Throttling", Message: "Rate exceeded"}) {
		t.Fatal("Throttling not classified")
	}
	if !rdssrc.IsThrottle(fmt.Errorf("failed to get rate limit token, %w", ratelimit.QuotaExceededError{Available: 3, Requested: 5})) {
		t.Fatal("SDK retry-quota exhaustion not classified as throttle")
	}
	if rdssrc.IsThrottle(&smithy.GenericAPIError{Code: "DBInstanceNotFound"}) {
		t.Fatal("non-throttle classified as throttle")
	}
}

func TestParseHourMarkerAndSplitRotated(t *testing.T) {
	id, off, ok := rdssrc.ParseHourMarker("2026-10-10.7:185849")
	if !ok || id.String() != "2026-10-10.7" || off != 185849 {
		t.Fatalf("got %v %v %v", id, off, ok)
	}
	base, rid, ok := rdssrc.SplitRotated("error/mysql-error-running.log.2026-09-28.17")
	if !ok || base != "error/mysql-error-running.log" || rid.String() != "2026-09-28.17" {
		t.Fatalf("got %q %v %v", base, rid, ok)
	}
	if _, _, ok := rdssrc.SplitRotated("error/postgresql.log.2026-10-10-07"); ok {
		t.Fatal("postgres hourly file misread as MySQL rotation")
	}
	if rid.Compare(id) >= 0 {
		t.Fatal("hour ordering wrong")
	}
}
