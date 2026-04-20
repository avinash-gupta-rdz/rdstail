package rds_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

// mockAPI is a hand-rolled stub for RDSAPI. Each call records its input and
// returns the next programmed response. Concurrent-safe enough for these tests
// (no overlap).
type mockAPI struct {
	describeResponses []*awsrds.DescribeDBLogFilesOutput
	describeCalls     []*awsrds.DescribeDBLogFilesInput
	describeErr       error

	// keyed by "logfile|marker" -> response
	downloadResponses map[string]*awsrds.DownloadDBLogFilePortionOutput
	downloadCalls     []*awsrds.DownloadDBLogFilePortionInput
	downloadErr       error
}

func (m *mockAPI) DescribeDBLogFiles(ctx context.Context, in *awsrds.DescribeDBLogFilesInput, _ ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	m.describeCalls = append(m.describeCalls, in)
	if m.describeErr != nil {
		return nil, m.describeErr
	}
	if len(m.describeResponses) == 0 {
		return &awsrds.DescribeDBLogFilesOutput{}, nil
	}
	out := m.describeResponses[0]
	m.describeResponses = m.describeResponses[1:]
	return out, nil
}

func (m *mockAPI) DownloadDBLogFilePortion(ctx context.Context, in *awsrds.DownloadDBLogFilePortionInput, _ ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	m.downloadCalls = append(m.downloadCalls, in)
	if m.downloadErr != nil {
		return nil, m.downloadErr
	}
	key := fmt.Sprintf("%s|%s", aws.ToString(in.LogFileName), aws.ToString(in.Marker))
	if out, ok := m.downloadResponses[key]; ok {
		return out, nil
	}
	return &awsrds.DownloadDBLogFilePortionOutput{Marker: in.Marker}, nil
}

func newFixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestDiscoverFiles_FiltersByClassifier(t *testing.T) {
	m := &mockAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{
			{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
				{LogFileName: aws.String("error/postgresql.log.2025-01-01"), Size: aws.Int64(100), LastWritten: aws.Int64(1_700_000_000_000)},
				{LogFileName: aws.String("error/mysql-error.log"), Size: aws.Int64(50), LastWritten: aws.Int64(1_700_000_001_000)},
				{LogFileName: aws.String("audit/server_audit.log"), Size: aws.Int64(10), LastWritten: aws.Int64(1_700_000_002_000)},
			}},
		},
	}
	f, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db-1", Engine: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	files, err := f.DiscoverFiles(context.Background())
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(files) != 1 || files[0].Name != "error/postgresql.log.2025-01-01" {
		t.Fatalf("expected only postgres file, got %+v", files)
	}
	if files[0].Size != 100 || files[0].LastWrittenMS != 1_700_000_000_000 {
		t.Fatalf("metadata mismatch: %+v", files[0])
	}
}

func TestDiscoverFiles_Paginates(t *testing.T) {
	m := &mockAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{
			{
				DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
					{LogFileName: aws.String("error/postgresql.log.A"), Size: aws.Int64(1)},
				},
				Marker: aws.String("page2"),
			},
			{
				DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
					{LogFileName: aws.String("error/postgresql.log.B"), Size: aws.Int64(2)},
				},
			},
		},
	}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db-1", Engine: "postgres"})
	files, err := f.DiscoverFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
	if len(m.describeCalls) != 2 {
		t.Fatalf("expected 2 describe calls, got %d", len(m.describeCalls))
	}
	if aws.ToString(m.describeCalls[1].Marker) != "page2" {
		t.Fatalf("expected pagination marker=page2, got %q", aws.ToString(m.describeCalls[1].Marker))
	}
}

func TestPullPortion_ParsesLinesAndAssignsBatchID(t *testing.T) {
	fixed := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	m := &mockAPI{
		downloadResponses: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			"error/postgresql.log|0": {
				LogFileData:           aws.String("line 1\nline 2\n\nline 3\n"),
				Marker:                aws.String("100"),
				AdditionalDataPending: aws.Bool(true),
			},
		},
	}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{
		API: m, InstanceID: "db-1", Engine: "postgres", Clock: newFixedClock(fixed),
	})

	chunk, err := f.PullPortion(context.Background(), "error/postgresql.log", "")
	if err != nil {
		t.Fatal(err)
	}
	if chunk == nil {
		t.Fatal("nil chunk")
	}
	if len(chunk.Records) != 3 {
		t.Fatalf("expected 3 non-empty records, got %d", len(chunk.Records))
	}
	if chunk.Records[0].Message != "line 1" || chunk.Records[0].Timestamp != fixed {
		t.Fatalf("first record wrong: %+v", chunk.Records[0])
	}
	if chunk.NextMarker != "100" || !chunk.AdditionalPending {
		t.Fatalf("marker/pending wrong: %+v", chunk)
	}
	want := rdssrc.BatchID("db-1", "error/postgresql.log", "", "100")
	if chunk.BatchID != want {
		t.Fatalf("batch id mismatch: got %s want %s", chunk.BatchID, want)
	}
	if chunk.Bytes != int64(len("line 1\nline 2\n\nline 3\n")) {
		t.Fatalf("bytes mismatch: %d", chunk.Bytes)
	}
}

func TestPullPortion_EmptyDataYieldsNoRecords(t *testing.T) {
	m := &mockAPI{
		downloadResponses: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			"f|m": {LogFileData: aws.String(""), Marker: aws.String("m")},
		},
	}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db", Engine: "mysql"})
	chunk, err := f.PullPortion(context.Background(), "f", "m")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(chunk.Records))
	}
}

func TestPullPortion_EmptyMarkerNormalisedToZero(t *testing.T) {
	m := &mockAPI{}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db", Engine: "postgres"})
	_, _ = f.PullPortion(context.Background(), "f", "")
	if len(m.downloadCalls) != 1 {
		t.Fatalf("expected 1 download call, got %d", len(m.downloadCalls))
	}
	if got := aws.ToString(m.downloadCalls[0].Marker); got != "0" {
		t.Fatalf("expected marker=0 after normalisation, got %q", got)
	}
}

func TestSkipToEnd_LoopsUntilNoPending(t *testing.T) {
	m := &mockAPI{
		downloadResponses: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			"f|0":   {Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(true)},
			"f|m1":  {Marker: aws.String("m2"), AdditionalDataPending: aws.Bool(true)},
			"f|m2":  {Marker: aws.String("tail"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db", Engine: "postgres"})
	tail, err := f.SkipToEnd(context.Background(), "f")
	if err != nil {
		t.Fatal(err)
	}
	if tail != "tail" {
		t.Fatalf("expected tail=tail, got %q", tail)
	}
	if len(m.downloadCalls) != 3 {
		t.Fatalf("expected 3 download calls, got %d", len(m.downloadCalls))
	}
}

func TestPullPortion_PropagatesAPIErr(t *testing.T) {
	m := &mockAPI{downloadErr: errors.New("throttled")}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db", Engine: "postgres"})
	_, err := f.PullPortion(context.Background(), "f", "0")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBatchID_IsDeterministicAndShort(t *testing.T) {
	a := rdssrc.BatchID("i", "f", "p", "n")
	b := rdssrc.BatchID("i", "f", "p", "n")
	if a != b {
		t.Fatal("non-deterministic batch ID")
	}
	if a == rdssrc.BatchID("i", "f", "p", "n2") {
		t.Fatal("batch ID should differ when next marker changes")
	}
	if len(a) != 16 {
		t.Fatalf("expected 16-char hex (8 bytes), got %d: %s", len(a), a)
	}
}

func TestNewFetcher_ValidatesInputs(t *testing.T) {
	if _, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{InstanceID: "x"}); err == nil {
		t.Fatal("expected err for missing API")
	}
	if _, err := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: &mockAPI{}}); err == nil {
		t.Fatal("expected err for missing InstanceID")
	}
}
