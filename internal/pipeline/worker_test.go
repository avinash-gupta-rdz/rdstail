package pipeline_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	filestore "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
)

// scriptedAPI is a deterministic mock RDS client. Describe responses are played
// in order per poll; download responses are keyed by (logfile, marker).
type scriptedAPI struct {
	mu                sync.Mutex
	describeResponses []*awsrds.DescribeDBLogFilesOutput
	downloadByKey     map[string]*awsrds.DownloadDBLogFilePortionOutput
	downloadCalls     []string
}

func (s *scriptedAPI) DescribeDBLogFiles(_ context.Context, in *awsrds.DescribeDBLogFilesInput, _ ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = in
	if len(s.describeResponses) == 0 {
		return &awsrds.DescribeDBLogFilesOutput{}, nil
	}
	out := s.describeResponses[0]
	if len(s.describeResponses) > 1 {
		s.describeResponses = s.describeResponses[1:]
	}
	return out, nil
}

func (s *scriptedAPI) DownloadDBLogFilePortion(_ context.Context, in *awsrds.DownloadDBLogFilePortionInput, _ ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s|%s", aws.ToString(in.LogFileName), aws.ToString(in.Marker))
	s.downloadCalls = append(s.downloadCalls, key)
	if out, ok := s.downloadByKey[key]; ok {
		return out, nil
	}
	// Default: empty chunk at the same marker, no more data.
	return &awsrds.DownloadDBLogFilePortionOutput{Marker: in.Marker, AdditionalDataPending: aws.Bool(false)}, nil
}

func newFileStore(t *testing.T) state.StateStore {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.json")
	s, err := filestore.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func describeOne(name string, size int64) *awsrds.DescribeDBLogFilesOutput {
	return &awsrds.DescribeDBLogFilesOutput{
		DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
			{LogFileName: aws.String(name), Size: aws.Int64(size), LastWritten: aws.Int64(1)},
		},
	}
}

func TestWorker_NewFile_StartFromEnd_SkipsAndCheckpoints(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne("error/postgresql.log", 1000)},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			"error/postgresql.log|0":  {Marker: aws.String("tail-x"), AdditionalDataPending: aws.Bool(false), LogFileData: aws.String("old\n")},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)

	w, err := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 10 * time.Millisecond,
		StartFrom: config.StartFromEnd,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	// New file + start_from=end → records discarded; checkpoint is "tail-x"; sink empty.
	if sink.RecordCount() != 0 {
		t.Fatalf("expected 0 records (skipped to end), got %d", sink.RecordCount())
	}
	got, ok, _ := store.Get(context.Background(), "db-1", "error/postgresql.log")
	if !ok || got.Marker != "tail-x" {
		t.Fatalf("expected checkpoint marker=tail-x, got %+v ok=%v", got, ok)
	}
}

func TestWorker_ExistingCheckpoint_PaginatesAndDelivers(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne("error/postgresql.log", 2000)},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			"error/postgresql.log|m0": {LogFileData: aws.String("a\nb\n"), Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(true)},
			"error/postgresql.log|m1": {LogFileData: aws.String("c\n"), Marker: aws.String("m2"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)
	// Pre-seed checkpoint so worker skips the new-file path.
	if err := store.Set(context.Background(), "db-1", "error/postgresql.log",
		state.Checkpoint{Marker: "m0", FileSize: 1000}); err != nil {
		t.Fatal(err)
	}
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	if sink.RecordCount() != 3 {
		t.Fatalf("expected 3 records (a,b,c), got %d", sink.RecordCount())
	}
	got, _, _ := store.Get(context.Background(), "db-1", "error/postgresql.log")
	if got.Marker != "m2" {
		t.Fatalf("expected final marker=m2, got %q", got.Marker)
	}
	// Verify batch-id stamping and marker on records.
	batches := sink.Batches()
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if batches[0][0].Marker != "m1" {
		t.Fatalf("batch 0 marker wrong: %q", batches[0][0].Marker)
	}
	if batches[0][0].BatchID == "" {
		t.Fatal("batch id missing")
	}
}

func TestWorker_TruncationResetsMarker(t *testing.T) {
	const fname = "error/postgresql.log"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne(fname, 100)}, // size=100 < prev=5000
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			fname + "|0": {LogFileData: aws.String("fresh\n"), Marker: aws.String("mNew"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "oldBig", FileSize: 5000})

	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	if sink.RecordCount() != 1 || sink.Batches()[0][0].Message != "fresh" {
		t.Fatalf("expected single 'fresh' record, got %+v; downloadCalls=%v", sink.Batches(), api.downloadCalls)
	}
	got, _, _ := store.Get(context.Background(), "db-1", fname)
	if got.Marker != "mNew" || got.FileSize != 100 {
		t.Fatalf("unexpected checkpoint after truncation: %+v", got)
	}
}

func TestWorker_SinkFailureLeavesCheckpointUnchanged(t *testing.T) {
	const fname = "error/postgresql.log"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne(fname, 100)},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			fname + "|m0": {LogFileData: aws.String("x\n"), Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	sink.FailNext(memory.ErrForced)
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "m0", FileSize: 50})

	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	got, _, _ := store.Get(context.Background(), "db-1", fname)
	if got.Marker != "m0" {
		t.Fatalf("sink failed → checkpoint must remain m0, got %q", got.Marker)
	}
}
