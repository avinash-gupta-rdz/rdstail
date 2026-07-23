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
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// scriptedAPI is a deterministic mock RDS client. Describe responses are played
// in order per poll; download responses are keyed by (logfile, marker).
type scriptedAPI struct {
	mu                sync.Mutex
	describeResponses []*awsrds.DescribeDBLogFilesOutput
	downloadByKey     map[string]*awsrds.DownloadDBLogFilePortionOutput
	downloadCalls     []string
	describeCalls     int
}

func (s *scriptedAPI) DescribeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.describeCalls
}

func (s *scriptedAPI) DescribeDBLogFiles(_ context.Context, in *awsrds.DescribeDBLogFilesInput, _ ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = in
	s.describeCalls++
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
			"error/postgresql.log|0": {Marker: aws.String("tail-x"), AdditionalDataPending: aws.Bool(false), LogFileData: aws.String("old\n")},
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

func TestWorker_AdaptivePolling_BacksOffWhenIdle(t *testing.T) {
	// A file that never grows: every poll after the pre-seeded checkpoint is
	// idle (default download response returns an empty chunk at the same
	// marker). Fixed 5ms polling over 300ms would make ~60 describe calls;
	// adaptive backoff 5ms→80ms should stay far below that.
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne("error/postgresql.log", 1000)},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)
	if err := store.Set(context.Background(), "db-1", "error/postgresql.log",
		state.Checkpoint{Marker: "tail", FileSize: 1000}); err != nil {
		t.Fatal(err)
	}

	w, err := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink,
		PollInterval:    5 * time.Millisecond,
		PollIntervalMax: 80 * time.Millisecond,
		PollMultiplier:  2.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	calls := api.DescribeCalls()
	if calls < 3 {
		t.Fatalf("worker barely polled: %d describe calls", calls)
	}
	if calls > 25 {
		t.Fatalf("backoff ineffective: %d describe calls in 300ms (fixed 5ms would be ~60)", calls)
	}
	if sink.RecordCount() != 0 {
		t.Fatalf("idle test shipped %d records", sink.RecordCount())
	}
}

func TestWorker_CrossChunkBatching_CoalescesIntoOneWrite(t *testing.T) {
	const fname = "error/postgresql.log"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne(fname, 3000)},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			fname + "|m0": {LogFileData: aws.String("a\nb\n"), Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(true)},
			fname + "|m1": {LogFileData: aws.String("c\nd\n"), Marker: aws.String("m2"), AdditionalDataPending: aws.Bool(true)},
			fname + "|m2": {LogFileData: aws.String("e\n"), Marker: aws.String("m3"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "m0", FileSize: 1000})

	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 500 * time.Millisecond,
		MaxBatchBytes: 1 << 20, // never trips → everything coalesces
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	batches := sink.Batches()
	if len(batches) != 1 || len(batches[0]) != 5 {
		t.Fatalf("expected 1 coalesced batch of 5 records, got %d batches: %v", len(batches), batches)
	}
	// Records keep their originating chunk's BatchID for dedupe.
	if batches[0][0].BatchID == batches[0][4].BatchID {
		t.Fatal("records from different chunks must keep distinct batch ids")
	}
	got, _, _ := store.Get(context.Background(), "db-1", fname)
	if got.Marker != "m3" {
		t.Fatalf("expected final marker=m3, got %q", got.Marker)
	}
}

func TestWorker_CrossChunkBatching_RecordThresholdFlushesMidPagination(t *testing.T) {
	const fname = "error/postgresql.log"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne(fname, 3000)},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			fname + "|m0": {LogFileData: aws.String("a\nb\n"), Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(true)},
			fname + "|m1": {LogFileData: aws.String("c\nd\n"), Marker: aws.String("m2"), AdditionalDataPending: aws.Bool(true)},
			fname + "|m2": {LogFileData: aws.String("e\n"), Marker: aws.String("m3"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "m0", FileSize: 1000})

	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 500 * time.Millisecond,
		MaxBatchRecords: 2, // trips after every chunk here
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	batches := sink.Batches()
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches (2+2+1), got %d", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 1 {
		t.Fatalf("unexpected batch sizes: %d/%d/%d", len(batches[0]), len(batches[1]), len(batches[2]))
	}
	if sink.RecordCount() != 5 {
		t.Fatalf("expected 5 records total, got %d", sink.RecordCount())
	}
}

func TestWorker_CrossChunkBatching_MidBatchSinkFailureKeepsLastFlushedCheckpoint(t *testing.T) {
	const fname = "error/postgresql.log"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{describeOne(fname, 3000)},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			fname + "|m0": {LogFileData: aws.String("a\nb\n"), Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(true)},
			fname + "|m1": {LogFileData: aws.String("c\n"), Marker: aws.String("m2"), AdditionalDataPending: aws.Bool(false)},
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "m0", FileSize: 1000})

	// First flush (records threshold) succeeds, every later write fails: the
	// final flush must NOT advance the checkpoint past the last ACK.
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: &failAfterSink{inner: sink, allow: 1}, PollInterval: time.Hour,
		MaxBatchRecords: 2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	if sink.RecordCount() != 2 {
		t.Fatalf("expected only the first flush (a,b) delivered, got %d records", sink.RecordCount())
	}
	got, _, _ := store.Get(context.Background(), "db-1", fname)
	if got.Marker != "m1" {
		t.Fatalf("checkpoint must stop at last ACKed flush (m1), got %q", got.Marker)
	}
}

// failAfterSink allows the first `allow` writes through, then fails everything.
type failAfterSink struct {
	inner  *memory.Sink
	allow  int
	writes int
}

func (f *failAfterSink) Name() string { return f.inner.Name() }
func (f *failAfterSink) Type() string { return f.inner.Type() }
func (f *failAfterSink) Close() error { return f.inner.Close() }
func (f *failAfterSink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	f.writes++
	if f.writes > f.allow {
		return memory.ErrForced
	}
	return f.inner.Write(ctx, records)
}

func TestWorker_ParallelFileDrain_AllFilesCheckpointed(t *testing.T) {
	// Three files with pending data; a shared 2-slot drain semaphore forces
	// bounded concurrency. All files must deliver and checkpoint.
	files := []string{"error/mysql-error.log", "slowquery/mysql-slowquery.log", "general/mysql-general.log"}
	details := make([]rdstypes.DescribeDBLogFilesDetails, len(files))
	downloads := map[string]*awsrds.DownloadDBLogFilePortionOutput{}
	for i, f := range files {
		details[i] = rdstypes.DescribeDBLogFilesDetails{
			LogFileName: aws.String(f), Size: aws.Int64(100), LastWritten: aws.Int64(1),
		}
		downloads[f+"|m0"] = &awsrds.DownloadDBLogFilePortionOutput{
			LogFileData: aws.String("x\ny\n"), Marker: aws.String("m1"), AdditionalDataPending: aws.Bool(false),
		}
	}
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{{DescribeDBLogFiles: details}},
		downloadByKey:     downloads,
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	sink := memory.New("mem")
	store := newFileStore(t)
	for _, f := range files {
		_ = store.Set(context.Background(), "db-1", f, state.Checkpoint{Marker: "m0", FileSize: 50})
	}

	w, err := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: time.Hour,
		DrainSem: make(chan struct{}, 2),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	if sink.RecordCount() != 6 {
		t.Fatalf("expected 6 records (2 per file), got %d", sink.RecordCount())
	}
	for _, f := range files {
		got, _, _ := store.Get(context.Background(), "db-1", f)
		if got.Marker != "m1" {
			t.Fatalf("file %s not checkpointed: %+v", f, got)
		}
	}
}
