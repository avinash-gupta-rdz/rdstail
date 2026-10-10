package pipeline_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// offsetAPI serves one log file whose content can be swapped mid-test, with
// RDS-style "11:<byte offset>" markers — modelling what a reboot or Multi-AZ
// failover does to a log file (observed live): the last few KB vanish and
// the server appends new lines from the shorter end.
type offsetAPI struct {
	mu      sync.Mutex
	name    string
	content string
}

func (a *offsetAPI) set(c string) { a.mu.Lock(); a.content = c; a.mu.Unlock() }

func (a *offsetAPI) DescribeDBLogFiles(context.Context, *awsrds.DescribeDBLogFilesInput, ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return &awsrds.DescribeDBLogFilesOutput{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
		{LogFileName: aws.String(a.name), Size: aws.Int64(int64(len(a.content))), LastWritten: aws.Int64(time.Now().UnixMilli())},
	}}, nil
}

func (a *offsetAPI) DownloadDBLogFilePortion(_ context.Context, in *awsrds.DownloadDBLogFilePortionInput, _ ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	off := 0
	if m := aws.ToString(in.Marker); m != "0" {
		off, _ = strconv.Atoi(m[strings.IndexByte(m, ':')+1:])
	}
	if off > len(a.content) {
		off = len(a.content)
	}
	return &awsrds.DownloadDBLogFilePortionOutput{
		LogFileData:           aws.String(a.content[off:]),
		Marker:                aws.String("11:" + strconv.Itoa(len(a.content))),
		AdditionalDataPending: aws.Bool(false),
	}, nil
}

func pgl(msg string) string {
	return fmt.Sprintf("2026-10-10 10:40:00 UTC::@:[1]:LOG:  %s\n", msg)
}

func runFailover(t *testing.T, before, after string, notify bool) []string {
	t.Helper()
	const f = "error/postgresql.log.2026-10-10-10"
	api := &offsetAPI{name: f, content: before}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", f, state.Checkpoint{Marker: "11:0"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() {
		for sink.RecordCount() < 3 {
			time.Sleep(5 * time.Millisecond)
		}
		api.set(after) // failover: tail lost, new lines appended from the shorter end
		if notify {
			w.NotifyRestart(time.Now(), "test")
		}
	}()
	_ = w.Run(ctx)
	var got []string
	for _, b := range sink.Batches() {
		for _, r := range b {
			got = append(got, r.Message[strings.LastIndex(r.Message, "  ")+2:])
		}
	}
	return got
}

// The new file is smaller than our position: no re-ship of the whole file,
// and the post-failover line is delivered.
func TestFailover_FileShrank_NoMassDuplicatesNoLoss(t *testing.T) {
	before := pgl("A") + pgl("B") + pgl("C-unflushed")
	after := pgl("A") + pgl("B") + pgl("N1")
	got := runFailover(t, before, after, false)
	if !slices.Equal(got, []string{"A", "B", "C-unflushed", "N1"}) {
		t.Fatalf("got %q", got)
	}
}

// The new file already grew past our old position before we noticed: without
// a restart signal the lines in the gap are skipped. With an RDS event
// (NotifyRestart) they are recovered and nothing is shipped twice.
func TestFailover_GrewPastMarker_RecoveredOnRestartEvent(t *testing.T) {
	before := pgl("A") + pgl("B") + pgl("C-unflushed")
	after := pgl("A") + pgl("B") + pgl("N1-in-gap") + pgl("N2-after")
	got := runFailover(t, before, after, true)
	if !slices.Equal(got, []string{"A", "B", "C-unflushed", "N1-in-gap", "N2-after"}) {
		t.Fatalf("got %q", got)
	}
}

func TestIsRestartEvent(t *testing.T) {
	ev := func(cat, msg string) rdstypes.Event {
		return rdstypes.Event{EventCategories: []string{cat}, Message: aws.String(msg)}
	}
	for _, c := range []struct {
		e    rdstypes.Event
		want bool
	}{
		{ev("failover", "Multi-AZ instance failover completed"), true},
		{ev("availability", "DB instance restarted"), true},
		{ev("availability", "DB instance shutdown"), true},
		{ev("backup", "Finished DB Instance backup"), false},
		{ev("configuration change", "Applied change to security group"), false},
	} {
		if got := rdssrc.IsRestartEvent(c.e); got != c.want {
			t.Errorf("%v: got %v", aws.ToString(c.e.Message), got)
		}
	}
}

// A start banner seen again on a re-read (rewinds, RDS's error-log copy) must
// not count as a new restart — seen live as a loop that rewound every file
// on every poll and shipped ~650k duplicates.
func TestFailover_BannerReRead_NotANewRestart(t *testing.T) {
	const f = "error/postgresql.log.2026-10-10-10"
	ts := time.Now().UTC().Add(time.Second).Format("2006-01-02 15:04:05")
	var b strings.Builder
	for i := 0; i < 3000; i++ {
		b.WriteString(pgl(fmt.Sprintf("q-%04d", i)))
	}
	b.WriteString(ts + " UTC::@:[1]:LOG:  database system is ready to accept connections\n")
	api := &offsetAPI{name: f, content: b.String()}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", f, state.Checkpoint{Marker: "11:0"})
	anomaly := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "a"}, []string{"instance", "kind"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 10 * time.Millisecond, AnomalyCounter: anomaly,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	if n := testutil.ToFloat64(anomaly.WithLabelValues("db-1", "restart")); n != 1 {
		t.Fatalf("restart counted %v times, want 1", n)
	}
	if got := sink.RecordCount(); got != 3001 {
		t.Fatalf("shipped %d records, want 3001 (no duplicates)", got)
	}
}

// flakyOffsetAPI fails the first download after arm() is called.
type flakyOffsetAPI struct {
	*offsetAPI
	failNext atomic.Bool
}

func (a *flakyOffsetAPI) DownloadDBLogFilePortion(ctx context.Context, in *awsrds.DownloadDBLogFilePortionInput, o ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	if a.failNext.CompareAndSwap(true, false) {
		return nil, fmt.Errorf("throttled")
	}
	return a.offsetAPI.DownloadDBLogFilePortion(ctx, in, o...)
}

// Review #2: a restart must stay pending until its rewind is durable — if the
// first rewound read fails, the next poll must still recover the gap.
func TestFailover_RewindSurvivesFailedFirstRead(t *testing.T) {
	const f = "error/postgresql.log.2026-10-10-10"
	before := pgl("A") + pgl("B") + pgl("C-unflushed")
	after := pgl("A") + pgl("B") + pgl("N1-in-gap") + pgl("N2-after")
	api := &flakyOffsetAPI{offsetAPI: &offsetAPI{name: f, content: before}}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", f, state.Checkpoint{Marker: "11:0"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{Fetcher: fetcher, Store: store, Sink: sink, PollInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() {
		for sink.RecordCount() < 3 {
			time.Sleep(5 * time.Millisecond)
		}
		api.set(after)
		api.failNext.Store(true)
		w.NotifyRestart(time.Now(), "test")
	}()
	_ = w.Run(ctx)
	got := tails(sink)
	if !slices.Equal(got, []string{"A", "B", "C-unflushed", "N1-in-gap", "N2-after"}) {
		t.Fatalf("got %q", got)
	}
}

// Review #5: a line repeated more times than it was shipped is new data.
func TestFailover_DedupeConsumesCounts(t *testing.T) {
	before := pgl("A") + pgl("dup") + pgl("C-unflushed")
	after := pgl("A") + pgl("dup") + pgl("dup") + pgl("dup") + pgl("N")
	got := runFailover(t, before, after, true)
	if !slices.Equal(got, []string{"A", "dup", "C-unflushed", "dup", "dup", "N"}) {
		t.Fatalf("got %q", got)
	}
}

// Review #1: files not written since before the restart aren't re-read.
func TestFailover_IdleFileNotRewound(t *testing.T) {
	const old = "error/postgresql.log.2026-10-07-03"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
			{LogFileName: aws.String(old), Size: aws.Int64(400_000), LastWritten: aws.Int64(time.Now().Add(-72 * time.Hour).UnixMilli())},
		}}},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", old, state.Checkpoint{Marker: "11:400000", FileSize: 400_000})
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{Fetcher: fetcher, Store: store, Sink: memory.New("mem"), PollInterval: time.Hour})
	w.NotifyRestart(time.Now(), "test")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	for _, c := range api.downloadCalls {
		if c != old+"|11:400000" {
			t.Fatalf("idle file was rewound: %v", api.downloadCalls)
		}
	}
}

func tails(s *memory.Sink) []string {
	var got []string
	for _, b := range s.Batches() {
		for _, r := range b {
			got = append(got, r.Message[strings.LastIndex(r.Message, "  ")+2:])
		}
	}
	return got
}
