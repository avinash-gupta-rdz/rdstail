package pipeline_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// These scenarios mirror RDS MySQL 8.4 hourly-rotation behaviour observed on a
// live instance (see rds/rotation.go). Base file: general/mysql-general.log.

const genBase = "general/mysql-general.log"

func listing(files map[string]int64) *awsrds.DescribeDBLogFilesOutput {
	out := &awsrds.DescribeDBLogFilesOutput{}
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		out.DescribeDBLogFiles = append(out.DescribeDBLogFiles, rdstypes.DescribeDBLogFilesDetails{
			LogFileName: aws.String(n), Size: aws.Int64(files[n]), LastWritten: aws.Int64(1),
		})
	}
	return out
}

func genLine(ts, q string) string { return ts + "\t    7 Query\t" + q + "\n" }

func done(data, next string) *awsrds.DownloadDBLogFilePortionOutput {
	return &awsrds.DownloadDBLogFilePortionOutput{LogFileData: aws.String(data), Marker: aws.String(next), AdditionalDataPending: aws.Bool(false)}
}

type rotationRun struct {
	sink    *memory.Sink
	store   state.StateStore
	api     *scriptedAPI
	anomaly *prometheus.CounterVec
}

func runRotation(t *testing.T, api *scriptedAPI, seed map[string]string, startFrom string) rotationRun {
	t.Helper()
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	store := newFileStore(t)
	for f, m := range seed {
		_ = store.Set(context.Background(), "db-1", f, state.Checkpoint{Marker: m, FileSize: 50})
	}
	anomaly := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "a"}, []string{"instance", "kind"})
	sink := memory.New("mem")
	w, err := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: time.Hour,
		StartFrom: startFrom, AnomalyCounter: anomaly,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	return rotationRun{sink: sink, store: store, api: api, anomaly: anomaly}
}

func messages(s *memory.Sink) []string {
	var out []string
	for _, b := range s.Batches() {
		for _, r := range b {
			out = append(out, strings.TrimSpace(r.Message[strings.LastIndexByte(r.Message, '\t')+1:]))
		}
	}
	slices.Sort(out)
	return out
}

// One rotation since the last poll: the base checkpoint (in hour 7) is handed
// to the rotated hour-7 file so its tail is read, and the base restarts at the
// live file. The old code reset the base marker to 0 and lost hour 7's tail.
func TestRotation_OneRotation_HandsCheckpointToRotatedFile(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{
			genBase: 40, genBase + ".2026-10-10.7": 300,
		})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			genBase + ".2026-10-10.7|2026-10-10.7:100": done(genLine("2026-10-10T06:59:59.1Z", "tail-of-7"), "2026-10-10.8:300"),
			genBase + "|0": done(genLine("2026-10-10T07:00:01.1Z", "start-of-8"), "2026-10-10.8:40"),
		},
	}
	r := runRotation(t, api, map[string]string{genBase: "2026-10-10.7:100"}, config.StartFromEnd)
	if got := messages(r.sink); !slices.Equal(got, []string{"start-of-8", "tail-of-7"}) {
		t.Fatalf("got %v; calls %v", got, api.downloadCalls)
	}
}

// Two rotations while rdstail was down: the RDS marker chain would stick at
// the end of hour 7 forever. Each hour file must be read exactly once.
func TestRotation_TwoRotations_ReadsEveryHourOnce(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{
			genBase: 40, genBase + ".2026-10-10.7": 300, genBase + ".2026-10-10.8": 200,
		})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			genBase + ".2026-10-10.7|2026-10-10.7:100": done(genLine("2026-10-10T06:59:59Z", "tail-of-7"), "2026-10-10.9:300"),
			genBase + ".2026-10-10.8|0":                done(genLine("2026-10-10T07:30:00Z", "all-of-8"), "2026-10-10.9:200"),
			genBase + "|0":                             done(genLine("2026-10-10T08:00:01Z", "start-of-9"), "2026-10-10.9:40"),
		},
	}
	r := runRotation(t, api, map[string]string{genBase: "2026-10-10.7:100"}, config.StartFromEnd)
	if got := messages(r.sink); !slices.Equal(got, []string{"all-of-8", "start-of-9", "tail-of-7"}) {
		t.Fatalf("got %v; calls %v", got, api.downloadCalls)
	}
	if n := testutil.ToFloat64(r.anomaly.WithLabelValues("db-1", "gap")); n != 0 {
		t.Fatalf("no gap expected, got %v", n)
	}
}

// Down past retention: the checkpoint's hour (5) was purged. Report the gap
// and read every hour RDS still has.
func TestRotation_PurgedHour_ReportsGapAndReadsRetained(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{
			genBase: 40, genBase + ".2026-10-10.7": 300, genBase + ".2026-10-10.8": 200,
		})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			genBase + ".2026-10-10.7|0": done(genLine("2026-10-10T06:00:00Z", "all-of-7"), "x:300"),
			genBase + ".2026-10-10.8|0": done(genLine("2026-10-10T07:00:00Z", "all-of-8"), "x:200"),
			genBase + "|0":              done(genLine("2026-10-10T08:00:00Z", "start-of-9"), "2026-10-10.9:40"),
		},
	}
	r := runRotation(t, api, map[string]string{genBase: "2026-10-10.5:100"}, config.StartFromEnd)
	if got := messages(r.sink); !slices.Equal(got, []string{"all-of-7", "all-of-8", "start-of-9"}) {
		t.Fatalf("got %v; calls %v", got, api.downloadCalls)
	}
	if n := testutil.ToFloat64(r.anomaly.WithLabelValues("db-1", "gap")); n != 1 {
		t.Fatalf("expected one gap anomaly, got %v", n)
	}
}

// Steady state: the base checkpoint is in the live hour, so rotated hours were
// already read through it — they must cost no API calls and ship nothing.
func TestRotation_SteadyState_SkipsRotatedWithoutAPICalls(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{
			genBase: 140, genBase + ".2026-10-10.7": 300, genBase + ".2026-10-10.8": 200,
		})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			genBase + "|2026-10-10.9:40": done(genLine("2026-10-10T08:10:00Z", "new-in-9"), "2026-10-10.9:140"),
		},
	}
	r := runRotation(t, api, map[string]string{genBase: "2026-10-10.9:40"}, config.StartFromEnd)
	if got := messages(r.sink); !slices.Equal(got, []string{"new-in-9"}) {
		t.Fatalf("got %v", got)
	}
	for _, c := range api.downloadCalls {
		if strings.Contains(c, ".2026-10-10.") && !strings.HasPrefix(c, genBase+"|") {
			t.Fatalf("rotated file was fetched in steady state: %v", api.downloadCalls)
		}
	}
}

// First run with start_from=beginning backfills every retained hour plus the
// live file; with start_from=end rotated hours are ignored (no 24h download).
func TestRotation_FirstRun_BackfillOnlyWithBeginning(t *testing.T) {
	mk := func() *scriptedAPI {
		return &scriptedAPI{
			describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{
				genBase: 40, genBase + ".2026-10-10.8": 200,
			})},
			downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
				genBase + ".2026-10-10.8|0": done(genLine("2026-10-10T07:00:00Z", "all-of-8"), "2026-10-10.9:200"),
				genBase + "|0":              done(genLine("2026-10-10T08:00:00Z", "start-of-9"), "2026-10-10.9:40"),
			},
		}
	}
	r := runRotation(t, mk(), nil, config.StartFromBeginning)
	if got := messages(r.sink); !slices.Equal(got, []string{"all-of-8", "start-of-9"}) {
		t.Fatalf("beginning: got %v", got)
	}
	api := mk()
	r = runRotation(t, api, nil, config.StartFromEnd)
	if got := messages(r.sink); len(got) != 0 {
		t.Fatalf("end: expected nothing shipped, got %v", got)
	}
	for _, c := range api.downloadCalls {
		if strings.HasPrefix(c, genBase+".2026") {
			t.Fatalf("end: rotated file fetched: %v", api.downloadCalls)
		}
	}
}

// A file that appears after the first poll (Postgres opens a new file every
// hour) is new data: it must be read from the beginning even with
// start_from=end. The old code skipped it to the end, dropping the first poll
// interval of every hour.
func TestWorker_FileAppearingLater_ReadFromBeginning(t *testing.T) {
	const h1, h2 = "error/postgresql.log.2026-10-10-07", "error/postgresql.log.2026-10-10-08"
	pg := func(msg string) string { return "2026-10-10 08:00:00 UTC::@:[1]:LOG:  " + msg + "\n" }
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{
			listing(map[string]int64{h1: 100}),
			listing(map[string]int64{h1: 100, h2: 30}),
		},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			h1 + "|0": done(pg("old"), "m1"),
			h2 + "|0": done(pg("first-lines-of-new-hour"), "m2"),
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: newFileStore(t), Sink: sink, PollInterval: 20 * time.Millisecond,
		StartFrom: config.StartFromEnd,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	var got []string
	for _, b := range sink.Batches() {
		for _, r := range b {
			got = append(got, r.Message)
		}
	}
	if len(got) != 1 || !strings.HasSuffix(got[0], "first-lines-of-new-hour") {
		t.Fatalf("expected only the new hour's lines, got %q", got)
	}
}

// A file that keeps growing while its marker never moves is reported.
func TestWorker_StalledMarker_Reported(t *testing.T) {
	var resp []*awsrds.DescribeDBLogFilesOutput
	for i := int64(1); i <= 6; i++ {
		resp = append(resp, &awsrds.DescribeDBLogFilesOutput{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
			{LogFileName: aws.String(genBase), Size: aws.Int64(100 * i), LastWritten: aws.Int64(1000 * i)},
		}})
	}
	api := &scriptedAPI{describeResponses: resp} // every download: no data, same marker
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", genBase, state.Checkpoint{Marker: "stuck", LastWritten: time.UnixMilli(1)})
	anomaly := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "a"}, []string{"instance", "kind"})
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: memory.New("mem"), PollInterval: 10 * time.Millisecond,
		AnomalyCounter: anomaly,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	if n := testutil.ToFloat64(anomaly.WithLabelValues("db-1", "stall")); n != 1 {
		t.Fatalf("expected one stall anomaly, got %v", n)
	}
}

// RDS copies mysql-error.log into mysql-error-running.log every ~5 minutes.
// Lines already shipped from the realtime file must not ship again; lines the
// realtime file lost to truncation must still ship from the running log.
func TestWorker_MySQLErrorLog_NotShippedTwice(t *testing.T) {
	const errLog, running = "error/mysql-error.log", "error/mysql-error-running.log"
	l1 := "2026-10-10T08:35:24.833233Z 22 [Warning] [MY-010057] [Server] seen-in-realtime\n"
	l2 := "2026-10-10T08:35:27.121341Z 23 [Warning] [MY-010057] [Server] only-in-running\n"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{errLog: 80, running: 160})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			errLog + "|e0":  done(l1, "e1"),
			running + "|r0": done(l1+l2, "r1"),
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", errLog, state.Checkpoint{Marker: "e0"})
	_ = store.Set(context.Background(), "db-1", running, state.Checkpoint{Marker: "r0"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: time.Hour, DrainSem: make(chan struct{}, 4),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	var got []string
	for _, b := range sink.Batches() {
		for _, r := range b {
			got = append(got, r.LogFile+": "+r.Message[strings.LastIndexByte(r.Message, ' ')+1:])
		}
	}
	slices.Sort(got)
	want := []string{running + ": only-in-running", errLog + ": seen-in-realtime"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q want %q", got, want)
	}
}

// An old, idle file whose checkpoint row was pruned (checkpoint_retention)
// must not be re-shipped from the beginning on restart.
func TestWorker_PrunedOldFile_NotReshipped(t *testing.T) {
	const old, cur = "error/postgresql.log.2026-10-07-03", "error/postgresql.log.2026-10-10-09"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
			{LogFileName: aws.String(old), Size: aws.Int64(500), LastWritten: aws.Int64(1_000)},
			{LogFileName: aws.String(cur), Size: aws.Int64(80), LastWritten: aws.Int64(9_000)},
		}}},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			old + "|0":  {LogFileData: aws.String("2026-10-07 03:00:00 UTC::@:[1]:LOG:  ancient\n"), Marker: aws.String("o1"), AdditionalDataPending: aws.Bool(false)},
			cur + "|c0": done("2026-10-10 09:00:00 UTC::@:[1]:LOG:  fresh\n", "c1"),
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", cur, state.Checkpoint{Marker: "c0", FileSize: 40, LastWritten: time.UnixMilli(8_000)})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: time.Hour, StartFrom: config.StartFromEnd,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	for _, b := range sink.Batches() {
		for _, r := range b {
			if strings.Contains(r.Message, "ancient") {
				t.Fatalf("pruned old file re-shipped: %q", r.Message)
			}
		}
	}
	if sink.RecordCount() != 1 {
		t.Fatalf("expected only the fresh line, got %d", sink.RecordCount())
	}
}

// Rotation during a drain: the RDS marker chain already moved the base
// checkpoint into the new hour (offset 120) while the stored size is the old
// hour's. The smaller live file must not be treated as truncated and re-read.
func TestRotation_MidDrainRace_NotReread(t *testing.T) {
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{
			genBase: 200, genBase + ".2026-10-10.9": 69_000_000,
		})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			genBase + "|2026-10-10.10:120": done(genLine("2026-10-10T09:00:09Z", "next"), "2026-10-10.10:200"),
			genBase + "|0":                 done(genLine("2026-10-10T09:00:00Z", "REREAD"), "2026-10-10.10:200"),
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "mysql"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", genBase, state.Checkpoint{Marker: "2026-10-10.10:120", FileSize: 69_000_000})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{Fetcher: fetcher, Store: store, Sink: sink, PollInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	if got := messages(sink); !slices.Equal(got, []string{"next"}) {
		t.Fatalf("got %v (calls %v)", got, api.downloadCalls)
	}
}

// start_from=end anchors at the file's size when rdstail started: if the
// first skip is delayed (throttling), lines written after startup still ship.
func TestWorker_StartFromEnd_AnchoredAtStartup(t *testing.T) {
	const f = "error/postgresql.log.2026-10-10-09"
	pg := "2026-10-10 09:00:00 UTC::@:[1]:LOG:  written-after-start\n"
	api := &scriptedAPI{
		describeResponses: []*awsrds.DescribeDBLogFilesOutput{listing(map[string]int64{f: 1000}), listing(map[string]int64{f: 1060})},
		downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
			f + "|0":       {Marker: aws.String("10:50"), AdditionalDataPending: aws.Bool(true), LogFileData: aws.String("x\n")},
			f + "|10:1000": done(pg, "10:1060"),
		},
	}
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: newFileStore(t), Sink: sink, PollInterval: 20 * time.Millisecond, StartFrom: config.StartFromEnd,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)
	var got []string
	for _, b := range sink.Batches() {
		for _, r := range b {
			got = append(got, r.Message)
		}
	}
	if len(got) != 1 || !strings.HasSuffix(got[0], "written-after-start") {
		t.Fatalf("got %q; calls %v", got, api.downloadCalls)
	}
}
