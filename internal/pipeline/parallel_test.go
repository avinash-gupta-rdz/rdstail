package pipeline_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// rangeAPI serves a fixed file with "11:<offset>" markers, capping each
// response at cap bytes (cut at a line end when possible) like RDS's 1 MB cap.
type rangeAPI struct {
	name    string
	content string
	cap     int
	calls   atomic.Int64
	ranged  atomic.Int64 // requests starting one byte before a segment boundary
}

func (a *rangeAPI) DescribeDBLogFiles(context.Context, *awsrds.DescribeDBLogFilesInput, ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	return &awsrds.DescribeDBLogFilesOutput{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
		{LogFileName: aws.String(a.name), Size: aws.Int64(int64(len(a.content))), LastWritten: aws.Int64(1)},
	}}, nil
}

func (a *rangeAPI) DownloadDBLogFilePortion(_ context.Context, in *awsrds.DownloadDBLogFilePortionInput, _ ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	a.calls.Add(1)
	off := 0
	if m := aws.ToString(in.Marker); m != "0" {
		off, _ = strconv.Atoi(m[strings.IndexByte(m, ':')+1:])
	}
	if off > 0 && (off+1)%(2<<20) == 0 {
		a.ranged.Add(1)
	}
	end := min(off+a.cap, len(a.content))
	if end < len(a.content) {
		if i := strings.LastIndexByte(a.content[off:end], '\n'); i >= 0 {
			end = off + i + 1
		}
	}
	return &awsrds.DownloadDBLogFilePortionOutput{
		LogFileData:           aws.String(a.content[off:end]),
		Marker:                aws.String("11:" + strconv.Itoa(end)),
		AdditionalDataPending: aws.Bool(end < len(a.content)),
	}, nil
}

func syntheticPG(bytes int) string {
	rng := rand.New(rand.NewPCG(7, 7))
	var b strings.Builder
	for i := 0; b.Len() < bytes; i++ {
		fmt.Fprintf(&b, "2026-10-10 10:40:%02d UTC:10.0.0.1(5432):app@db:[%d]:LOG:  statement: entry-%d", i%60, i%9000, i)
		if i%97 == 0 {
			b.WriteString(" " + strings.Repeat("v", 100_000)) // long line
		}
		b.WriteByte('\n')
		for j := 0; j < rng.IntN(4); j++ { // multi-line SQL continuation lines
			fmt.Fprintf(&b, "\tAND col_%d = %d\n", j, i)
		}
	}
	return b.String()
}

func drainAll(t *testing.T, api *rangeAPI, parallel int) ([]string, string) {
	t.Helper()
	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})
	store := newFileStore(t)
	_ = store.Set(context.Background(), "db-1", api.name, state.Checkpoint{Marker: "11:0"})
	sink := memory.New("mem")
	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: sink, PollInterval: time.Hour,
		ParallelReads: parallel, MaxBatchBytes: 5 << 20,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { // stop after the first poll completes
		for {
			if cp, ok, _ := store.Get(context.Background(), "db-1", api.name); ok && cp.Marker == "11:"+strconv.Itoa(len(api.content)) {
				time.Sleep(50 * time.Millisecond)
				cancel()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	_ = w.Run(ctx)
	var msgs []string
	for _, b := range sink.Batches() {
		for _, r := range b {
			msgs = append(msgs, r.Message)
		}
	}
	cp, _, _ := store.Get(context.Background(), "db-1", api.name)
	return msgs, cp.Marker
}

// Parallel range reads must produce exactly the records a sequential read
// does — same entries, same grouping, same order, nothing split or lost.
func TestParallelReads_IdenticalToSequential(t *testing.T) {
	content := syntheticPG(18 << 20)
	seq, seqMarker := drainAll(t, &rangeAPI{name: "error/postgresql.log.2026-10-10-10", content: content, cap: 256 << 10}, 1)
	for _, n := range []int{2, 4, 8} {
		api := &rangeAPI{name: "error/postgresql.log.2026-10-10-10", content: content, cap: 256 << 10}
		par, parMarker := drainAll(t, api, n)
		if !slices.Equal(seq, par) {
			first := 0
			for first < min(len(seq), len(par)) && seq[first] == par[first] {
				first++
			}
			t.Fatalf("parallel=%d: %d records vs %d sequential; first difference at %d", n, len(par), len(seq), first)
		}
		if api.ranged.Load() == 0 {
			t.Fatalf("parallel=%d: no byte-range requests made — parallel path never ran", n)
		}
		if parMarker != seqMarker {
			t.Fatalf("parallel=%d: checkpoint %q, sequential %q", n, parMarker, seqMarker)
		}
	}
	if len(seq) < 1000 || strings.Join(seq, "\n")+"\n" != content {
		t.Fatalf("sequential reference doesn't reproduce the file (%d records)", len(seq))
	}
}
