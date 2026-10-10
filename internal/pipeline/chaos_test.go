package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"sync"
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
	filestore "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// TestChaos_AtLeastOnce generates a synthetic stream of two-line Postgres
// entries behind a fake RDS server, flips sink failures on and off, and
// verifies every source entry ends up in the sink at least once (duplicates
// allowed) and whole — chunk boundaries deliberately fall mid-entry.
func TestChaos_AtLeastOnce(t *testing.T) {
	// Sweep seeds × batching modes: per-chunk flushes (every boundary is a
	// hold-back checkpoint) and record-threshold coalescing.
	for seed := uint64(1); seed <= 12; seed++ {
		for _, maxRecs := range []int{0, 25} {
			t.Run(fmt.Sprintf("seed=%d/max_batch_records=%d", seed, maxRecs), func(t *testing.T) {
				t.Parallel()
				runChaos(t, seed, maxRecs)
			})
		}
	}
}

func runChaos(t *testing.T, seed uint64, maxBatchRecords int) {
	const (
		fname  = "error/postgresql.log"
		lines  = 200 // entries; each is a header line plus one continuation line
		chunks = 7   // 400 lines / 7 → boundaries land between header and continuation
	)
	// Deterministic RNG so failures are reproducible.
	rng := rand.New(rand.NewPCG(seed, 0xDEAD))

	// Build a scripted API that paginates `lines` lines across `chunks` chunks.
	api := newChaosAPI(fname, lines, chunks)

	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})

	flakySink := &chaosSink{inner: memory.New("mem"), rng: rng, failProb: 0.4}

	p := filepath.Join(t.TempDir(), "state.json")
	store, err := filestore.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "0"})

	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: flakySink, PollInterval: 5 * time.Millisecond,
		MaxBatchRecords: maxBatchRecords,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = w.Run(ctx)

	seen := map[string]int{}
	for _, batch := range flakySink.inner.Batches() {
		for _, r := range batch {
			seen[r.Message]++
		}
	}
	want := map[string]bool{}
	missing := 0
	for i := 1; i <= lines; i++ {
		msg := chaosEntry(i)
		want[msg] = true
		if _, ok := seen[msg]; !ok {
			missing++
		}
	}
	for msg := range seen {
		if !want[msg] {
			t.Fatalf("split or corrupted entry delivered: %q", msg)
		}
	}
	if missing > 0 {
		t.Fatalf("missing %d/%d source lines (delivered ⊇ source must hold). first 5 present: %d",
			missing, lines, len(seen))
	}
	t.Logf("delivered %d total records (source=%d); duplicates=%d; failures=%d",
		flakySink.inner.RecordCount(), lines, flakySink.inner.RecordCount()-lines, flakySink.failed.Load())
}

// chaosEntry is source entry i as one record should carry it.
func chaosEntry(i int) string {
	return fmt.Sprintf("2026-07-21 10:00:00 UTC::@:[7]:LOG:  line-%d\n\tdetail-%d", i, i)
}

// chaosAPI emits `total` two-line entries across `chunks` responses for a
// single logfile, cutting chunks by line count so entries straddle chunks.
// Markers are "c0", "c1", .., "tail". It always lists the same file via Describe.
type chaosAPI struct {
	fname  string
	total  int
	chunks int
}

func newChaosAPI(fname string, total, chunks int) *chaosAPI {
	return &chaosAPI{fname: fname, total: total, chunks: chunks}
}

func (a *chaosAPI) DescribeDBLogFiles(_ context.Context, _ *awsrds.DescribeDBLogFilesInput, _ ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error) {
	return &awsrds.DescribeDBLogFilesOutput{
		DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
			{LogFileName: aws.String(a.fname), Size: aws.Int64(int64(a.total * 10)), LastWritten: aws.Int64(1)},
		},
	}, nil
}

func (a *chaosAPI) DownloadDBLogFilePortion(_ context.Context, in *awsrds.DownloadDBLogFilePortionInput, _ ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error) {
	cur := aws.ToString(in.Marker)
	idx := 0
	if cur == "tail" {
		idx = a.chunks
	} else if cur != "0" && cur != "" {
		_, _ = fmt.Sscanf(cur, "c%d", &idx)
		idx++
	}
	if idx >= a.chunks {
		return &awsrds.DownloadDBLogFilePortionOutput{Marker: aws.String("tail"), AdditionalDataPending: aws.Bool(false)}, nil
	}
	var all []string
	for i := 1; i <= a.total; i++ {
		all = append(all, strings.Split(chaosEntry(i), "\n")...)
	}
	perChunk := len(all) / a.chunks
	start := idx * perChunk
	end := start + perChunk
	if idx == a.chunks-1 {
		end = len(all)
	}
	data := strings.Join(all[start:end], "\n") + "\n"
	next := fmt.Sprintf("c%d", idx)
	pending := idx < a.chunks-1
	return &awsrds.DownloadDBLogFilePortionOutput{
		LogFileData:           aws.String(data),
		Marker:                aws.String(next),
		AdditionalDataPending: aws.Bool(pending),
	}, nil
}

// chaosSink injects random transient failures based on failProb.
type chaosSink struct {
	inner    *memory.Sink
	rng      *rand.Rand
	failProb float64
	failed   atomic.Int64
	mu       sync.Mutex
}

func (c *chaosSink) Name() string { return "chaos" }
func (c *chaosSink) Type() string { return "test" }
func (c *chaosSink) Close() error { return nil }
func (c *chaosSink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	c.mu.Lock()
	fail := c.rng.Float64() < c.failProb
	c.mu.Unlock()
	if fail {
		c.failed.Add(1)
		return errors.New("chaos: transient failure")
	}
	return c.inner.Write(ctx, records)
}
