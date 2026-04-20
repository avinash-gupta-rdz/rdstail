package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
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

// TestChaos_AtLeastOnce generates a synthetic stream of lines behind a fake
// RDS server, flips sink failures on and off, and verifies every source line
// ends up in the sink at least once (duplicates allowed).
func TestChaos_AtLeastOnce(t *testing.T) {
	const (
		fname  = "error/postgresql.log"
		lines  = 200
		chunks = 8
	)
	// Deterministic RNG so failures are reproducible.
	rng := rand.New(rand.NewPCG(0xC0FFEE, 0xDEAD))

	// Build a scripted API that paginates `lines` lines across `chunks` chunks.
	api := newChaosAPI(fname, lines, chunks)

	fetcher, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: api, InstanceID: "db-1", Engine: "postgres"})

	flakySink := &chaosSink{inner: memory.New("mem"), rng: rng, failProb: 0.3}

	p := filepath.Join(t.TempDir(), "state.json")
	store, err := filestore.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_ = store.Set(context.Background(), "db-1", fname, state.Checkpoint{Marker: "0"})

	w, _ := pipeline.NewInstanceWorker(pipeline.InstanceWorkerOpts{
		Fetcher: fetcher, Store: store, Sink: flakySink, PollInterval: 5 * time.Millisecond,
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
	missing := 0
	for i := 1; i <= lines; i++ {
		msg := fmt.Sprintf("line-%d", i)
		if _, ok := seen[msg]; !ok {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("missing %d/%d source lines (delivered ⊇ source must hold). first 5 present: %d",
			missing, lines, len(seen))
	}
	t.Logf("delivered %d total records (source=%d); duplicates=%d; failures=%d",
		flakySink.inner.RecordCount(), lines, flakySink.inner.RecordCount()-lines, flakySink.failed.Load())
}

// chaosAPI emits `total` lines across `chunks` responses for a single logfile.
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
	if cur != "0" && cur != "" {
		_, _ = fmt.Sscanf(cur, "c%d", &idx)
		idx++
	}
	if idx >= a.chunks {
		return &awsrds.DownloadDBLogFilePortionOutput{Marker: aws.String("tail"), AdditionalDataPending: aws.Bool(false)}, nil
	}
	perChunk := a.total / a.chunks
	start := idx*perChunk + 1
	end := start + perChunk - 1
	if idx == a.chunks-1 {
		end = a.total
	}
	var data string
	for i := start; i <= end; i++ {
		data += fmt.Sprintf("line-%d\n", i)
	}
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
}

func (c *chaosSink) Name() string { return "chaos" }
func (c *chaosSink) Type() string { return "test" }
func (c *chaosSink) Close() error { return nil }
func (c *chaosSink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	if c.rng.Float64() < c.failProb {
		c.failed.Add(1)
		return errors.New("chaos: transient failure")
	}
	return c.inner.Write(ctx, records)
}
