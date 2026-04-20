package pipeline_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
)

func TestKeyedRunner_SerializesPerKey(t *testing.T) {
	r := pipeline.NewKeyedRunner(4)
	defer r.Close()

	var (
		mu         sync.Mutex
		order      []int
		activeSame atomic.Int32
		maxSame    atomic.Int32
	)
	const n = 20
	for i := 0; i < n; i++ {
		i := i
		r.Submit(context.Background(), "k1", func() {
			cur := activeSame.Add(1)
			if cur > maxSame.Load() {
				maxSame.Store(cur)
			}
			time.Sleep(1 * time.Millisecond)
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			activeSame.Add(-1)
		})
	}
	r.Close()
	if maxSame.Load() > 1 {
		t.Fatalf("expected no concurrent runs for same key, saw %d", maxSame.Load())
	}
	if len(order) != n {
		t.Fatalf("expected %d runs, got %d", n, len(order))
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("out-of-order at %d: got %d", i, v)
		}
	}
}

func TestKeyedRunner_AllowsParallelAcrossKeys(t *testing.T) {
	r := pipeline.NewKeyedRunner(4)

	var active atomic.Int32
	var peak atomic.Int32
	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		key := string(rune('a' + i))
		r.Submit(context.Background(), key, func() {
			defer wg.Done()
			cur := active.Add(1)
			if cur > peak.Load() {
				peak.Store(cur)
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
		})
	}
	wg.Wait()
	r.Close()
	if peak.Load() < 2 {
		t.Fatalf("expected >=2 concurrent runs across keys, peak=%d", peak.Load())
	}
}

func TestKeyedRunner_ClosedRejectsSubmit(t *testing.T) {
	r := pipeline.NewKeyedRunner(1)
	r.Close()
	if r.Submit(context.Background(), "k", func() {}) {
		t.Fatal("submit on closed runner should return false")
	}
}
