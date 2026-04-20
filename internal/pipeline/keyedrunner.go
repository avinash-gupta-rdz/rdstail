// Package pipeline wires fetchers, state, and sinks into a per-instance worker
// pool with graceful shutdown.
package pipeline

import (
	"context"
	"sync"
)

// KeyedRunner executes functions serialised per key across a bounded worker
// pool. Two submissions with the same key are guaranteed to run in submission
// order; submissions with different keys may run concurrently up to the pool
// size.
//
// Typical use: key = instance|logfile, so each file's chunks are delivered to
// sinks in order while different files progress in parallel.
type KeyedRunner struct {
	mu      sync.Mutex
	queues  map[string]*keyedQueue
	sem     chan struct{}
	closed  bool
	wg      sync.WaitGroup
}

type keyedQueue struct {
	key   string
	tasks chan func()
	done  chan struct{}
}

// NewKeyedRunner returns a runner with the given worker pool size.
// poolSize <= 0 is treated as 1.
func NewKeyedRunner(poolSize int) *KeyedRunner {
	if poolSize <= 0 {
		poolSize = 1
	}
	return &KeyedRunner{
		queues: map[string]*keyedQueue{},
		sem:    make(chan struct{}, poolSize),
	}
}

// Submit schedules fn for key. Submit blocks if the key's per-key buffer is
// full OR if the pool is saturated. Returns immediately after the task has
// been queued (fn itself runs asynchronously).
//
// Returns false if the runner has already been Closed.
func (r *KeyedRunner) Submit(ctx context.Context, key string, fn func()) bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	q, ok := r.queues[key]
	if !ok {
		q = &keyedQueue{
			key:   key,
			tasks: make(chan func(), 16),
			done:  make(chan struct{}),
		}
		r.queues[key] = q
		r.wg.Add(1)
		go r.runQueue(q)
	}
	r.mu.Unlock()

	select {
	case q.tasks <- fn:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *KeyedRunner) runQueue(q *keyedQueue) {
	defer r.wg.Done()
	for fn := range q.tasks {
		r.sem <- struct{}{}
		fn()
		<-r.sem
	}
	close(q.done)
}

// Close signals that no more tasks will be submitted and waits for all
// already-queued tasks to drain. It is safe to call Close more than once;
// subsequent calls return immediately.
func (r *KeyedRunner) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	queues := make([]*keyedQueue, 0, len(r.queues))
	for _, q := range r.queues {
		queues = append(queues, q)
	}
	r.mu.Unlock()

	for _, q := range queues {
		close(q.tasks)
	}
	r.wg.Wait()
}
