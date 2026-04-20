package sink

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/avinash-gupta-rdz/rdstail/internal/state"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// dlqDecorator routes terminal Write failures into a state.DLQ store so they
// can be replayed later. Batches that succeed OR are deemed permanent-failure
// are both acknowledged — this lets the pipeline advance its checkpoint even
// when a sink is misconfigured (the operator replays from DLQ once fixed).
type dlqDecorator struct {
	inner Sink
	dlq   state.DLQ
}

// WithDLQ wraps s so that terminal failures are parked in dlq instead of
// propagated. Returns s unchanged if dlq is nil.
//
// Semantics: if inner.Write returns any error (transient or permanent), the
// batch is serialised into the DLQ and Write returns nil. The batch_id from
// the first record is used as the DLQ row's correlation id.
//
// Pair this with WithRetry (wrap DLQ outside of retry) so retries run first
// and only truly-exhausted batches hit DLQ.
func WithDLQ(s Sink, dlq state.DLQ) Sink {
	if dlq == nil {
		return s
	}
	return &dlqDecorator{inner: s, dlq: dlq}
}

func (d *dlqDecorator) Name() string { return d.inner.Name() }
func (d *dlqDecorator) Type() string { return d.inner.Type() }
func (d *dlqDecorator) Close() error { return d.inner.Close() }

func (d *dlqDecorator) Write(ctx context.Context, records []logrecord.LogRecord) error {
	err := d.inner.Write(ctx, records)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Don't DLQ cancel-driven failures; the next poll retries naturally.
		return err
	}
	if len(records) == 0 {
		return err
	}
	batchID := records[0].BatchID
	payload, jerr := json.Marshal(records)
	if jerr != nil {
		return err
	}
	if putErr := d.dlq.DLQPut(ctx, d.inner.Name(), batchID, payload, err.Error()); putErr != nil {
		return errors.Join(err, putErr)
	}
	return nil
}
