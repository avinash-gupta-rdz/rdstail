package sink

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// RetryConfig parameterises the retry decorator. Zero fields use sensible defaults.
type RetryConfig struct {
	MaxAttempts int
	InitialWait time.Duration
	MaxWait     time.Duration
	Multiplier  float64
}

// retryDecorator wraps a Sink with exponential-backoff-with-jitter retries.
type retryDecorator struct {
	inner Sink
	cfg   RetryConfig
}

// WithRetry wraps s with exp-backoff-with-jitter retries per RetryConfig. If the
// underlying Write returns a PermanentError, retry is skipped and the error is
// propagated immediately so the caller can route the batch to a DLQ.
func WithRetry(s Sink, cfg RetryConfig) Sink {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 10
	}
	if cfg.InitialWait <= 0 {
		cfg.InitialWait = 500 * time.Millisecond
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 60 * time.Second
	}
	if cfg.Multiplier <= 1 {
		cfg.Multiplier = 2.0
	}
	return &retryDecorator{inner: s, cfg: cfg}
}

func (r *retryDecorator) Name() string { return r.inner.Name() }
func (r *retryDecorator) Type() string { return r.inner.Type() }
func (r *retryDecorator) Close() error { return r.inner.Close() }

func (r *retryDecorator) Write(ctx context.Context, records []logrecord.LogRecord) error {
	var lastErr error
	for attempt := 1; attempt <= r.cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := r.inner.Write(ctx, records)
		if err == nil {
			return nil
		}
		// Permanent errors short-circuit retries and bubble up.
		var perm *PermanentError
		if errors.As(err, &perm) {
			return err
		}
		lastErr = err
		if attempt == r.cfg.MaxAttempts {
			break
		}
		wait := r.backoff(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return lastErr
}

func (r *retryDecorator) backoff(attempt int) time.Duration {
	// attempt is 1-indexed. Base = InitialWait * Multiplier^(attempt-1).
	mult := math.Pow(r.cfg.Multiplier, float64(attempt-1))
	d := time.Duration(float64(r.cfg.InitialWait) * mult)
	if d > r.cfg.MaxWait {
		d = r.cfg.MaxWait
	}
	// Full jitter: uniform in [d/2, d].
	jitter := d/2 + time.Duration(rand.Int64N(int64(d/2+1)))
	return jitter
}

// PermanentError marks a sink failure that should NOT be retried (e.g. HTTP 4xx,
// auth failure). Callers route these to DLQ after propagation.
type PermanentError struct {
	Cause error
}

// NewPermanentError wraps cause so it is recognised by WithRetry as terminal.
func NewPermanentError(cause error) error { return &PermanentError{Cause: cause} }

func (e *PermanentError) Error() string {
	if e.Cause == nil {
		return "permanent error"
	}
	return "permanent: " + e.Cause.Error()
}

func (e *PermanentError) Unwrap() error { return e.Cause }
