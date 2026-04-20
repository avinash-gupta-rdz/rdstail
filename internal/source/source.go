// Package source defines the ingestion source contract. The only implementation
// in v1 is source/rds, but the interface leaves room for non-RDS sources.
package source

import (
	"context"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// Source emits log records for a single logical origin (e.g. one RDS instance).
// Fetch is called repeatedly by the pipeline; returning an empty slice with no
// error means "nothing new this cycle".
type Source interface {
	Fetch(ctx context.Context) ([]logrecord.LogRecord, error)
}
