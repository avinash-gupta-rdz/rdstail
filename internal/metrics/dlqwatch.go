package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// DLQCounter is the one method the depth watcher needs — satisfied by the
// SQLite state store. Kept narrow so tests inject fakes without importing the
// state package.
type DLQCounter interface {
	DLQCount(ctx context.Context, sinkName string) (int64, error)
}

// WatchDLQDepth updates gauge with each sink's parked-batch count every
// interval until ctx is cancelled. Blocking — run in a goroutine. Count errors
// are logged and skipped; the gauge keeps its last good value rather than
// flapping to zero.
func WatchDLQDepth(ctx context.Context, dlq DLQCounter, sinkNames []string, gauge *prometheus.GaugeVec, interval time.Duration, lg *slog.Logger) {
	if dlq == nil || gauge == nil || len(sinkNames) == 0 {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	update := func() {
		for _, name := range sinkNames {
			n, err := dlq.DLQCount(ctx, name)
			if err != nil {
				if ctx.Err() == nil {
					lg.Warn("dlq depth count failed", "sink", name, "err", err)
				}
				continue
			}
			gauge.WithLabelValues(name).Set(float64(n))
		}
	}
	update()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			update()
		}
	}
}
