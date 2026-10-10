package pipeline

import (
	"testing"
	"time"
)

func TestNextPollDelay_AdaptiveBackoffAndReset(t *testing.T) {
	w := &InstanceWorker{
		pollInterval:    10 * time.Second,
		pollIntervalMax: 60 * time.Second,
		pollMultiplier:  2.0,
	}

	// Idle polls grow exponentially and cap at pollIntervalMax.
	cases := []struct {
		current time.Duration
		want    time.Duration
	}{
		{10 * time.Second, 20 * time.Second},
		{20 * time.Second, 40 * time.Second},
		{40 * time.Second, 60 * time.Second}, // 80s capped to 60s
		{60 * time.Second, 60 * time.Second},
	}
	for _, c := range cases {
		if got := w.nextPollDelay(c.current, false, false); got != c.want {
			t.Errorf("idle from %v: got %v, want %v", c.current, got, c.want)
		}
	}

	// Any activity snaps back to the base interval, from any height.
	if got := w.nextPollDelay(60*time.Second, true, false); got != 10*time.Second {
		t.Errorf("active reset: got %v, want 10s", got)
	}
}

func TestNextPollDelay_FixedWhenMaxUnset(t *testing.T) {
	w := &InstanceWorker{pollInterval: 10 * time.Second, pollMultiplier: 2.0}
	if got := w.nextPollDelay(10*time.Second, false, false); got != 10*time.Second {
		t.Errorf("fixed mode idle: got %v, want 10s", got)
	}
	if got := w.nextPollDelay(10*time.Second, true, false); got != 10*time.Second {
		t.Errorf("fixed mode active: got %v, want 10s", got)
	}
}
