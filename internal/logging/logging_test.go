package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewEmitsJSONAtLevel(t *testing.T) {
	var buf bytes.Buffer
	lg := New(&buf, "warn")

	lg.Info("suppressed")
	lg.Warn("kept", "k", "v")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("expected one JSON record, got %q: %v", buf.String(), err)
	}
	if rec["msg"] != "kept" || rec["level"] != "WARN" || rec["k"] != "v" {
		t.Fatalf("unexpected record: %#v", rec)
	}
}

func TestParseLevelUnknownFallsToInfo(t *testing.T) {
	if got := parseLevel("gibberish"); got.String() != "INFO" {
		t.Fatalf("want INFO, got %s", got)
	}
	if got := parseLevel("DEBUG"); got.String() != "DEBUG" {
		t.Fatalf("want DEBUG, got %s", got)
	}
}
