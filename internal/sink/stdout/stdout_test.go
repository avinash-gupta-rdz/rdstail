package stdout_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/sink/stdout"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

func TestNDJSON_OneObjectPerLine(t *testing.T) {
	var buf bytes.Buffer
	s, err := stdout.New("out", "", &buf)
	if err != nil {
		t.Fatal(err)
	}
	recs := []logrecord.LogRecord{
		{InstanceID: "db-1", Severity: "ERROR", Message: "boom", Timestamp: time.Unix(1, 0).UTC(), BatchID: "b1"},
		{InstanceID: "db-1", Message: "plain line", Timestamp: time.Unix(2, 0).UTC(), BatchID: "b1"},
	}
	if err := s.Write(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), buf.String())
	}
	var got logrecord.LogRecord
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("line 0 not JSON: %v", err)
	}
	if got.Severity != "ERROR" || got.Message != "boom" {
		t.Fatalf("bad round-trip: %+v", got)
	}
}

func TestText_RawMessagesOnly(t *testing.T) {
	var buf bytes.Buffer
	s, err := stdout.New("out", stdout.FormatText, &buf)
	if err != nil {
		t.Fatal(err)
	}
	recs := []logrecord.LogRecord{{Message: "first"}, {Message: "second"}}
	if err := s.Write(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "first\nsecond\n" {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestNew_RejectsUnknownFormat(t *testing.T) {
	if _, err := stdout.New("out", "xml", nil); err == nil {
		t.Fatal("expected error for unknown format")
	}
}
