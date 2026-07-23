// Package stdout is the pipe-integration Sink: it writes records to standard
// output (NDJSON by default), one line per record, flushed per batch. rdstail's
// own structured logs go to stderr, so stdout stays clean for data — pipe it
// into vector, fluent-bit, jq, or anything that reads lines.
//
// ACK semantics: Write returns nil once the batch is flushed to the writer.
// For a pipe that means the consumer's buffer accepted it — the strongest
// guarantee a pipe can give. If the downstream process dies, writes fail
// (EPIPE) and the pipeline's checkpoint stops advancing.
package stdout

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

const (
	// FormatNDJSON emits the full LogRecord as one JSON object per line.
	FormatNDJSON = "ndjson"
	// FormatText emits only the raw message line (what `tail -f` would show).
	FormatText = "text"
)

// Sink writes records to an io.Writer, one line per record.
type Sink struct {
	name   string
	format string

	mu sync.Mutex
	w  *bufio.Writer
}

// New constructs a stdout sink. A nil writer defaults to os.Stdout; tests
// inject a buffer. Format "" defaults to ndjson.
func New(name, format string, w io.Writer) (*Sink, error) {
	switch format {
	case "", FormatNDJSON:
		format = FormatNDJSON
	case FormatText:
	default:
		return nil, fmt.Errorf("stdout sink %q: unknown format %q (ndjson|text)", name, format)
	}
	if w == nil {
		w = os.Stdout
	}
	return &Sink{name: name, format: format, w: bufio.NewWriter(w)}, nil
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Type implements sink.Sink.
func (*Sink) Type() string { return "stdout" }

// Close flushes any buffered output.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Flush()
}

// Write emits one line per record and flushes the batch.
func (s *Sink) Write(_ context.Context, records []logrecord.LogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range records {
		var err error
		if s.format == FormatText {
			_, err = s.w.WriteString(records[i].Message)
		} else {
			var b []byte
			if b, err = json.Marshal(&records[i]); err == nil {
				_, err = s.w.Write(b)
			}
		}
		if err == nil {
			err = s.w.WriteByte('\n')
		}
		if err != nil {
			return fmt.Errorf("stdout sink %q: %w", s.name, err)
		}
	}
	if err := s.w.Flush(); err != nil {
		return fmt.Errorf("stdout sink %q flush: %w", s.name, err)
	}
	return nil
}
