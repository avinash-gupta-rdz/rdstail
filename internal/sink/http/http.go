// Package http is an HTTP-webhook Sink. Each batch becomes a single
// `POST application/json` of a JSON array of records; gzip is optional.
package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdhttp "net/http"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// Sink posts batches to a configured URL.
type Sink struct {
	name   string
	url    string
	hdr    map[string]string
	gzip   bool
	client *stdhttp.Client
}

// New builds an HTTP sink from the configured parameters. name is the sink's
// operator-facing name; cfg provides URL, headers, gzip flag, and timeout.
func New(name string, cfg *config.HTTPSink) (*Sink, error) {
	if cfg == nil || cfg.URL == "" {
		return nil, fmt.Errorf("http sink %q: url required", name)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Sink{
		name:   name,
		url:    cfg.URL,
		hdr:    cfg.Headers,
		gzip:   cfg.GZIP,
		client: &stdhttp.Client{Timeout: timeout},
	}, nil
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Type implements sink.Sink.
func (*Sink) Type() string { return "http" }

// Close implements sink.Sink. HTTP has no long-lived resources.
func (*Sink) Close() error { return nil }

// Write POSTs records as a JSON array. 2xx = success; 4xx is a PermanentError
// (routed to DLQ by caller); 5xx / network errors are retryable.
func (s *Sink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	if len(records) == 0 {
		return nil
	}
	body, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("http sink %q marshal: %w", s.name, err)
	}
	var reader io.Reader = bytes.NewReader(body)
	var contentEncoding string
	if s.gzip {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(body); err != nil {
			return fmt.Errorf("http sink %q gzip: %w", s.name, err)
		}
		if err := gw.Close(); err != nil {
			return fmt.Errorf("http sink %q gzip close: %w", s.name, err)
		}
		reader = &buf
		contentEncoding = "gzip"
	}

	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, s.url, reader)
	if err != nil {
		return fmt.Errorf("http sink %q new request: %w", s.name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	// Routing/identity headers so the receiver can shard or trace.
	req.Header.Set("X-Batch-Id", records[0].BatchID)
	req.Header.Set("X-Instance-Id", records[0].InstanceID)
	req.Header.Set("X-Log-File", records[0].LogFile)
	for k, v := range s.hdr {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http sink %q post: %w", s.name, err)
	}
	defer resp.Body.Close()
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return sink.NewPermanentError(fmt.Errorf("http sink %q: status %d", s.name, resp.StatusCode))
	default:
		return fmt.Errorf("http sink %q: status %d", s.name, resp.StatusCode)
	}
}
