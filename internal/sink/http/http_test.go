package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	httpsink "github.com/avinash-gupta-rdz/rdstail/internal/sink/http"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

func sampleRecords() []logrecord.LogRecord {
	return []logrecord.LogRecord{
		{InstanceID: "db-1", Engine: "postgres", LogFile: "err.log", Timestamp: time.Unix(0, 0), Message: "line 1", BatchID: "b1"},
		{InstanceID: "db-1", Engine: "postgres", LogFile: "err.log", Timestamp: time.Unix(0, 0), Message: "line 2", BatchID: "b1"},
	}
}

func TestWrite_Success_SetsHeadersAndBody(t *testing.T) {
	var gotBody []byte
	var gotHdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHdr = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s, err := httpsink.New("webhook", &config.HTTPSink{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer xyz"},
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(context.Background(), sampleRecords()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if gotHdr.Get("Authorization") != "Bearer xyz" {
		t.Fatalf("bad auth: %q", gotHdr.Get("Authorization"))
	}
	if gotHdr.Get("X-Batch-Id") != "b1" {
		t.Fatalf("bad batch id: %q", gotHdr.Get("X-Batch-Id"))
	}
	if gotHdr.Get("Content-Type") != "application/json" {
		t.Fatalf("bad content type: %q", gotHdr.Get("Content-Type"))
	}
	var parsed []logrecord.LogRecord
	if err := json.Unmarshal(gotBody, &parsed); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, gotBody)
	}
	if len(parsed) != 2 || parsed[0].Message != "line 1" {
		t.Fatalf("bad body: %+v", parsed)
	}
}

func TestWrite_Gzip_SetsContentEncoding(t *testing.T) {
	var gotEnc string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEnc = r.Header.Get("Content-Encoding")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	s, _ := httpsink.New("webhook", &config.HTTPSink{URL: srv.URL, GZIP: true, Timeout: 5 * time.Second})
	if err := s.Write(context.Background(), sampleRecords()); err != nil {
		t.Fatal(err)
	}
	if gotEnc != "gzip" {
		t.Fatalf("expected Content-Encoding=gzip, got %q", gotEnc)
	}
}

func TestWrite_4xx_IsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	s, _ := httpsink.New("webhook", &config.HTTPSink{URL: srv.URL, Timeout: 5 * time.Second})
	err := s.Write(context.Background(), sampleRecords())
	if err == nil {
		t.Fatal("expected error on 400")
	}
	var perm *sink.PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("expected PermanentError, got %T: %v", err, err)
	}
}

func TestWrite_5xx_IsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	s, _ := httpsink.New("webhook", &config.HTTPSink{URL: srv.URL, Timeout: 5 * time.Second})
	err := s.Write(context.Background(), sampleRecords())
	if err == nil {
		t.Fatal("expected error on 503")
	}
	var perm *sink.PermanentError
	if errors.As(err, &perm) {
		t.Fatalf("503 must not be permanent")
	}
}

func TestWrite_EmptyRecords_NoRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()
	s, _ := httpsink.New("webhook", &config.HTTPSink{URL: srv.URL, Timeout: 5 * time.Second})
	if err := s.Write(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 0 {
		t.Fatalf("expected 0 requests for empty batch, got %d", hits.Load())
	}
}
