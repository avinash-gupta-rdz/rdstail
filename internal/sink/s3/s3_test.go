package s3_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	s3sink "github.com/avinash-gupta-rdz/rdstail/internal/sink/s3"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

type mockS3 struct {
	mu    sync.Mutex
	calls []*awss3.PutObjectInput
	body  []byte
}

func (m *mockS3) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, in)
	if in.Body != nil {
		b, _ := io.ReadAll(in.Body)
		m.body = b
	}
	return &awss3.PutObjectOutput{}, nil
}

func recs() []logrecord.LogRecord {
	ts := time.Unix(0, 0)
	return []logrecord.LogRecord{
		{InstanceID: "db-1", Engine: "postgres", LogFile: "error/postgresql.log", Timestamp: ts, Message: "line 1", BatchID: "b1"},
		{InstanceID: "db-1", Engine: "postgres", LogFile: "error/postgresql.log", Timestamp: ts, Message: "line 2", BatchID: "b1"},
	}
}

func TestWrite_PutsGzippedNDJSON(t *testing.T) {
	m := &mockS3{}
	clock := func() time.Time { return time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC) }
	s, err := s3sink.New(s3sink.Opts{
		Name: "s3-primary",
		Cfg:  &config.S3Sink{Bucket: "my-bucket", Prefix: "rds/"},
		API:  m, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(context.Background(), recs()); err != nil {
		t.Fatal(err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("expected 1 put, got %d", len(m.calls))
	}
	in := m.calls[0]
	if aws.ToString(in.Bucket) != "my-bucket" {
		t.Fatalf("bucket mismatch: %s", aws.ToString(in.Bucket))
	}
	if aws.ToString(in.ContentType) != "application/x-ndjson" {
		t.Fatalf("content-type mismatch: %s", aws.ToString(in.ContentType))
	}
	if aws.ToString(in.ContentEncoding) != "gzip" {
		t.Fatalf("content-encoding mismatch: %s", aws.ToString(in.ContentEncoding))
	}
	if in.ServerSideEncryption != s3types.ServerSideEncryptionAes256 {
		t.Fatalf("expected default SSE AES256, got %s", in.ServerSideEncryption)
	}
	// Key sanity: has prefix, includes date path, ends in .ndjson.gz
	key := aws.ToString(in.Key)
	if !strings.HasPrefix(key, "rds/db-1/postgres/postgresql.log/2026/04/19/") {
		t.Fatalf("unexpected key path: %s", key)
	}
	if !strings.HasSuffix(key, "-b1.ndjson.gz") {
		t.Fatalf("unexpected key suffix: %s", key)
	}

	// Decode body and verify NDJSON contents.
	gr, err := gzip.NewReader(bytes.NewReader(m.body))
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()
	body, err := io.ReadAll(gr)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %d: %s", len(lines), body)
	}
	var first logrecord.LogRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Message != "line 1" {
		t.Fatalf("bad first record: %+v", first)
	}
}

func TestWrite_KMS(t *testing.T) {
	m := &mockS3{}
	s, _ := s3sink.New(s3sink.Opts{
		Name: "s3",
		Cfg:  &config.S3Sink{Bucket: "b", KMSKeyID: "arn:aws:kms:..."},
		API:  m,
	})
	_ = s.Write(context.Background(), recs())
	if m.calls[0].ServerSideEncryption != s3types.ServerSideEncryptionAwsKms {
		t.Fatalf("expected KMS SSE, got %s", m.calls[0].ServerSideEncryption)
	}
	if aws.ToString(m.calls[0].SSEKMSKeyId) != "arn:aws:kms:..." {
		t.Fatalf("kms key not set: %s", aws.ToString(m.calls[0].SSEKMSKeyId))
	}
}

func TestWrite_EmptyRecords_NoPut(t *testing.T) {
	m := &mockS3{}
	s, _ := s3sink.New(s3sink.Opts{Name: "s3", Cfg: &config.S3Sink{Bucket: "b"}, API: m})
	if err := s.Write(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(m.calls) != 0 {
		t.Fatalf("expected 0 puts for empty batch, got %d", len(m.calls))
	}
}
