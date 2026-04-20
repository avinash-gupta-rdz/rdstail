// Package s3 is the S3 Sink. Each batch is serialised as gzipped NDJSON and
// PUT as a single object. Key format:
//
//	{prefix}/{instance}/{engine}/{logfile-basename}/{YYYY/MM/DD}/{unix-ms}-{batch_id}.ndjson.gz
package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// S3API is the narrow interface over the SDK v2 S3 client used by this sink.
// Defining our own keeps tests mock-friendly.
type S3API interface {
	PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

// Ensure SDK client satisfies the narrow interface at compile time.
var _ S3API = (*awss3.Client)(nil)

// Sink uploads NDJSON+gzip batches to a configured S3 bucket.
type Sink struct {
	name     string
	bucket   string
	prefix   string
	kmsKeyID string
	api      S3API
	clock    func() time.Time
}

// Opts configure New.
type Opts struct {
	Name   string
	Cfg    *config.S3Sink
	API    S3API
	Clock  func() time.Time // optional, defaults to time.Now
}

// New constructs an S3 Sink.
func New(opts Opts) (*Sink, error) {
	if opts.Cfg == nil {
		return nil, fmt.Errorf("s3 sink %q: config required", opts.Name)
	}
	if opts.Cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 sink %q: bucket required", opts.Name)
	}
	if opts.API == nil {
		return nil, fmt.Errorf("s3 sink %q: api client required", opts.Name)
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Sink{
		name:     opts.Name,
		bucket:   opts.Cfg.Bucket,
		prefix:   strings.TrimSuffix(opts.Cfg.Prefix, "/"),
		kmsKeyID: opts.Cfg.KMSKeyID,
		api:      opts.API,
		clock:    clock,
	}, nil
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Type implements sink.Sink.
func (*Sink) Type() string { return "s3" }

// Close implements sink.Sink. No long-lived resources.
func (*Sink) Close() error { return nil }

// Write PUTs records as a single gzipped NDJSON object.
func (s *Sink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	if len(records) == 0 {
		return nil
	}
	body, err := encodeNDJSONGzip(records)
	if err != nil {
		return fmt.Errorf("s3 sink %q encode: %w", s.name, err)
	}
	key := s.buildKey(records[0])

	in := &awss3.PutObjectInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(body),
		ContentType:     aws.String("application/x-ndjson"),
		ContentEncoding: aws.String("gzip"),
		Metadata: map[string]string{
			"batch-id":    records[0].BatchID,
			"instance-id": records[0].InstanceID,
			"log-file":    records[0].LogFile,
			"engine":      records[0].Engine,
		},
	}
	if s.kmsKeyID != "" {
		in.ServerSideEncryption = s3types.ServerSideEncryptionAwsKms
		in.SSEKMSKeyId = aws.String(s.kmsKeyID)
	} else {
		in.ServerSideEncryption = s3types.ServerSideEncryptionAes256
	}

	if _, err := s.api.PutObject(ctx, in); err != nil {
		return fmt.Errorf("s3 sink %q put: %w", s.name, err)
	}
	return nil
}

func (s *Sink) buildKey(sample logrecord.LogRecord) string {
	now := s.clock().UTC()
	base := path.Base(sample.LogFile)
	if base == "." || base == "/" || base == "" {
		base = "log"
	}
	// Trim any / in instance/engine to keep key sane.
	instance := strings.ReplaceAll(sample.InstanceID, "/", "_")
	engine := sample.Engine
	if engine == "" {
		engine = "unknown"
	}
	parts := []string{
		instance, engine, base,
		fmt.Sprintf("%04d/%02d/%02d", now.Year(), int(now.Month()), now.Day()),
		fmt.Sprintf("%d-%s.ndjson.gz", now.UnixMilli(), sample.BatchID),
	}
	p := strings.Join(parts, "/")
	if s.prefix == "" {
		return p
	}
	return s.prefix + "/" + p
}

func encodeNDJSONGzip(records []logrecord.LogRecord) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gw)
	for i := range records {
		if err := enc.Encode(&records[i]); err != nil {
			_ = gw.Close()
			return nil, err
		}
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
