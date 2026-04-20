// Package validate implements the network-level `validate --deep` probes:
// STS identity, RDS describe-probe per instance, S3 HeadBucket, HTTP HEAD, and
// state-store open+close. Kafka metadata probing is left for a future phase
// since it requires the real broker client with TLS/SASL plumbing.
package validate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"

	// register backends
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
	_ "github.com/avinash-gupta-rdz/rdstail/internal/state/sqlite"
)

// Result is one check outcome. Empty Err means success.
type Result struct {
	Name string
	Err  error
}

func (r Result) OK() bool { return r.Err == nil }

// Deep runs all deep probes and returns per-check results. The caller decides
// how to render them (and whether any failures should cause non-zero exit).
func Deep(ctx context.Context, cfg *config.Config) []Result {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	var results []Result
	results = append(results, probeStateStore(ctx, cfg))
	results = append(results, probeSTS(ctx, cfg)...)
	results = append(results, probeRDSInstances(ctx, cfg)...)
	results = append(results, probeSinks(ctx, cfg)...)
	return results
}

func probeStateStore(ctx context.Context, cfg *config.Config) Result {
	name := "state." + cfg.State.Type
	s, err := state.Open(ctx, state.Config{Type: cfg.State.Type, Path: cfg.State.Path})
	if err != nil {
		return Result{Name: name, Err: fmt.Errorf("open: %w", err)}
	}
	defer s.Close()
	// probe Get on a nonsense key to exercise a read path.
	if _, _, err := s.Get(ctx, "__probe__", "__probe__"); err != nil {
		return Result{Name: name, Err: fmt.Errorf("get probe: %w", err)}
	}
	return Result{Name: name}
}

func probeSTS(ctx context.Context, cfg *config.Config) []Result {
	// One STS call per unique (region, assume_role) — the same dedupe app.go does.
	type key struct{ region, role string }
	seen := map[key]bool{}
	var out []Result
	for _, src := range cfg.Sources {
		k := key{src.Region, src.AssumeRole}
		if seen[k] {
			continue
		}
		seen[k] = true
		name := fmt.Sprintf("sts.GetCallerIdentity[%s%s]", src.Region, roleSuffix(src.AssumeRole))
		awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: src.Region, AssumeRole: src.AssumeRole})
		if err != nil {
			out = append(out, Result{Name: name, Err: err})
			continue
		}
		client := sts.NewFromConfig(awsCfg)
		if _, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err != nil {
			out = append(out, Result{Name: name, Err: err})
			continue
		}
		out = append(out, Result{Name: name})
	}
	return out
}

func probeRDSInstances(ctx context.Context, cfg *config.Config) []Result {
	type key struct{ region, role string }
	clients := map[key]*awsrds.Client{}

	var out []Result
	for _, src := range cfg.Sources {
		k := key{src.Region, src.AssumeRole}
		client, ok := clients[k]
		if !ok {
			awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: src.Region, AssumeRole: src.AssumeRole})
			if err != nil {
				for _, inst := range src.Instances {
					out = append(out, Result{Name: "rds." + inst, Err: err})
				}
				continue
			}
			client = awsrds.NewFromConfig(awsCfg)
			clients[k] = client
		}
		for _, inst := range src.Instances {
			name := "rds.DescribeDBLogFiles[" + inst + "]"
			_, err := client.DescribeDBLogFiles(ctx, &awsrds.DescribeDBLogFilesInput{
				DBInstanceIdentifier: aws.String(inst),
			})
			out = append(out, Result{Name: name, Err: err})
		}
	}
	return out
}

func probeSinks(ctx context.Context, cfg *config.Config) []Result {
	var out []Result
	for i := range cfg.Sinks {
		s := &cfg.Sinks[i]
		switch s.Type {
		case config.SinkTypeS3:
			out = append(out, probeS3(ctx, s))
		case config.SinkTypeHTTP:
			out = append(out, probeHTTP(ctx, s))
		case config.SinkTypeKafka:
			// Kafka deep probe requires a full client spin-up; skipped for v1.
			out = append(out, Result{Name: "sink." + s.Name, Err: errors.New("kafka deep probe: not implemented (v1)")})
		}
	}
	return out
}

func probeS3(ctx context.Context, s *config.Sink) Result {
	name := "sink.s3[" + s.Name + "].HeadBucket"
	if s.S3 == nil {
		return Result{Name: name, Err: errors.New("missing s3 config")}
	}
	region := s.S3.Region
	if region == "" {
		region = "us-east-1"
	}
	awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region})
	if err != nil {
		return Result{Name: name, Err: err}
	}
	client := awss3.NewFromConfig(awsCfg)
	_, err = client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(s.S3.Bucket)})
	return Result{Name: name, Err: err}
}

func probeHTTP(ctx context.Context, s *config.Sink) Result {
	name := "sink.http[" + s.Name + "].HEAD"
	if s.HTTP == nil {
		return Result{Name: name, Err: errors.New("missing http config")}
	}
	timeout := s.HTTP.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, s.HTTP.URL, nil)
	if err != nil {
		return Result{Name: name, Err: err}
	}
	for k, v := range s.HTTP.Headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Name: name, Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return Result{Name: name, Err: fmt.Errorf("status %d", resp.StatusCode)}
	}
	return Result{Name: name}
}

func roleSuffix(role string) string {
	if role == "" {
		return ""
	}
	return " via " + strings.TrimPrefix(role, "arn:aws:iam::")
}
