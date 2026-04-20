// Package factory builds concrete Sink implementations from config entries.
// Lives in its own package to avoid an import cycle: the factory imports every
// concrete sink sub-package, each of which imports the parent `sink` package
// for PermanentError / Sink.
package factory

import (
	"context"
	"fmt"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/avinash-gupta-rdz/rdstail/internal/awsx"
	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
	httpsink "github.com/avinash-gupta-rdz/rdstail/internal/sink/http"
	kafkasink "github.com/avinash-gupta-rdz/rdstail/internal/sink/kafka"
	s3sink "github.com/avinash-gupta-rdz/rdstail/internal/sink/s3"
	"github.com/avinash-gupta-rdz/rdstail/internal/state"
)

// BuildAll instantiates all configured sinks. Decorator order, innermost-first:
// metrics → retry → DLQ (so retries count in metrics, and DLQ only sees
// post-retry terminal failures).
//
// Either probe or dlq may be nil — the corresponding decorator is skipped.
// Caller owns Close() on every returned sink.
func BuildAll(ctx context.Context, cfg *config.Config, dlq state.DLQ, probe sink.MetricsProbe) ([]sink.Sink, error) {
	out := make([]sink.Sink, 0, len(cfg.Sinks))
	for i := range cfg.Sinks {
		cfgSink := &cfg.Sinks[i]
		s, err := buildOne(ctx, cfgSink)
		if err != nil {
			for _, ss := range out {
				_ = ss.Close()
			}
			return nil, err
		}
		wrapped := sink.WithMetrics(s, probe)
		wrapped = sink.WithRetry(wrapped, sink.RetryConfig{
			MaxAttempts: cfgSink.Retry.MaxAttempts,
			InitialWait: cfgSink.Retry.InitialWait,
			MaxWait:     cfgSink.Retry.MaxWait,
			Multiplier:  cfgSink.Retry.Multiplier,
		})
		wrapped = sink.WithDLQ(wrapped, dlq)
		out = append(out, wrapped)
	}
	return out, nil
}

func buildOne(ctx context.Context, cfgSink *config.Sink) (sink.Sink, error) {
	switch cfgSink.Type {
	case config.SinkTypeS3:
		if cfgSink.S3 == nil {
			return nil, fmt.Errorf("sink %q: s3 config required", cfgSink.Name)
		}
		region := cfgSink.S3.Region
		if region == "" {
			region = "us-east-1"
		}
		awsCfg, err := awsx.NewConfig(ctx, awsx.Options{Region: region})
		if err != nil {
			return nil, fmt.Errorf("sink %q: aws config: %w", cfgSink.Name, err)
		}
		client := awss3.NewFromConfig(awsCfg)
		return s3sink.New(s3sink.Opts{Name: cfgSink.Name, Cfg: cfgSink.S3, API: client})

	case config.SinkTypeHTTP:
		return httpsink.New(cfgSink.Name, cfgSink.HTTP)

	case config.SinkTypeKafka:
		return kafkasink.New(kafkasink.Opts{Name: cfgSink.Name, Cfg: cfgSink.Kafka})

	default:
		return nil, fmt.Errorf("sink %q: unsupported type %q", cfgSink.Name, cfgSink.Type)
	}
}
