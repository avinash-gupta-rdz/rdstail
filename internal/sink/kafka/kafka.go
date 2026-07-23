// Package kafka is the Kafka Sink, built on franz-go. Each record becomes one
// Kafka message; the partitioning key is `instance|logfile` so a single file's
// records stay on the same partition (preserves intra-file order).
package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

// Producer is the narrow interface we use from franz-go — lets tests inject a mock.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Sink produces batched records to Kafka.
type Sink struct {
	name          string
	topic         string
	topicTemplate string
	producer      Producer
}

// Opts configure New.
type Opts struct {
	Name     string
	Cfg      *config.KafkaSink
	Producer Producer // if nil, New constructs a default franz-go client
}

// New constructs a Kafka sink. If opts.Producer is nil a default franz-go client
// is built from opts.Cfg (brokers, client_id, SASL, TLS).
func New(opts Opts) (*Sink, error) {
	if opts.Cfg == nil {
		return nil, fmt.Errorf("kafka sink %q: config required", opts.Name)
	}
	if len(opts.Cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink %q: brokers required", opts.Name)
	}
	if opts.Cfg.Topic == "" && opts.Cfg.TopicTemplate == "" {
		return nil, fmt.Errorf("kafka sink %q: topic or topic_template required", opts.Name)
	}
	p := opts.Producer
	if p == nil {
		clientOpts, err := ClientOpts(opts.Cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka sink %q: %w", opts.Name, err)
		}
		clientOpts = append(clientOpts,
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
		)
		client, err := kgo.NewClient(clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("kafka sink %q client: %w", opts.Name, err)
		}
		p = client
	}
	return &Sink{
		name:          opts.Name,
		topic:         opts.Cfg.Topic,
		topicTemplate: opts.Cfg.TopicTemplate,
		producer:      p,
	}, nil
}

// ClientOpts builds the connection-level franz-go options (brokers, client id,
// TLS, SASL) shared by the producer and the `validate --deep` probe. Producer
// concerns (acks, compression) are NOT included — append those separately.
func ClientOpts(cfg *config.KafkaSink) ([]kgo.Opt, error) {
	out := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.ClientID != "" {
		out = append(out, kgo.ClientID(cfg.ClientID))
	}
	if cfg.TLS {
		out = append(out, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if cfg.SASLUsername != "" {
		switch cfg.SASLMechanism {
		case "", "plain":
			out = append(out, kgo.SASL(plain.Auth{
				User: cfg.SASLUsername, Pass: cfg.SASLPassword,
			}.AsMechanism()))
		case "scram-sha-256":
			out = append(out, kgo.SASL(scram.Auth{
				User: cfg.SASLUsername, Pass: cfg.SASLPassword,
			}.AsSha256Mechanism()))
		case "scram-sha-512":
			out = append(out, kgo.SASL(scram.Auth{
				User: cfg.SASLUsername, Pass: cfg.SASLPassword,
			}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported sasl_mechanism %q", cfg.SASLMechanism)
		}
	}
	return out, nil
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return s.name }

// Type implements sink.Sink.
func (*Sink) Type() string { return "kafka" }

// Close implements sink.Sink.
func (s *Sink) Close() error {
	s.producer.Close()
	return nil
}

// Write sends each record as a Kafka message. Returns the first producer error encountered.
func (s *Sink) Write(ctx context.Context, records []logrecord.LogRecord) error {
	if len(records) == 0 {
		return nil
	}
	msgs := make([]*kgo.Record, 0, len(records))
	for i := range records {
		r := &records[i]
		body, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("kafka sink %q marshal: %w", s.name, err)
		}
		key := r.InstanceID + "|" + r.LogFile
		msgs = append(msgs, &kgo.Record{
			Topic: s.resolveTopic(r),
			Key:   []byte(key),
			Value: body,
			Headers: []kgo.RecordHeader{
				{Key: "batch-id", Value: []byte(r.BatchID)},
				{Key: "instance-id", Value: []byte(r.InstanceID)},
				{Key: "log-file", Value: []byte(r.LogFile)},
				{Key: "engine", Value: []byte(r.Engine)},
			},
		})
	}

	results := s.producer.ProduceSync(ctx, msgs...)
	var errs []error
	for _, res := range results {
		if res.Err != nil {
			errs = append(errs, res.Err)
		}
	}
	return errors.Join(errs...)
}

func (s *Sink) resolveTopic(r *logrecord.LogRecord) string {
	if s.topic != "" {
		return s.topic
	}
	t := s.topicTemplate
	t = strings.ReplaceAll(t, "{engine}", r.Engine)
	t = strings.ReplaceAll(t, "{instance}", r.InstanceID)
	return t
}
