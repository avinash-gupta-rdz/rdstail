// Package config owns the YAML schema, loading, and static validation for rdstail.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config is the top-level on-disk schema.
type Config struct {
	Sources  []Source  `koanf:"sources" yaml:"sources"`
	Sinks    []Sink    `koanf:"sinks" yaml:"sinks"`
	State    State     `koanf:"state" yaml:"state"`
	Runtime  Runtime   `koanf:"runtime" yaml:"runtime"`
	Metrics  Metrics   `koanf:"metrics" yaml:"metrics"`
	Logging  Logging   `koanf:"logging" yaml:"logging"`
}

// Source describes an ingestion source. Only type=rds is supported in v1.
type Source struct {
	Type       string   `koanf:"type" yaml:"type"`
	Engine     string   `koanf:"engine" yaml:"engine"`
	Region     string   `koanf:"region" yaml:"region"`
	Instances  []string `koanf:"instances" yaml:"instances"`
	AssumeRole string   `koanf:"assume_role" yaml:"assume_role,omitempty"`
}

// Sink describes an output destination. Only one of the nested blocks is populated
// depending on Type.
type Sink struct {
	Name    string      `koanf:"name" yaml:"name"`
	Type    string      `koanf:"type" yaml:"type"`
	Retry   RetryPolicy `koanf:"retry" yaml:"retry"`
	S3      *S3Sink     `koanf:"s3" yaml:"s3,omitempty"`
	Kafka   *KafkaSink  `koanf:"kafka" yaml:"kafka,omitempty"`
	HTTP    *HTTPSink   `koanf:"http" yaml:"http,omitempty"`
}

type S3Sink struct {
	Bucket     string        `koanf:"bucket" yaml:"bucket"`
	Prefix     string        `koanf:"prefix" yaml:"prefix"`
	Region     string        `koanf:"region" yaml:"region"`
	KMSKeyID   string        `koanf:"kms_key_id" yaml:"kms_key_id,omitempty"`
	MaxBytes   int           `koanf:"max_bytes" yaml:"max_bytes"`
	MaxRecords int           `koanf:"max_records" yaml:"max_records"`
	MaxAge     time.Duration `koanf:"max_age" yaml:"max_age"`
}

type KafkaSink struct {
	Brokers       []string `koanf:"brokers" yaml:"brokers"`
	Topic         string   `koanf:"topic" yaml:"topic"`
	TopicTemplate string   `koanf:"topic_template" yaml:"topic_template,omitempty"`
	ClientID      string   `koanf:"client_id" yaml:"client_id,omitempty"`
	TLS           bool     `koanf:"tls" yaml:"tls"`
	SASLUsername  string   `koanf:"sasl_username" yaml:"sasl_username,omitempty"`
	SASLPassword  string   `koanf:"sasl_password" yaml:"sasl_password,omitempty"`
}

type HTTPSink struct {
	URL     string            `koanf:"url" yaml:"url"`
	Headers map[string]string `koanf:"headers" yaml:"headers,omitempty"`
	GZIP    bool              `koanf:"gzip" yaml:"gzip"`
	Timeout time.Duration     `koanf:"timeout" yaml:"timeout"`
}

type RetryPolicy struct {
	MaxAttempts int           `koanf:"max_attempts" yaml:"max_attempts"`
	InitialWait time.Duration `koanf:"initial_wait" yaml:"initial_wait"`
	MaxWait     time.Duration `koanf:"max_wait" yaml:"max_wait"`
	Multiplier  float64       `koanf:"multiplier" yaml:"multiplier"`
}

type State struct {
	Type string `koanf:"type" yaml:"type"` // "sqlite" | "file"
	Path string `koanf:"path" yaml:"path"`
}

type Runtime struct {
	PollInterval           time.Duration `koanf:"poll_interval" yaml:"poll_interval"`
	MaxWorkers             int           `koanf:"max_workers" yaml:"max_workers"`
	MaxInstancesConcurrent int           `koanf:"max_instances_concurrent" yaml:"max_instances_concurrent"`
	ShutdownTimeout        time.Duration `koanf:"shutdown_timeout" yaml:"shutdown_timeout"`
	StartFrom              string        `koanf:"start_from" yaml:"start_from"` // "beginning" | "end"
	MemoryBudgetBytes      int64         `koanf:"memory_budget_bytes" yaml:"memory_budget_bytes"`
}

type Metrics struct {
	Enabled bool   `koanf:"enabled" yaml:"enabled"`
	Listen  string `koanf:"listen" yaml:"listen"`
}

type Logging struct {
	Level string `koanf:"level" yaml:"level"`
}

const (
	EngineMySQL    = "mysql"
	EngineMariaDB  = "mariadb"
	EnginePostgres = "postgres"

	SourceTypeRDS = "rds"

	SinkTypeS3    = "s3"
	SinkTypeKafka = "kafka"
	SinkTypeHTTP  = "http"

	StateTypeSQLite = "sqlite"
	StateTypeFile   = "file"

	StartFromBeginning = "beginning"
	StartFromEnd       = "end"
)

// Load reads and parses a YAML config file. Environment variable overrides use the
// prefix RDSTAIL_ and double-underscore as the nesting separator
// (e.g. RDSTAIL_RUNTIME__POLL_INTERVAL=5s).
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path is required")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("stat config: %w", err)
	}

	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	// Env overrides: RDSTAIL_FOO__BAR → foo.bar
	envProv := env.Provider("RDSTAIL_", ".", func(s string) string {
		trimmed := strings.TrimPrefix(s, "RDSTAIL_")
		return strings.ReplaceAll(strings.ToLower(trimmed), "__", ".")
	})
	if err := k.Load(envProv, nil); err != nil {
		return nil, fmt.Errorf("load env: %w", err)
	}

	cfg := defaults()
	if err := k.UnmarshalWithConf("", cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	applyPostDefaults(cfg)
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		State: State{Type: StateTypeSQLite, Path: "./state.db"},
		Runtime: Runtime{
			PollInterval:           10 * time.Second,
			MaxWorkers:             5,
			ShutdownTimeout:        30 * time.Second,
			StartFrom:              StartFromEnd,
			MemoryBudgetBytes:      256 * 1024 * 1024,
		},
		Metrics: Metrics{Enabled: true, Listen: ":9090"},
		Logging: Logging{Level: "info"},
	}
}

func applyPostDefaults(c *Config) {
	for i := range c.Sinks {
		s := &c.Sinks[i]
		if s.Retry.MaxAttempts == 0 {
			s.Retry.MaxAttempts = 10
		}
		if s.Retry.InitialWait == 0 {
			s.Retry.InitialWait = 500 * time.Millisecond
		}
		if s.Retry.MaxWait == 0 {
			s.Retry.MaxWait = 60 * time.Second
		}
		if s.Retry.Multiplier == 0 {
			s.Retry.Multiplier = 2.0
		}
		if s.S3 != nil {
			if s.S3.MaxBytes == 0 {
				s.S3.MaxBytes = 5 * 1024 * 1024
			}
			if s.S3.MaxRecords == 0 {
				s.S3.MaxRecords = 10_000
			}
			if s.S3.MaxAge == 0 {
				s.S3.MaxAge = 30 * time.Second
			}
		}
		if s.HTTP != nil && s.HTTP.Timeout == 0 {
			s.HTTP.Timeout = 30 * time.Second
		}
	}
	if c.Runtime.MaxInstancesConcurrent == 0 {
		total := 0
		for _, src := range c.Sources {
			total += len(src.Instances)
		}
		if total == 0 {
			total = 1
		}
		c.Runtime.MaxInstancesConcurrent = total
	}
}

// Validate runs static (non-network) validation on c. Every failing rule is
// collected and returned as a single joined error.
func Validate(c *Config) error {
	var errs []error

	if len(c.Sources) == 0 {
		errs = append(errs, errors.New("sources: at least one source is required"))
	}
	for i, src := range c.Sources {
		prefix := fmt.Sprintf("sources[%d]", i)
		if src.Type != SourceTypeRDS {
			errs = append(errs, fmt.Errorf("%s.type: only %q is supported", prefix, SourceTypeRDS))
		}
		if !isValidEngine(src.Engine) {
			errs = append(errs, fmt.Errorf("%s.engine: must be one of postgres, mysql, mariadb (got %q)", prefix, src.Engine))
		}
		if strings.TrimSpace(src.Region) == "" {
			errs = append(errs, fmt.Errorf("%s.region: required", prefix))
		}
		if len(src.Instances) == 0 {
			errs = append(errs, fmt.Errorf("%s.instances: at least one instance required", prefix))
		}
	}

	if len(c.Sinks) == 0 {
		errs = append(errs, errors.New("sinks: at least one sink is required"))
	}
	seenNames := map[string]struct{}{}
	for i, s := range c.Sinks {
		prefix := fmt.Sprintf("sinks[%d]", i)
		if strings.TrimSpace(s.Name) == "" {
			errs = append(errs, fmt.Errorf("%s.name: required", prefix))
		} else if _, dup := seenNames[s.Name]; dup {
			errs = append(errs, fmt.Errorf("%s.name: duplicate sink name %q", prefix, s.Name))
		} else {
			seenNames[s.Name] = struct{}{}
		}
		errs = append(errs, validateSinkShape(prefix, s)...)
	}

	if c.Runtime.PollInterval < time.Second {
		errs = append(errs, errors.New("runtime.poll_interval: must be >= 1s"))
	}
	if c.Runtime.MaxWorkers < 1 {
		errs = append(errs, errors.New("runtime.max_workers: must be >= 1"))
	}
	if c.Runtime.StartFrom != StartFromBeginning && c.Runtime.StartFrom != StartFromEnd {
		errs = append(errs, fmt.Errorf("runtime.start_from: must be %q or %q", StartFromBeginning, StartFromEnd))
	}

	switch c.State.Type {
	case StateTypeSQLite, StateTypeFile:
	default:
		errs = append(errs, fmt.Errorf("state.type: must be %q or %q", StateTypeSQLite, StateTypeFile))
	}
	if strings.TrimSpace(c.State.Path) == "" {
		errs = append(errs, errors.New("state.path: required"))
	}

	instanceCount := 0
	for _, src := range c.Sources {
		instanceCount += len(src.Instances)
	}
	if instanceCount > 500 {
		errs = append(errs, fmt.Errorf("sources: %d instances exceeds the 500-instance default cap (raise memory_budget_bytes and re-check cardinality before removing)", instanceCount))
	}

	return errors.Join(errs...)
}

func validateSinkShape(prefix string, s Sink) []error {
	var errs []error
	switch s.Type {
	case SinkTypeS3:
		if s.S3 == nil {
			errs = append(errs, fmt.Errorf("%s.s3: required when type=s3", prefix))
			return errs
		}
		if strings.TrimSpace(s.S3.Bucket) == "" {
			errs = append(errs, fmt.Errorf("%s.s3.bucket: required", prefix))
		}
		if strings.TrimSpace(s.S3.Region) == "" {
			errs = append(errs, fmt.Errorf("%s.s3.region: required", prefix))
		}
	case SinkTypeKafka:
		if s.Kafka == nil {
			errs = append(errs, fmt.Errorf("%s.kafka: required when type=kafka", prefix))
			return errs
		}
		if len(s.Kafka.Brokers) == 0 {
			errs = append(errs, fmt.Errorf("%s.kafka.brokers: at least one broker required", prefix))
		}
		if s.Kafka.Topic == "" && s.Kafka.TopicTemplate == "" {
			errs = append(errs, fmt.Errorf("%s.kafka: one of topic or topic_template is required", prefix))
		}
	case SinkTypeHTTP:
		if s.HTTP == nil {
			errs = append(errs, fmt.Errorf("%s.http: required when type=http", prefix))
			return errs
		}
		if strings.TrimSpace(s.HTTP.URL) == "" {
			errs = append(errs, fmt.Errorf("%s.http.url: required", prefix))
		} else if !strings.HasPrefix(s.HTTP.URL, "http://") && !strings.HasPrefix(s.HTTP.URL, "https://") {
			errs = append(errs, fmt.Errorf("%s.http.url: must begin with http:// or https://", prefix))
		}
	default:
		errs = append(errs, fmt.Errorf("%s.type: must be one of s3, kafka, http (got %q)", prefix, s.Type))
	}
	return errs
}

func isValidEngine(e string) bool {
	switch e {
	case EnginePostgres, EngineMySQL, EngineMariaDB:
		return true
	}
	return false
}
