// Package config owns the YAML schema, loading, and static validation for rdstail.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"

	"github.com/avinash-gupta-rdz/rdstail/internal/sink"
)

// Config is the top-level on-disk schema.
type Config struct {
	Sources []Source `koanf:"sources" yaml:"sources"`
	Sinks   []Sink   `koanf:"sinks" yaml:"sinks"`
	State   State    `koanf:"state" yaml:"state"`
	Runtime Runtime  `koanf:"runtime" yaml:"runtime"`
	Metrics Metrics  `koanf:"metrics" yaml:"metrics"`
	Logging Logging  `koanf:"logging" yaml:"logging"`
}

// Source describes an ingestion source. Only type=rds is supported in v1.
// Instances may be listed explicitly, discovered by tag (Discover), or both —
// the union is ingested, deduplicated by instance ID.
type Source struct {
	Type string `koanf:"type" yaml:"type"`
	// Engine is required when Instances are listed explicitly (the RDS log API
	// does not reveal the engine per log file). Discovered instances get their
	// engine from the DescribeDBInstances response, so a discover-only source
	// may omit it; when set, it additionally filters discovery to that engine.
	Engine     string    `koanf:"engine" yaml:"engine"`
	Region     string    `koanf:"region" yaml:"region"`
	Instances  []string  `koanf:"instances" yaml:"instances"`
	Discover   *Discover `koanf:"discover" yaml:"discover,omitempty"`
	AssumeRole string    `koanf:"assume_role" yaml:"assume_role,omitempty"`
	// IncludeAudit opts MySQL/MariaDB audit-plugin files
	// (audit/server_audit.log*) into ingestion. Off by default — audit logs
	// can be high-volume. No effect for postgres: pgAudit entries live inside
	// postgresql.log and always flow.
	IncludeAudit bool `koanf:"include_audit" yaml:"include_audit,omitempty"`
}

// Discover selects RDS instances by tag at startup instead of (or in addition
// to) an explicit instance list. All listed tags must match (AND semantics).
type Discover struct {
	Tags map[string]string `koanf:"tags" yaml:"tags"`
	// RefreshInterval enables periodic re-discovery: the tag query re-runs on
	// this cadence and workers are started/stopped to match the fleet. 0
	// (default) resolves once at startup.
	RefreshInterval time.Duration `koanf:"refresh_interval" yaml:"refresh_interval,omitempty"`
}

// Sink describes an output destination. Only one of the nested blocks is populated
// depending on Type.
type Sink struct {
	Name  string      `koanf:"name" yaml:"name"`
	Type  string      `koanf:"type" yaml:"type"`
	Retry RetryPolicy `koanf:"retry" yaml:"retry"`
	// Filter narrows which records this sink receives (fanout topologies:
	// archive everything to S3, route only ERROR+ to a webhook). Empty = all.
	Filter *SinkFilter `koanf:"filter" yaml:"filter,omitempty"`
	S3     *S3Sink     `koanf:"s3" yaml:"s3,omitempty"`
	Kafka  *KafkaSink  `koanf:"kafka" yaml:"kafka,omitempty"`
	HTTP   *HTTPSink   `koanf:"http" yaml:"http,omitempty"`
	Stdout *StdoutSink `koanf:"stdout" yaml:"stdout,omitempty"`
}

// StdoutSink emits records to standard output for pipe integration
// (vector, fluent-bit, jq). rdstail's own logs go to stderr, so stdout is
// clean data.
type StdoutSink struct {
	Format string `koanf:"format" yaml:"format,omitempty"` // ndjson (default) | text
}

// SinkFilter selects records by severity. Set at most one field.
type SinkFilter struct {
	// MinSeverity keeps records at or above this level (DEBUG < LOG/INFO/
	// NOTICE/NOTE/SYSTEM < WARNING < ERROR < FATAL < PANIC). Records without a
	// parsed severity are dropped by this mode.
	MinSeverity string `koanf:"min_severity" yaml:"min_severity,omitempty"`
	// Severities keeps only records whose severity matches one of these
	// tokens exactly (case insensitive).
	Severities []string `koanf:"severities" yaml:"severities,omitempty"`
}

type S3Sink struct {
	Bucket string `koanf:"bucket" yaml:"bucket"`
	Prefix string `koanf:"prefix" yaml:"prefix"`
	Region string `koanf:"region" yaml:"region"`
	// AssumeRole writes to a bucket in another account: the ARN of a role in
	// the bucket's account with s3:PutObject on the prefix. ExternalID is the
	// optional condition value for that role's trust policy.
	AssumeRole string        `koanf:"assume_role" yaml:"assume_role,omitempty"`
	ExternalID string        `koanf:"external_id" yaml:"external_id,omitempty"`
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
	// SASLMechanism selects the SASL flavour when sasl_username is set:
	// "plain" (default), "scram-sha-256", or "scram-sha-512".
	SASLMechanism string `koanf:"sasl_mechanism" yaml:"sasl_mechanism,omitempty"`
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
	PollInterval time.Duration `koanf:"poll_interval" yaml:"poll_interval"`
	// PollIntervalMax enables adaptive polling when > PollInterval: idle polls
	// back off exponentially up to this ceiling and snap back to PollInterval
	// as soon as data flows. 0 (default) keeps fixed-interval polling.
	PollIntervalMax time.Duration `koanf:"poll_interval_max" yaml:"poll_interval_max"`
	// PollBackoffMultiplier is the idle-poll growth factor (default 2.0).
	// Only meaningful when PollIntervalMax is set.
	PollBackoffMultiplier float64 `koanf:"poll_backoff_multiplier" yaml:"poll_backoff_multiplier"`
	// MaxBatchBytes / MaxBatchRecords bound how much log data the pipeline
	// coalesces into a single sink write. Chunks pulled within one poll cycle
	// accumulate until a threshold trips (or the file has no more pending
	// data), then flush as one write — fewer, larger S3 objects instead of one
	// object per ~1MB RDS chunk. Checkpoints advance only at flush points, so
	// at-least-once semantics are unchanged.
	MaxBatchBytes   int64 `koanf:"max_batch_bytes" yaml:"max_batch_bytes"`
	MaxBatchRecords int   `koanf:"max_batch_records" yaml:"max_batch_records"`

	// CheckpointRetention enables automatic pruning of checkpoint rows not
	// updated in this long (rotated-out files, departed instances). 0
	// (default) disables the sweep; `rdstail state gc` remains available.
	CheckpointRetention time.Duration `koanf:"checkpoint_retention" yaml:"checkpoint_retention,omitempty"`

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

	SinkTypeS3     = "s3"
	SinkTypeKafka  = "kafka"
	SinkTypeHTTP   = "http"
	SinkTypeStdout = "stdout"

	StateTypeSQLite = "sqlite"
	StateTypeFile   = "file"

	StartFromBeginning = "beginning"
	StartFromEnd       = "end"
)

// Load reads and parses a YAML config file. ${VAR} references anywhere in the
// file are replaced with the environment variable's value before parsing —
// the way secrets (API keys, SASL passwords) stay out of the YAML. References
// to unset variables are left verbatim — the broken reference travels intact
// (and thus greppable/visible at the receiving end) instead of silently
// becoming an empty string. Environment variable overrides use the
// prefix RDSTAIL_ and double-underscore as the nesting separator
// (e.g. RDSTAIL_RUNTIME__POLL_INTERVAL=5s).
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	k := koanf.New(".")
	if err := k.Load(rawbytes.Provider(expandEnvRefs(raw)), yaml.Parser()); err != nil {
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

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnvRefs substitutes ${VAR} with the environment value. Unset
// variables are left as-is; there is no escape syntax — RDS configs have no
// legitimate literal "${NAME}" strings.
func expandEnvRefs(b []byte) []byte {
	return envRef.ReplaceAllFunc(b, func(m []byte) []byte {
		if v, ok := os.LookupEnv(string(m[2 : len(m)-1])); ok {
			return []byte(v)
		}
		return m
	})
}

func defaults() *Config {
	return &Config{
		State: State{Type: StateTypeSQLite, Path: "./state.db"},
		Runtime: Runtime{
			PollInterval:          10 * time.Second,
			PollBackoffMultiplier: 2.0,
			MaxBatchBytes:         5 * 1024 * 1024,
			MaxBatchRecords:       10_000,
			MaxWorkers:            5,
			ShutdownTimeout:       30 * time.Second,
			StartFrom:             StartFromEnd,
			MemoryBudgetBytes:     256 * 1024 * 1024,
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
		// Discover-only sources may omit engine (inferred per instance from
		// AWS); explicit instances require it.
		if src.Engine == "" && src.Discover != nil && len(src.Instances) == 0 {
			// ok
		} else if !isValidEngine(src.Engine) {
			errs = append(errs, fmt.Errorf("%s.engine: must be one of postgres, mysql, mariadb (got %q)", prefix, src.Engine))
		}
		if strings.TrimSpace(src.Region) == "" {
			errs = append(errs, fmt.Errorf("%s.region: required", prefix))
		}
		if len(src.Instances) == 0 && src.Discover == nil {
			errs = append(errs, fmt.Errorf("%s.instances: at least one instance required (or set discover.tags)", prefix))
		}
		if src.Discover != nil && len(src.Discover.Tags) == 0 {
			errs = append(errs, fmt.Errorf("%s.discover.tags: at least one tag required", prefix))
		}
		if src.Discover != nil && src.Discover.RefreshInterval != 0 && src.Discover.RefreshInterval < 30*time.Second {
			errs = append(errs, fmt.Errorf("%s.discover.refresh_interval: must be >= 30s (or 0 for startup-only)", prefix))
		}
	}

	if len(c.Sinks) == 0 {
		errs = append(errs, errors.New("sinks: at least one sink is required"))
	}
	seenNames := map[string]struct{}{}
	for i, s := range c.Sinks {
		prefix := fmt.Sprintf("sinks[%d]", i)
		if s.Filter != nil {
			if s.Filter.MinSeverity != "" && len(s.Filter.Severities) > 0 {
				errs = append(errs, fmt.Errorf("%s.filter: set min_severity or severities, not both", prefix))
			}
			if s.Filter.MinSeverity == "" && len(s.Filter.Severities) == 0 {
				errs = append(errs, fmt.Errorf("%s.filter: min_severity or severities required", prefix))
			}
			if s.Filter.MinSeverity != "" && sink.SeverityRank(s.Filter.MinSeverity) == 0 {
				errs = append(errs, fmt.Errorf("%s.filter.min_severity: unknown severity %q", prefix, s.Filter.MinSeverity))
			}
		}
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
	if c.Runtime.PollIntervalMax != 0 && c.Runtime.PollIntervalMax < c.Runtime.PollInterval {
		errs = append(errs, errors.New("runtime.poll_interval_max: must be >= runtime.poll_interval (or 0 to disable adaptive polling)"))
	}
	if c.Runtime.PollBackoffMultiplier != 0 && c.Runtime.PollBackoffMultiplier <= 1 {
		errs = append(errs, errors.New("runtime.poll_backoff_multiplier: must be > 1"))
	}
	if c.Runtime.MaxBatchBytes < 0 {
		errs = append(errs, errors.New("runtime.max_batch_bytes: must be >= 0 (0 = per-chunk writes)"))
	}
	if c.Runtime.MaxBatchRecords < 0 {
		errs = append(errs, errors.New("runtime.max_batch_records: must be >= 0 (0 = per-chunk writes)"))
	}
	if c.Runtime.CheckpointRetention != 0 && c.Runtime.CheckpointRetention < 24*time.Hour {
		errs = append(errs, errors.New("runtime.checkpoint_retention: must be >= 24h (or 0 to disable)"))
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
		switch s.Kafka.SASLMechanism {
		case "", "plain", "scram-sha-256", "scram-sha-512":
		default:
			errs = append(errs, fmt.Errorf("%s.kafka.sasl_mechanism: must be plain, scram-sha-256, or scram-sha-512 (got %q)", prefix, s.Kafka.SASLMechanism))
		}
		if s.Kafka.SASLUsername != "" && s.Kafka.SASLPassword == "" {
			errs = append(errs, fmt.Errorf("%s.kafka.sasl_password: required when sasl_username is set", prefix))
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
	case SinkTypeStdout:
		// Stdout block is optional; validate format when present.
		if s.Stdout != nil && s.Stdout.Format != "" && s.Stdout.Format != "ndjson" && s.Stdout.Format != "text" {
			errs = append(errs, fmt.Errorf("%s.stdout.format: must be ndjson or text (got %q)", prefix, s.Stdout.Format))
		}
	default:
		errs = append(errs, fmt.Errorf("%s.type: must be one of s3, kafka, http, stdout (got %q)", prefix, s.Type))
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
