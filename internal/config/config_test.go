package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const goodYAML = `
sources:
  - type: rds
    engine: postgres
    region: ap-south-1
    instances: [db-1, db-2]
    include_audit: true

sinks:
  - name: primary
    type: s3
    s3:
      bucket: my-log-bucket
      region: ap-south-1
      prefix: rds/

state:
  type: sqlite
  path: ./state.db

runtime:
  poll_interval: 10s
  max_workers: 5
`

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAndValidate_Happy(t *testing.T) {
	p := writeTemp(t, goodYAML)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Runtime.MaxInstancesConcurrent != 0 {
		t.Fatalf("expected default max_instances_concurrent=0 (one worker per instance), got %d", cfg.Runtime.MaxInstancesConcurrent)
	}
	if cfg.Runtime.ShutdownTimeout != 30*time.Second {
		t.Fatalf("expected default shutdown_timeout=30s, got %s", cfg.Runtime.ShutdownTimeout)
	}
	if cfg.Sinks[0].Retry.MaxAttempts != 10 {
		t.Fatalf("expected default retry.max_attempts=10, got %d", cfg.Sinks[0].Retry.MaxAttempts)
	}
	if cfg.Sinks[0].S3.MaxBytes != 5*1024*1024 {
		t.Fatalf("expected default S3 max_bytes=5MB, got %d", cfg.Sinks[0].S3.MaxBytes)
	}
	if !cfg.Sources[0].IncludeAudit {
		t.Fatal("include_audit did not round-trip")
	}
}

func TestValidate_CatchesAllErrors(t *testing.T) {
	cfg := &Config{
		Sources: []Source{{Type: "rds", Engine: "oracle", Region: "", Instances: nil}},
		Sinks: []Sink{
			{Name: "a", Type: "s3", S3: &S3Sink{Region: "us-east-1"}}, // missing bucket
			{Name: "a", Type: "http", HTTP: &HTTPSink{URL: "ftp://nope"}},
			{Name: "", Type: "kafka", Kafka: &KafkaSink{}},
		},
		State: State{Type: "redis", Path: ""},
		Runtime: Runtime{
			PollInterval: 500 * time.Millisecond, MaxWorkers: 0, StartFrom: "middle",
			PollIntervalMax: 100 * time.Millisecond, PollBackoffMultiplier: 0.5,
		},
	}
	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected validation errors, got nil")
	}
	msg := err.Error()
	wants := []string{
		"engine", "region", "instances",
		"duplicate sink name",
		"s3.bucket",
		"http.url",
		"kafka.brokers",
		"poll_interval",
		"poll_interval_max",
		"poll_backoff_multiplier",
		"max_workers",
		"start_from",
		"state.type",
		"state.path",
	}
	for _, w := range wants {
		if !strings.Contains(msg, w) {
			t.Errorf("expected error to mention %q, got:\n%s", w, msg)
		}
	}
}

func TestLoad_FailsOnMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/path/config.yaml"); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	p := writeTemp(t, goodYAML)
	t.Setenv("RDSTAIL_RUNTIME__POLL_INTERVAL", "25s")
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Runtime.PollInterval != 25*time.Second {
		t.Fatalf("expected env override to 25s, got %s", cfg.Runtime.PollInterval)
	}
}

func TestLoad_ExpandsEnvRefs(t *testing.T) {
	yaml := `
sources:
  - type: rds
    engine: postgres
    region: ap-south-1
    instances: [db-1]

sinks:
  - name: hook
    type: http
    http:
      url: https://ingest.example.com/v1
      headers:
        Authorization: Bearer ${TEST_API_TOKEN}
        X-Unset: ${TEST_UNSET_VAR_XYZ}
  - name: mq
    type: kafka
    kafka:
      brokers: [b:9092]
      topic: t
      sasl_mechanism: plain
      sasl_username: rdstail
      sasl_password: ${TEST_SASL_PW}
`
	t.Setenv("TEST_API_TOKEN", "s3cr3t")
	t.Setenv("TEST_SASL_PW", "kafka-pw")
	cfg, err := Load(writeTemp(t, yaml))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	h := cfg.Sinks[0].HTTP.Headers
	if h["Authorization"] != "Bearer s3cr3t" {
		t.Errorf("Authorization = %q, want expanded token", h["Authorization"])
	}
	// Unset refs stay verbatim so typos are visible, not silently empty.
	if h["X-Unset"] != "${TEST_UNSET_VAR_XYZ}" {
		t.Errorf("X-Unset = %q, want the reference left as-is", h["X-Unset"])
	}
	// Expansion applies to any field, not just headers.
	if pw := cfg.Sinks[1].Kafka.SASLPassword; pw != "kafka-pw" {
		t.Errorf("sasl_password = %q, want expanded value", pw)
	}
}

func TestExpandEnvRefs(t *testing.T) {
	t.Setenv("TEST_EER_A", "alpha")
	t.Setenv("TEST_EER_DOLLARS", "pa$$wd${TEST_EER_A}") // metachars in value
	cases := []struct{ in, want string }{
		{"x: ${TEST_EER_A}", "x: alpha"},
		{"x: ${TEST_EER_A}${TEST_EER_A}", "x: alphaalpha"},
		{"x: $TEST_EER_A", "x: $TEST_EER_A"}, // bare $NAME not expanded
		{"x: ${TEST_EER_MISSING}", "x: ${TEST_EER_MISSING}"},
		{"x: ${1BAD}", "x: ${1BAD}"}, // invalid name untouched
		{"cost is $5", "cost is $5"},
		// Values are inserted literally: no corruption of $/${...} in the
		// value, and no recursive re-expansion.
		{"x: ${TEST_EER_DOLLARS}", "x: pa$$wd${TEST_EER_A}"},
	}
	for _, tc := range cases {
		if got := string(expandEnvRefs([]byte(tc.in))); got != tc.want {
			t.Errorf("expandEnvRefs(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidate_DiscoverSources(t *testing.T) {
	base := func() *Config {
		c := defaults()
		c.Sinks = []Sink{{Name: "s", Type: "http", HTTP: &HTTPSink{URL: "https://x.example/ingest"}}}
		return c
	}

	// Discover-only source: engine optional.
	c := base()
	c.Sources = []Source{{Type: "rds", Region: "ap-south-1",
		Discover: &Discover{Tags: map[string]string{"rdstail": "true"}}}}
	if err := Validate(c); err != nil {
		t.Fatalf("discover-only source should validate, got: %v", err)
	}

	// Discover with neither tags nor all: rejected.
	c = base()
	c.Sources = []Source{{Type: "rds", Region: "ap-south-1", Discover: &Discover{}}}
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "discover") {
		t.Fatalf("expected discover error, got: %v", err)
	}

	// Discover everything in the region, minus excluded tags.
	c = base()
	c.Sources = []Source{{Type: "rds", Region: "ap-south-1",
		Discover: &Discover{All: true, ExcludeTags: map[string]string{"rdstail": "off"}}}}
	if err := Validate(c); err != nil {
		t.Fatalf("discover all should validate, got: %v", err)
	}

	// Explicit instances may omit engine (detected per instance).
	c = base()
	c.Sources = []Source{{Type: "rds", Region: "ap-south-1", Instances: []string{"db-1", "pg-2"}}}
	if err := Validate(c); err != nil {
		t.Fatalf("explicit instances without engine should validate, got: %v", err)
	}

	// An invalid engine is still rejected.
	c.Sources[0].Engine = "oracle"
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "engine") {
		t.Fatalf("expected engine error, got: %v", err)
	}

	// region and regions together: rejected.
	c = base()
	c.Sources = []Source{{Type: "rds", Region: "us-east-1", Regions: []string{"eu-west-1"}, Instances: []string{"db"}}}
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected region/regions error, got: %v", err)
	}

	// Shard must be i/n.
	c = base()
	c.Sources = []Source{{Type: "rds", Region: "us-east-1", Instances: []string{"db"}}}
	c.Runtime.Shard = "5/4"
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "shard") {
		t.Fatalf("expected shard error, got: %v", err)
	}
}

func TestLoad_RegionsExpandToOneSourcePerRegion(t *testing.T) {
	p := writeTemp(t, `
sources:
  - type: rds
    regions: [us-east-1, eu-west-1, ap-south-1]
    discover:
      all: true
      exclude_tags: {rdstail: "off"}
sinks:
  - name: out
    type: stdout
state: {type: sqlite, path: ./state.db}
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(c); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(c.Sources) != 3 {
		t.Fatalf("want 3 sources, got %d", len(c.Sources))
	}
	for i, want := range []string{"us-east-1", "eu-west-1", "ap-south-1"} {
		s := c.Sources[i]
		if s.Region != want || len(s.Regions) != 0 || !s.Discover.All || s.Discover.ExcludeTags["rdstail"] != "off" {
			t.Fatalf("source %d: %+v", i, s)
		}
	}
}

func TestLoad_S3SinkAssumeRole(t *testing.T) {
	p := writeTemp(t, `
sources:
  - type: rds
    engine: postgres
    region: ap-south-1
    instances: [db-1]
sinks:
  - name: central-archive
    type: s3
    s3:
      bucket: org-logs
      region: us-east-1
      assume_role: arn:aws:iam::999999999999:role/log-writer
      external_id: rdstail-prod
state: {type: sqlite, path: ./s.db}
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatal(err)
	}
	s3 := cfg.Sinks[0].S3
	if s3.AssumeRole != "arn:aws:iam::999999999999:role/log-writer" || s3.ExternalID != "rdstail-prod" {
		t.Fatalf("assume_role/external_id not loaded: %+v", s3)
	}
}

// A discovery-only config used to default max_instances_concurrent to 1, so
// only one discovered database was ever tailed (found in live testing). The
// default must stay 0 (= one worker per resolved instance).
func TestLoad_DiscoveryOnlyDoesNotCapConcurrency(t *testing.T) {
	p := writeTemp(t, `
sources:
  - type: rds
    region: us-east-1
    discover: {all: true}
sinks:
  - name: out
    type: stdout
state: {type: sqlite, path: ./state.db}
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Runtime.MaxInstancesConcurrent != 0 {
		t.Fatalf("max_instances_concurrent defaulted to %d; want 0 (auto)", c.Runtime.MaxInstancesConcurrent)
	}
}
