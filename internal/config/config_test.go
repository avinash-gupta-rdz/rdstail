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
	if cfg.Runtime.MaxInstancesConcurrent != 2 {
		t.Fatalf("expected default max_instances_concurrent=2, got %d", cfg.Runtime.MaxInstancesConcurrent)
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
}

func TestValidate_CatchesAllErrors(t *testing.T) {
	cfg := &Config{
		Sources: []Source{{Type: "rds", Engine: "oracle", Region: "", Instances: nil}},
		Sinks: []Sink{
			{Name: "a", Type: "s3", S3: &S3Sink{Region: "us-east-1"}}, // missing bucket
			{Name: "a", Type: "http", HTTP: &HTTPSink{URL: "ftp://nope"}},
			{Name: "", Type: "kafka", Kafka: &KafkaSink{}},
		},
		State:   State{Type: "redis", Path: ""},
		Runtime: Runtime{PollInterval: 500 * time.Millisecond, MaxWorkers: 0, StartFrom: "middle"},
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
