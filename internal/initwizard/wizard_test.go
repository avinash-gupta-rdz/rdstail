package initwizard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
)

var fleet = []Instance{
	{ID: "orders-mysql", Engine: "mysql", Status: "available"},
	{ID: "prod-pg-1", Engine: "postgres", Status: "available"},
	{ID: "prod-pg-2", Engine: "postgres", Status: "available"},
}

// run drives the wizard with scripted answers (one per line).
func run(t *testing.T, answers []string, d Deps) (*Result, error) {
	t.Helper()
	d.In = strings.NewReader(strings.Join(answers, "\n") + "\n")
	var out strings.Builder
	d.Out = &out
	res, err := Run(context.Background(), d, "us-east-1", "rdstail.yaml")
	t.Logf("transcript:\n%s", out.String())
	return res, err
}

func lister(insts []Instance, err error) func(context.Context, string) ([]Instance, error) {
	return func(context.Context, string) ([]Instance, error) { return insts, err }
}

// mustValidate writes the YAML to disk and runs it through the real config
// loader — the wizard's core contract is that its output always validates.
func mustValidate(t *testing.T, yaml string) *config.Config {
	t.Helper()
	p := filepath.Join(t.TempDir(), "rdstail.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("generated YAML does not load: %v\n%s", err, yaml)
	}
	if err := config.Validate(cfg); err != nil {
		t.Fatalf("generated YAML does not validate: %v\n%s", err, yaml)
	}
	return cfg
}

func TestWizardS3AllInstances(t *testing.T) {
	res, err := run(t, []string{
		"",  // region → default us-east-1
		"a", // all instances
		"1", // sink: s3
		"2", // bucket picker: second bucket
		"",  // prefix → rds/
		"",  // bucket region → source region
		"",  // path → rdstail.yaml
	}, Deps{
		ListInstances: lister(fleet, nil),
		ListBuckets: func(_ context.Context, region string) ([]string, error) {
			if region != "us-east-1" {
				return nil, errors.New("bucket lister must receive the prompted region")
			}
			return []string{"other", "my-logs"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	// Mixed engines split into one source per engine, sorted.
	if len(cfg.Sources) != 2 || cfg.Sources[0].Engine != "mysql" || cfg.Sources[1].Engine != "postgres" {
		t.Fatalf("sources = %+v", cfg.Sources)
	}
	if got := cfg.Sources[1].Instances; len(got) != 2 || got[0] != "prod-pg-1" {
		t.Fatalf("postgres instances = %v", got)
	}
	s3 := cfg.Sinks[0].S3
	if s3.Bucket != "my-logs" || s3.Prefix != "rds/" || s3.Region != "us-east-1" {
		t.Fatalf("s3 sink = %+v", s3)
	}
	if res.Path != "rdstail.yaml" {
		t.Errorf("path = %q", res.Path)
	}
}

func TestWizardNumberedSelection(t *testing.T) {
	res, err := run(t, []string{
		"eu-west-1",
		"2,3", // just the two postgres instances
		"4",   // stdout sink
		"",    // path
	}, Deps{ListInstances: lister(fleet, nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	if len(cfg.Sources) != 1 || cfg.Sources[0].Engine != "postgres" || len(cfg.Sources[0].Instances) != 2 {
		t.Fatalf("sources = %+v", cfg.Sources)
	}
	if cfg.Sources[0].Region != "eu-west-1" {
		t.Errorf("region = %q", cfg.Sources[0].Region)
	}
	if cfg.Sinks[0].Type != config.SinkTypeStdout {
		t.Errorf("sink type = %q", cfg.Sinks[0].Type)
	}
	if cfg.Metrics.Enabled {
		t.Error("stdout config should disable metrics for pipe use")
	}
}

func TestWizardManualFallbackWhenListingFails(t *testing.T) {
	res, err := run(t, []string{
		"",                 // region
		"db-a, db-b",       // manual ids (listing failed)
		"mariadb",          // engine
		"2",                // kafka sink
		"k1:9092, k2:9092", // brokers
		"",                 // topic → rds-logs
		"",                 // path
	}, Deps{ListInstances: lister(nil, errors.New("no credentials"))})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	if cfg.Sources[0].Engine != "mariadb" || len(cfg.Sources[0].Instances) != 2 {
		t.Fatalf("sources = %+v", cfg.Sources)
	}
	k := cfg.Sinks[0].Kafka
	if len(k.Brokers) != 2 || k.Brokers[1] != "k2:9092" || k.Topic != "rds-logs" {
		t.Fatalf("kafka sink = %+v", k)
	}
}

func TestWizardHTTPSink(t *testing.T) {
	res, err := run(t, []string{
		"", "a", "3",
		"https://http-intake.logs.datadoghq.com/api/v2/logs",
		"",
	}, Deps{ListInstances: lister(fleet[:1], nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	if cfg.Sinks[0].HTTP.URL != "https://http-intake.logs.datadoghq.com/api/v2/logs" {
		t.Fatalf("http sink = %+v", cfg.Sinks[0].HTTP)
	}
	if !cfg.Sinks[0].HTTP.GZIP {
		t.Error("http sink should default to gzip")
	}
}

func TestWizardOverwriteRefused(t *testing.T) {
	_, err := run(t, []string{
		"", "a", "4",
		"", // path (exists)
		"", // overwrite? → default n
	}, Deps{
		ListInstances: lister(fleet[:1], nil),
		FileExists:    func(string) bool { return true },
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected overwrite refusal, got %v", err)
	}
}

func TestWizardOverwriteAccepted(t *testing.T) {
	res, err := run(t, []string{
		"", "a", "4",
		"",  // path (exists)
		"y", // overwrite
	}, Deps{
		ListInstances: lister(fleet[:1], nil),
		FileExists:    func(string) bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	mustValidate(t, res.YAML)
}

func TestWizardRejectsBadSelection(t *testing.T) {
	for _, sel := range []string{"0", "9", "x,y"} {
		_, err := run(t, []string{"", sel}, Deps{ListInstances: lister(fleet, nil)})
		if err == nil {
			t.Errorf("selection %q should error", sel)
		}
	}
}

func TestWizardRejectsBadEngine(t *testing.T) {
	_, err := run(t, []string{"", "db-1", "oracle"},
		Deps{ListInstances: lister(nil, nil)})
	if err == nil || !strings.Contains(err.Error(), "oracle") {
		t.Fatalf("expected engine error, got %v", err)
	}
}

func TestWizardInputClosedMidway(t *testing.T) {
	_, err := run(t, []string{""}, Deps{ListInstances: lister(fleet, nil)})
	// Region answered, then EOF at instance selection.
	if err == nil || !strings.Contains(err.Error(), "input closed") {
		t.Fatalf("expected input-closed error, got %v", err)
	}
}

func TestWizardBucketFreeTextWhenListingUnavailable(t *testing.T) {
	res, err := run(t, []string{
		"", "a", "1",
		"hand-typed-bucket", // free text: no bucket lister
		"archive/rds/",
		"eu-central-1", // bucket lives elsewhere
		"",
	}, Deps{ListInstances: lister(fleet[:1], nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	s3 := cfg.Sinks[0].S3
	if s3.Bucket != "hand-typed-bucket" || s3.Prefix != "archive/rds/" || s3.Region != "eu-central-1" {
		t.Fatalf("s3 sink = %+v", s3)
	}
}

func TestWizardBucketPickedByName(t *testing.T) {
	res, err := run(t, []string{
		"", "a", "1",
		"typed-not-listed", // a name, while the numbered list is shown
		"", "", "",
	}, Deps{
		ListInstances: lister(fleet[:1], nil),
		ListBuckets: func(context.Context, string) ([]string, error) {
			return []string{"other"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg := mustValidate(t, res.YAML); cfg.Sinks[0].S3.Bucket != "typed-not-listed" {
		t.Fatalf("bucket = %q", cfg.Sinks[0].S3.Bucket)
	}
}

// TestWizardMetacharactersRoundTrip pins the quoting fix: values containing
// YAML comment/complex-scalar characters must load back exactly as typed,
// not silently truncated or retyped.
func TestWizardMetacharactersRoundTrip(t *testing.T) {
	res, err := run(t, []string{
		"", "a", "3",
		"https://h.example/v1?x=1#frag", // '#' must not start a YAML comment
		"",
	}, Deps{ListInstances: lister(fleet[:1], nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	if got := cfg.Sinks[0].HTTP.URL; got != "https://h.example/v1?x=1#frag" {
		t.Fatalf("URL = %q, want it verbatim", got)
	}

	res, err = run(t, []string{
		"", "a", "2",
		"b1:9092",
		"true", // a YAML boolean if left unquoted
		"",
	}, Deps{ListInstances: lister(fleet[:1], nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg = mustValidate(t, res.YAML)
	if got := cfg.Sinks[0].Kafka.Topic; got != "true" {
		t.Fatalf("topic = %q, want the literal string", got)
	}

	res, err = run(t, []string{
		"", "a", "1",
		"my-logs",
		"rds/ # prod", // '#' after space would truncate an unquoted scalar
		"", "",
	}, Deps{ListInstances: lister(fleet[:1], nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg = mustValidate(t, res.YAML)
	if got := cfg.Sinks[0].S3.Prefix; got != "rds/ # prod" {
		t.Fatalf("prefix = %q, want it verbatim", got)
	}
}

func TestWizardDuplicateSelectionDeduped(t *testing.T) {
	res, err := run(t, []string{
		"", "2,2,2", "4", "",
	}, Deps{ListInstances: lister(fleet, nil)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustValidate(t, res.YAML)
	if got := cfg.Sources[0].Instances; len(got) != 1 {
		t.Fatalf("instances = %v, want the duplicate collapsed", got)
	}
}

func TestWizardOverwriteAcceptsYes(t *testing.T) {
	_, err := run(t, []string{
		"", "a", "4",
		"",    // path (exists)
		"YES", // any case, full word
	}, Deps{
		ListInstances: lister(fleet[:1], nil),
		FileExists:    func(string) bool { return true },
	})
	if err != nil {
		t.Fatalf("'YES' should accept the overwrite: %v", err)
	}
}

func TestWizardEngineCaseInsensitive(t *testing.T) {
	res, err := run(t, []string{
		"", "db-1", "Postgres", "4", "",
	}, Deps{ListInstances: lister(nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg := mustValidate(t, res.YAML); cfg.Sources[0].Engine != "postgres" {
		t.Fatalf("engine = %q", cfg.Sources[0].Engine)
	}
}
