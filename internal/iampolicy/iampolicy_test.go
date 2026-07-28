package iampolicy

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
)

func baseConfig() *config.Config {
	return &config.Config{
		Sources: []config.Source{{
			Type: config.SourceTypeRDS, Engine: config.EnginePostgres,
			Region: "ap-south-1", Instances: []string{"db-1", "db-2"},
		}},
		Sinks: []config.Sink{{
			Name: "s3-primary", Type: config.SinkTypeS3,
			S3: &config.S3Sink{Bucket: "my-logs", Prefix: "rds/", Region: "ap-south-1"},
		}},
	}
}

func findStmt(t *testing.T, p Policy, sid string) Statement {
	t.Helper()
	for _, s := range p.Statement {
		if s.Sid == sid {
			return s
		}
	}
	t.Fatalf("statement %q not found; got %+v", sid, p.Statement)
	return Statement{}
}

func hasStmt(p Policy, sid string) bool {
	for _, s := range p.Statement {
		if s.Sid == sid {
			return true
		}
	}
	return false
}

func TestGenerateMinimal(t *testing.T) {
	p, notes := Generate(baseConfig(), Options{})
	if len(notes) != 0 {
		t.Errorf("unexpected notes: %v", notes)
	}
	if p.Version != "2012-10-17" {
		t.Errorf("version = %q", p.Version)
	}

	logs := findStmt(t, p, "RDSReadLogs")
	want := []string{"arn:aws:rds:ap-south-1:*:db:db-1", "arn:aws:rds:ap-south-1:*:db:db-2"}
	if len(logs.Resource) != 2 || logs.Resource[0] != want[0] || logs.Resource[1] != want[1] {
		t.Errorf("RDSReadLogs resources = %v, want %v", logs.Resource, want)
	}

	put := findStmt(t, p, "S3WriteLogs")
	if len(put.Resource) != 1 || put.Resource[0] != "arn:aws:s3:::my-logs/rds/*" {
		t.Errorf("S3WriteLogs resources = %v", put.Resource)
	}

	findStmt(t, p, "RDSDescribeInstances")
	findStmt(t, p, "STSIdentity")
	for _, absent := range []string{"S3DeepValidate", "KMSEncryptLogs", "AssumeCrossAccountRoles"} {
		if hasStmt(p, absent) {
			t.Errorf("statement %q should be absent in minimal config", absent)
		}
	}
}

func TestGenerateDiscoveryWidensToRegionWildcard(t *testing.T) {
	cfg := baseConfig()
	cfg.Sources[0].Discover = &config.Discover{Tags: map[string]string{"rdstail": "true"}}
	p, _ := Generate(cfg, Options{})

	logs := findStmt(t, p, "RDSReadLogs")
	if len(logs.Resource) != 1 || logs.Resource[0] != "arn:aws:rds:ap-south-1:*:db:*" {
		t.Errorf("discovery should collapse instance ARNs to a region wildcard, got %v", logs.Resource)
	}
}

func TestGenerateDeepAddsListBucket(t *testing.T) {
	p, _ := Generate(baseConfig(), Options{Deep: true})
	probe := findStmt(t, p, "S3DeepValidate")
	if len(probe.Resource) != 1 || probe.Resource[0] != "arn:aws:s3:::my-logs" {
		t.Errorf("S3DeepValidate resources = %v", probe.Resource)
	}
	if probe.Action[0] != "s3:ListBucket" {
		t.Errorf("S3DeepValidate action = %v", probe.Action)
	}
}

func TestGenerateKMSVariants(t *testing.T) {
	cases := []struct{ key, want string }{
		{"arn:aws:kms:eu-west-1:123456789012:key/abc", "arn:aws:kms:eu-west-1:123456789012:key/abc"},
		{"alias/logs", "arn:aws:kms:ap-south-1:*:alias/logs"},
		{"1234-abcd", "arn:aws:kms:ap-south-1:*:key/1234-abcd"},
	}
	for _, tc := range cases {
		cfg := baseConfig()
		cfg.Sinks[0].S3.KMSKeyID = tc.key
		p, _ := Generate(cfg, Options{})
		kms := findStmt(t, p, "KMSEncryptLogs")
		if len(kms.Resource) != 1 || kms.Resource[0] != tc.want {
			t.Errorf("key %q → %v, want %q", tc.key, kms.Resource, tc.want)
		}
	}
}

func TestGenerateAssumeRoleMovesPermsToNotes(t *testing.T) {
	cfg := baseConfig()
	cfg.Sources[0].AssumeRole = "arn:aws:iam::111:role/reader"
	cfg.Sinks[0].S3.AssumeRole = "arn:aws:iam::222:role/writer"
	cfg.Sinks[0].S3.ExternalID = "xid-1"
	p, notes := Generate(cfg, Options{})

	// Local principal gets only AssumeRole + STSIdentity — the RDS and S3
	// permissions belong to the remote roles.
	for _, absent := range []string{"RDSReadLogs", "RDSDescribeInstances", "S3WriteLogs"} {
		if hasStmt(p, absent) {
			t.Errorf("statement %q should move to the assumed role", absent)
		}
	}
	ar := findStmt(t, p, "AssumeCrossAccountRoles")
	if len(ar.Resource) != 2 {
		t.Errorf("AssumeCrossAccountRoles resources = %v", ar.Resource)
	}
	if len(notes) != 2 {
		t.Fatalf("want 2 notes, got %v", notes)
	}
	joined := strings.Join(notes, "\n")
	for _, frag := range []string{"rds:DownloadDBLogFilePortion", "s3:PutObject on arn:aws:s3:::my-logs/rds/*", "xid-1"} {
		if !strings.Contains(joined, frag) {
			t.Errorf("notes missing %q:\n%s", frag, joined)
		}
	}
}

func TestGenerateNonS3SinksNeedNoIAM(t *testing.T) {
	cfg := baseConfig()
	cfg.Sinks = []config.Sink{
		{Name: "k", Type: config.SinkTypeKafka, Kafka: &config.KafkaSink{Brokers: []string{"b:9092"}, Topic: "t"}},
		{Name: "h", Type: config.SinkTypeHTTP, HTTP: &config.HTTPSink{URL: "https://x"}},
		{Name: "o", Type: config.SinkTypeStdout},
	}
	p, _ := Generate(cfg, Options{Deep: true})
	for _, absent := range []string{"S3WriteLogs", "S3DeepValidate", "KMSEncryptLogs"} {
		if hasStmt(p, absent) {
			t.Errorf("statement %q should be absent without S3 sinks", absent)
		}
	}
}

func TestGenerateDedupesAcrossSinks(t *testing.T) {
	cfg := baseConfig()
	dup := cfg.Sinks[0]
	dup.Name = "s3-secondary"
	cfg.Sinks = append(cfg.Sinks, dup)
	p, _ := Generate(cfg, Options{})
	put := findStmt(t, p, "S3WriteLogs")
	if len(put.Resource) != 1 {
		t.Errorf("identical bucket/prefix should dedupe, got %v", put.Resource)
	}
}

func TestJSONRoundTrips(t *testing.T) {
	p, _ := Generate(baseConfig(), Options{})
	out, err := p.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if parsed["Version"] != "2012-10-17" {
		t.Errorf("Version missing from JSON output")
	}
}

func TestTerraformOutput(t *testing.T) {
	p, _ := Generate(baseConfig(), Options{})
	hcl := p.Terraform()
	for _, frag := range []string{
		`data "aws_iam_policy_document" "rdstail" {`,
		`sid       = "RDSReadLogs"`,
		`actions   = ["rds:DescribeDBLogFiles", "rds:DownloadDBLogFilePortion"]`,
		`resources = ["arn:aws:s3:::my-logs/rds/*"]`,
	} {
		if !strings.Contains(hcl, frag) {
			t.Errorf("terraform output missing %q:\n%s", frag, hcl)
		}
	}
}
