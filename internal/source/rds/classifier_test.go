package rds

import "testing"

func TestPostgresClassifier(t *testing.T) {
	c := NewClassifier("postgres", false)
	if c.FilenameContains() != "postgres" {
		t.Fatalf("expected FilenameContains=postgres, got %q", c.FilenameContains())
	}
	cases := map[string]bool{
		"error/postgresql.log.2025-01-01":    true,
		"error/postgresql.log.2025-04-01-00": true,
		"error/postgres.log":                 true,
		"error/mysql-error.log":              false,
		"slowquery/mysql-slowquery.log":      false,
		"":                                   false,
	}
	for name, want := range cases {
		if got := c.Accepts(name); got != want {
			t.Errorf("Accepts(%q)=%v, want %v", name, got, want)
		}
	}
}

func TestMySQLClassifier(t *testing.T) {
	c := NewClassifier("mysql", false)
	cases := map[string]bool{
		"error/mysql-error.log":         true,
		"error/mysql-error-running.log": true,
		"slowquery/mysql-slowquery.log": true,
		"general/mysql-general.log":     true,
		"error/mariadb-error.log":       true,
		"error/postgresql.log":          true, // under error/, fallback accepts
		"audit/server_audit.log":        false,
		"other/something.txt":           false,
	}
	for name, want := range cases {
		if got := c.Accepts(name); got != want {
			t.Errorf("Accepts(%q)=%v, want %v", name, got, want)
		}
	}
	// MariaDB uses same classifier, same behavior.
	if got := NewClassifier("mariadb", false).Accepts("error/mariadb-error.log"); !got {
		t.Fatal("mariadb classifier did not accept mariadb-error.log")
	}
}

func TestMySQLClassifier_IncludeAudit(t *testing.T) {
	c := NewClassifier("mariadb", true)
	cases := map[string]bool{
		"audit/server_audit.log":   true,
		"audit/server_audit.log.1": true,
		"server_audit.log":         true, // Aurora flat layout
		"error/mariadb-error.log":  true, // regular logs still flow
		"other/something.txt":      false,
	}
	for name, want := range cases {
		if got := c.Accepts(name); got != want {
			t.Errorf("Accepts(%q)=%v, want %v", name, got, want)
		}
	}
	// Postgres ignores the flag: pgAudit lives inside postgresql.log.
	if NewClassifier("postgres", true).Accepts("audit/server_audit.log") {
		t.Fatal("postgres classifier must not accept mysql audit files")
	}
}

func TestAllClassifier_UnknownEngine(t *testing.T) {
	c := NewClassifier("unknown", false)
	if !c.Accepts("anything.log") {
		t.Fatal("all classifier should accept everything")
	}
	if c.FilenameContains() != "" {
		t.Fatalf("expected empty filter, got %q", c.FilenameContains())
	}
}
