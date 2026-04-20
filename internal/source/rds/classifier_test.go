package rds

import "testing"

func TestPostgresClassifier(t *testing.T) {
	c := NewClassifier("postgres")
	if c.FilenameContains() != "postgres" {
		t.Fatalf("expected FilenameContains=postgres, got %q", c.FilenameContains())
	}
	cases := map[string]bool{
		"error/postgresql.log.2025-01-01":         true,
		"error/postgresql.log.2025-04-01-00":      true,
		"error/postgres.log":                      true,
		"error/mysql-error.log":                   false,
		"slowquery/mysql-slowquery.log":           false,
		"":                                        false,
	}
	for name, want := range cases {
		if got := c.Accepts(name); got != want {
			t.Errorf("Accepts(%q)=%v, want %v", name, got, want)
		}
	}
}

func TestMySQLClassifier(t *testing.T) {
	c := NewClassifier("mysql")
	cases := map[string]bool{
		"error/mysql-error.log":                true,
		"error/mysql-error-running.log":        true,
		"slowquery/mysql-slowquery.log":        true,
		"general/mysql-general.log":            true,
		"error/mariadb-error.log":              true,
		"error/postgresql.log":                 true,  // under error/, fallback accepts
		"audit/server_audit.log":               false,
		"other/something.txt":                  false,
	}
	for name, want := range cases {
		if got := c.Accepts(name); got != want {
			t.Errorf("Accepts(%q)=%v, want %v", name, got, want)
		}
	}
	// MariaDB uses same classifier, same behavior.
	if got := NewClassifier("mariadb").Accepts("error/mariadb-error.log"); !got {
		t.Fatal("mariadb classifier did not accept mariadb-error.log")
	}
}

func TestAllClassifier_UnknownEngine(t *testing.T) {
	c := NewClassifier("unknown")
	if !c.Accepts("anything.log") {
		t.Fatal("all classifier should accept everything")
	}
	if c.FilenameContains() != "" {
		t.Fatalf("expected empty filter, got %q", c.FilenameContains())
	}
}
