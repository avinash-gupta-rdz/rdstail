package rds

import (
	"path"
	"strings"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
)

// LogFileClassifier decides whether a given RDS log-file name is eligible for
// ingestion for the configured engine. PostgreSQL and MySQL/MariaDB use
// different naming conventions; this encapsulates those rules.
type LogFileClassifier interface {
	Accepts(logFileName string) bool
	// FilenameContains returns a filter string suitable for
	// DescribeDBLogFilesInput.FilenameContains, used as a server-side pre-filter.
	// Empty string means "no server-side filter".
	FilenameContains() string
}

// NewClassifier returns the classifier for an engine name. includeAudit opts
// MySQL/MariaDB audit-plugin files (audit/server_audit.log*) into ingestion —
// off by default because audit logs can be high-volume. It has no effect for
// postgres, whose pgAudit entries live inside postgresql.log and always flow.
func NewClassifier(engine string, includeAudit bool) LogFileClassifier {
	switch engine {
	case config.EnginePostgres:
		return postgresClassifier{}
	case config.EngineMySQL, config.EngineMariaDB:
		return mysqlClassifier{includeAudit: includeAudit}
	default:
		return allClassifier{}
	}
}

// postgresClassifier accepts error/postgresql.log* and error/postgres.log*.
type postgresClassifier struct{}

func (postgresClassifier) Accepts(name string) bool {
	base := strings.ToLower(path.Base(name))
	return strings.HasPrefix(base, "postgresql.log") || strings.HasPrefix(base, "postgres.log")
}

func (postgresClassifier) FilenameContains() string { return "postgres" }

// mysqlClassifier accepts MySQL/MariaDB error, slow-query, and general logs. The
// RDS layout places them under error/, slowquery/, general/ directories; we
// filter on basename to handle both RDS and Aurora's flat layouts. Audit-plugin
// files are accepted only when includeAudit is set.
type mysqlClassifier struct {
	includeAudit bool
}

func (c mysqlClassifier) Accepts(name string) bool {
	base := strings.ToLower(path.Base(name))
	lower := strings.ToLower(name)
	if c.includeAudit &&
		(strings.HasPrefix(base, "server_audit.log") || strings.HasPrefix(lower, "audit/")) {
		return true
	}
	switch {
	case strings.HasPrefix(base, "mysql-error"),
		strings.HasPrefix(base, "mysql-slowquery"),
		strings.HasPrefix(base, "mysql-general"),
		// MariaDB variants observed in the wild:
		strings.HasPrefix(base, "mariadb-error"),
		strings.HasPrefix(base, "mariadb-slowquery"),
		strings.HasPrefix(base, "mariadb-general"):
		return true
	}
	// Fallback: names under the conventional directories.
	if strings.HasPrefix(lower, "error/") ||
		strings.HasPrefix(lower, "slowquery/") ||
		strings.HasPrefix(lower, "general/") {
		return true
	}
	return false
}

func (mysqlClassifier) FilenameContains() string { return "" } // multi-prefix; filter client-side

// allClassifier is used when engine is unknown; accepts everything so tests/dev
// configs can experiment.
type allClassifier struct{}

func (allClassifier) Accepts(string) bool      { return true }
func (allClassifier) FilenameContains() string { return "" }
