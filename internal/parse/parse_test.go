package parse

import (
	"reflect"
	"testing"
	"time"

	"github.com/avinash-gupta-rdz/rdstail/pkg/logrecord"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestPostgres(t *testing.T) {
	p := ForEngine("postgres")
	cases := []struct {
		name string
		line string
		want Meta
	}{
		{
			"error with client context",
			`2026-07-21 10:15:32 UTC:10.0.1.5(53422):app@orders:[12345]:ERROR:  relation "missing" does not exist at character 15`,
			Meta{Timestamp: ts("2026-07-21T10:15:32Z"), Severity: "ERROR"},
		},
		{
			"system process, empty r/u/d",
			`2026-07-21 10:15:33 UTC::@:[389]:LOG:  checkpoint starting: time`,
			Meta{Timestamp: ts("2026-07-21T10:15:33Z"), Severity: "LOG"},
		},
		{
			"fatal auth failure",
			`2026-07-21 10:15:34 UTC:192.168.0.9(60000):bad@postgres:[9999]:FATAL:  password authentication failed for user "bad"`,
			Meta{Timestamp: ts("2026-07-21T10:15:34Z"), Severity: "FATAL"},
		},
		{
			"statement continuation carries its own prefix",
			`2026-07-21 10:15:34 UTC:10.0.1.5(53422):app@orders:[12345]:STATEMENT:  SELECT * FROM missing`,
			Meta{Timestamp: ts("2026-07-21T10:15:34Z"), Severity: "STATEMENT"},
		},
		{
			"millisecond timestamp (%m style)",
			`2026-07-21 10:15:35.123 UTC:10.0.1.5(53422):app@orders:[12345]:WARNING:  bare warning`,
			Meta{Timestamp: ts("2026-07-21T10:15:35Z"), Severity: "WARNING"},
		},
		{
			"debug with level digit",
			`2026-07-21 10:15:36 UTC::@:[400]:DEBUG1:  something verbose`,
			Meta{Timestamp: ts("2026-07-21T10:15:36Z"), Severity: "DEBUG1"},
		},
		{
			"raw continuation line without prefix",
			`	at some_function (query text spills over)`,
			Meta{},
		},
		{"empty", "", Meta{}},
	}
	for _, c := range cases {
		if got := p.Parse(c.line); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestMySQLAndMariaDB(t *testing.T) {
	p := ForEngine("mysql")
	cases := []struct {
		name string
		line string
		want Meta
	}{
		{
			"mysql 8.0 with err code and subsystem",
			`2026-07-21T10:15:32.835618Z 8 [Warning] [MY-010055] [Server] CA certificate is self signed.`,
			Meta{Timestamp: ts("2026-07-21T10:15:32.835618Z"), Severity: "WARNING"},
		},
		{
			"mysql 5.7 without err code",
			`2026-07-21T10:15:33.000001Z 0 [Note] InnoDB: Buffer pool(s) load completed`,
			Meta{Timestamp: ts("2026-07-21T10:15:33.000001Z"), Severity: "NOTE"},
		},
		{
			"mysql 8.0 system severity",
			`2026-07-21T10:15:34.100000Z 0 [System] [MY-010116] [Server] /usr/sbin/mysqld (mysqld 8.0.35) starting`,
			Meta{Timestamp: ts("2026-07-21T10:15:34.1Z"), Severity: "SYSTEM"},
		},
		{
			"timezone offset instead of Z",
			`2026-07-21T10:15:35.000000+05:30 12 [Error] [MY-013183] [InnoDB] Assertion failure`,
			Meta{Timestamp: ts("2026-07-21T10:15:35+05:30"), Severity: "ERROR"},
		},
		{
			"mariadb 10.x",
			`2026-07-21 10:15:36 0 [Note] InnoDB: log sequence number 12345`,
			Meta{Timestamp: ts("2026-07-21T10:15:36Z"), Severity: "NOTE"},
		},
		{
			"slow query time header",
			`# Time: 2026-07-21T10:15:37.123456Z`,
			Meta{Timestamp: ts("2026-07-21T10:15:37.123456Z")},
		},
		{
			"slow query body line",
			`# Query_time: 12.000000  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 500000`,
			Meta{},
		},
		{
			"sql text line",
			`SELECT sleep(12);`,
			Meta{},
		},
	}
	for _, c := range cases {
		if got := p.Parse(c.line); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}

	if ForEngine("mariadb") == nil {
		t.Fatal("mariadb must have a parser")
	}
	if ForEngine("oracle") != nil {
		t.Fatal("unknown engine must return nil parser")
	}
}

func TestPostgresParser_PGAudit(t *testing.T) {
	p := ForEngine("postgres")

	line := `2026-07-21 10:00:00 UTC:10.0.0.9(5432):app@mydb:[12345]:LOG:  AUDIT: SESSION,1,1,READ,SELECT,TABLE,public.accounts,"SELECT * FROM accounts, users",<not logged>`
	got := p.Parse(line)
	if got.Severity != "LOG" {
		t.Fatalf("severity: got %q", got.Severity)
	}
	want := &logrecord.Audit{
		Type: "SESSION", Class: "READ", Command: "SELECT",
		ObjectType: "TABLE", ObjectName: "public.accounts",
	}
	if !reflect.DeepEqual(got.Audit, want) {
		t.Fatalf("audit: got %+v, want %+v", got.Audit, want)
	}

	// DDL entry without object fields still yields type/class/command.
	ddl := `2026-07-21 10:00:00 UTC::@:[389]:LOG:  AUDIT: SESSION,2,1,DDL,CREATE TABLE,,,CREATE TABLE t(i int),<none>`
	if a := p.Parse(ddl).Audit; a == nil || a.Command != "CREATE TABLE" || a.ObjectName != "" {
		t.Fatalf("ddl audit: got %+v", a)
	}

	// A statement merely containing "AUDIT: " must not be misread: the
	// payload is anchored to the prefix end, and non-pgAudit CSV yields nil.
	trap := `2026-07-21 10:00:00 UTC:10.0.0.9(5432):app@mydb:[12345]:ERROR:  relation "AUDIT: SESSION" does not exist`
	if a := p.Parse(trap).Audit; a != nil {
		t.Fatalf("expected nil audit for non-audit line, got %+v", a)
	}

	// Plain lines keep working, audit nil.
	if a := p.Parse(`2026-07-21 10:00:00 UTC::@:[389]:LOG:  checkpoint starting`).Audit; a != nil {
		t.Fatalf("expected nil audit, got %+v", a)
	}
}

func TestMySQLParser_ServerAuditTimestamp(t *testing.T) {
	p := ForEngine("mariadb")
	got := p.Parse(`20260729 06:00:00,ip-10-0-0-1,app,10.0.0.9,64,1234,QUERY,mydb,'SELECT 1',0`)
	want := time.Date(2026, 7, 29, 6, 0, 0, 0, time.UTC)
	if !got.Timestamp.Equal(want) {
		t.Fatalf("timestamp: got %v, want %v", got.Timestamp, want)
	}
	if got.Severity != "" || got.Audit != nil {
		t.Fatalf("server_audit lines carry no severity/audit fields, got %+v", got)
	}
}
