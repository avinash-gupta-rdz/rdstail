package parse

import (
	"testing"
	"time"
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
