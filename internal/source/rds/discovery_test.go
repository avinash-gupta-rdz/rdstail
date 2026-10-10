package rds_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

type mockDiscoveryAPI struct {
	pages []*awsrds.DescribeDBInstancesOutput
	calls int
}

func (m *mockDiscoveryAPI) DescribeDBInstances(_ context.Context, _ *awsrds.DescribeDBInstancesInput, _ ...func(*awsrds.Options)) (*awsrds.DescribeDBInstancesOutput, error) {
	if m.calls >= len(m.pages) {
		return &awsrds.DescribeDBInstancesOutput{}, nil
	}
	out := m.pages[m.calls]
	m.calls++
	return out, nil
}

func db(id, engine, status string, tags map[string]string) rdstypes.DBInstance {
	var tl []rdstypes.Tag
	for k, v := range tags {
		tl = append(tl, rdstypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return rdstypes.DBInstance{
		DBInstanceIdentifier: aws.String(id),
		Engine:               aws.String(engine),
		DBInstanceStatus:     aws.String(status),
		TagList:              tl,
	}
}

func TestDiscoverInstances_TagAndEngineFiltering(t *testing.T) {
	api := &mockDiscoveryAPI{pages: []*awsrds.DescribeDBInstancesOutput{
		{
			DBInstances: []rdstypes.DBInstance{
				db("pg-payments", "postgres", "available", map[string]string{"rdstail": "true", "team": "payments"}),
				db("pg-other-team", "postgres", "available", map[string]string{"rdstail": "true", "team": "search"}),
				db("mysql-payments", "mysql", "available", map[string]string{"rdstail": "true", "team": "payments"}),
			},
			Marker: aws.String("page2"),
		},
		{
			DBInstances: []rdstypes.DBInstance{
				db("aurora-pg-payments", "aurora-postgresql", "available", map[string]string{"rdstail": "true", "team": "payments"}),
				db("oracle-payments", "oracle-ee", "available", map[string]string{"rdstail": "true", "team": "payments"}),
				db("pg-untagged", "postgres", "available", nil),
			},
		},
	}}

	// All tags must match (AND); pagination followed; oracle skipped.
	found, err := rdssrc.DiscoverInstances(context.Background(), api, rdssrc.DiscoverFilter{
		Tags: map[string]string{"rdstail": "true", "team": "payments"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 {
		t.Fatalf("expected 3 matches, got %d: %+v", len(found), found)
	}
	// Sorted by ID; aurora normalised to postgres.
	if found[0].ID != "aurora-pg-payments" || found[0].Engine != "postgres" {
		t.Fatalf("bad first match: %+v", found[0])
	}
	if found[1].ID != "mysql-payments" || found[1].Engine != "mysql" {
		t.Fatalf("bad second match: %+v", found[1])
	}
	if found[2].ID != "pg-payments" {
		t.Fatalf("bad third match: %+v", found[2])
	}

	// Engine filter narrows to postgres only.
	api.calls = 0
	found, err = rdssrc.DiscoverInstances(context.Background(), api, rdssrc.DiscoverFilter{
		Tags:   map[string]string{"rdstail": "true", "team": "payments"},
		Engine: "postgres",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 || found[0].ID != "aurora-pg-payments" || found[1].ID != "pg-payments" {
		t.Fatalf("engine filter wrong: %+v", found)
	}
}

func TestListInstances_NoTagFilter(t *testing.T) {
	api := &mockDiscoveryAPI{pages: []*awsrds.DescribeDBInstancesOutput{
		{
			DBInstances: []rdstypes.DBInstance{
				db("pg-1", "postgres", "available", nil),
				db("oracle-1", "oracle-ee", "available", nil),
			},
			Marker: aws.String("page2"),
		},
		{
			DBInstances: []rdstypes.DBInstance{
				db("mysql-1", "mysql", "available", map[string]string{"any": "tag"}),
			},
		},
	}}
	found, err := rdssrc.ListInstances(context.Background(), api, "")
	if err != nil {
		t.Fatal(err)
	}
	// Everything supported, regardless of tags; oracle skipped; sorted.
	if len(found) != 2 || found[0].ID != "mysql-1" || found[1].ID != "pg-1" {
		t.Fatalf("unexpected instances: %+v", found)
	}

	api.calls = 0
	found, err = rdssrc.ListInstances(context.Background(), api, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].ID != "pg-1" {
		t.Fatalf("engine filter wrong: %+v", found)
	}
}

func TestDiscoverInstances_RequiresTags(t *testing.T) {
	if _, err := rdssrc.DiscoverInstances(context.Background(), &mockDiscoveryAPI{}, rdssrc.DiscoverFilter{}); err == nil {
		t.Fatal("expected error for empty tag filter")
	}
}

func TestNormalizeEngine(t *testing.T) {
	cases := map[string]string{
		"postgres":          "postgres",
		"aurora-postgresql": "postgres",
		"MySQL":             "mysql",
		"aurora-mysql":      "mysql",
		"aurora":            "mysql",
		"mariadb":           "mariadb",
		"oracle-ee":         "",
		"sqlserver-ex":      "",
		"":                  "",
	}
	for in, want := range cases {
		if got := rdssrc.NormalizeEngine(in); got != want {
			t.Errorf("NormalizeEngine(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestDiscoverInstances_AllWithExcludeTags(t *testing.T) {
	m := &mockDiscoveryAPI{pages: []*awsrds.DescribeDBInstancesOutput{{DBInstances: []rdstypes.DBInstance{
		db("prod-mysql", "mysql", "available", map[string]string{"env": "prod"}),
		db("prod-pg", "postgres", "available", nil),
		db("scratch", "mysql", "available", map[string]string{"rdstail": "off"}),
		db("tmp-pg", "postgres", "available", map[string]string{"ephemeral": "yes"}),
		db("oracle-db", "oracle-ee", "available", nil),
	}}}}
	got, err := rdssrc.DiscoverInstances(context.Background(), m, rdssrc.DiscoverFilter{
		All:         true,
		ExcludeTags: map[string]string{"rdstail": "off", "ephemeral": "*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, d := range got {
		ids = append(ids, d.ID+"/"+d.Engine)
	}
	if strings.Join(ids, ",") != "prod-mysql/mysql,prod-pg/postgres" {
		t.Fatalf("got %v", ids)
	}
	if _, err := rdssrc.DiscoverInstances(context.Background(), m, rdssrc.DiscoverFilter{}); err == nil {
		t.Fatal("discovery with neither tags nor all must be rejected")
	}
}
