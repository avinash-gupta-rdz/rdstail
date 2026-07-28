package rds

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
)

// DiscoveryAPI is the narrow interface discovery needs. Kept separate from
// RDSAPI so fetcher mocks don't have to stub it. *awsrds.Client satisfies both.
type DiscoveryAPI interface {
	DescribeDBInstances(ctx context.Context, in *awsrds.DescribeDBInstancesInput, opts ...func(*awsrds.Options)) (*awsrds.DescribeDBInstancesOutput, error)
}

var _ DiscoveryAPI = (*awsrds.Client)(nil)

// DiscoverFilter selects instances during discovery. Tags use AND semantics:
// every listed tag must be present with the exact value. Engine ("" == any)
// filters on the normalised engine name (postgres/mysql/mariadb).
type DiscoverFilter struct {
	Tags   map[string]string
	Engine string
}

// DiscoveredInstance is one tag-matched RDS instance.
type DiscoveredInstance struct {
	ID     string
	Engine string // normalised: postgres | mysql | mariadb
	Status string // raw DBInstanceStatus, e.g. "available"
}

// DiscoverInstances lists the account's RDS instances (paginated) and returns
// those matching the filter, sorted by ID. Instances whose engine has no
// rdstail support (oracle, sqlserver, ...) are skipped even when their tags
// match. Aurora engines normalise to their base engine — log-file naming
// matches where RDS's does (same caveat as explicit configuration).
func DiscoverInstances(ctx context.Context, api DiscoveryAPI, filter DiscoverFilter) ([]DiscoveredInstance, error) {
	if len(filter.Tags) == 0 {
		return nil, fmt.Errorf("rds discover: at least one tag is required")
	}
	return listInstances(ctx, api, filter.Engine, filter.Tags)
}

// ListInstances lists every rdstail-supported RDS instance in the account's
// region (paginated), sorted by ID — no tag filter. engineFilter ("" == any)
// filters on the normalised engine name.
func ListInstances(ctx context.Context, api DiscoveryAPI, engineFilter string) ([]DiscoveredInstance, error) {
	return listInstances(ctx, api, engineFilter, nil)
}

// listInstances pages through DescribeDBInstances, keeping instances that pass
// the engine filter ("" == any) and tag filter (nil == any; otherwise AND
// semantics). Instances whose engine has no rdstail support (oracle,
// sqlserver, ...) are skipped.
func listInstances(ctx context.Context, api DiscoveryAPI, engineFilter string, tagFilter map[string]string) ([]DiscoveredInstance, error) {
	var out []DiscoveredInstance
	var marker *string
	for {
		resp, err := api.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{Marker: marker})
		if err != nil {
			return nil, fmt.Errorf("describe db instances: %w", err)
		}
		for _, db := range resp.DBInstances {
			engine := NormalizeEngine(aws.ToString(db.Engine))
			if engine == "" {
				continue // unsupported engine
			}
			if engineFilter != "" && engine != engineFilter {
				continue
			}
			if tagFilter != nil {
				tags := map[string]string{}
				for _, t := range db.TagList {
					tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
				}
				if !tagsMatch(tagFilter, tags) {
					continue
				}
			}
			out = append(out, DiscoveredInstance{
				ID:     aws.ToString(db.DBInstanceIdentifier),
				Engine: engine,
				Status: aws.ToString(db.DBInstanceStatus),
			})
		}
		if resp.Marker == nil || aws.ToString(resp.Marker) == "" {
			break
		}
		marker = resp.Marker
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// tagsMatch reports whether every wanted tag is present in got with the exact
// value (AND semantics).
func tagsMatch(want, got map[string]string) bool {
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// NormalizeEngine maps an AWS engine identifier to rdstail's engine label, or
// "" for engines rdstail cannot tail.
func NormalizeEngine(apiEngine string) string {
	switch strings.ToLower(apiEngine) {
	case "postgres", "aurora-postgresql":
		return "postgres"
	case "mysql", "aurora-mysql", "aurora":
		return "mysql"
	case "mariadb":
		return "mariadb"
	default:
		return ""
	}
}
