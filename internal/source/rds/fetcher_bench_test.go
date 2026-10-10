package rds_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"

	rdssrc "github.com/avinash-gupta-rdz/rdstail/internal/source/rds"
)

// BenchmarkPullPortion_GeneralLog measures rdstail's own per-core cost of
// turning a 1 MB RDS response into records (split, parse, group) — the part
// that isn't bounded by the RDS API.
func BenchmarkPullPortion_GeneralLog(b *testing.B) {
	var sb strings.Builder
	for i := 0; sb.Len() < 1<<20; i++ {
		fmt.Fprintf(&sb, "2026-10-10T06:10:05.%06dZ\t   %d Query\tSELECT * FROM `orders` WHERE id = %d AND tenant = 'acme'\n", i%1000000, i%500, i)
	}
	data := sb.String()
	m := &mockAPI{downloadResponses: map[string]*awsrds.DownloadDBLogFilePortionOutput{
		"general/mysql-general.log|0": {LogFileData: aws.String(data), Marker: aws.String("m")},
	}}
	f, _ := rdssrc.NewFetcher(rdssrc.FetcherOpts{API: m, InstanceID: "db", Engine: "mysql"})
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.downloadCalls = m.downloadCalls[:0]
		if _, err := f.PullPortion(context.Background(), "general/mysql-general.log", "0"); err != nil {
			b.Fatal(err)
		}
	}
}
