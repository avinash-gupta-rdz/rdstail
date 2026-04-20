// Package rds is the AWS RDS implementation of source.Source. The Fetcher in
// this package owns marker pagination, rotation detection, and engine-specific
// log-file classification.
package rds

import (
	"context"

	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
)

// RDSAPI is the narrow interface over the SDK v2 RDS client — only the two
// operations we use. Defining our own lets us mock the SDK trivially in tests
// (the SDK v2 does not publish service interfaces itself).
type RDSAPI interface {
	DescribeDBLogFiles(ctx context.Context, in *awsrds.DescribeDBLogFilesInput, opts ...func(*awsrds.Options)) (*awsrds.DescribeDBLogFilesOutput, error)
	DownloadDBLogFilePortion(ctx context.Context, in *awsrds.DownloadDBLogFilePortionInput, opts ...func(*awsrds.Options)) (*awsrds.DownloadDBLogFilePortionOutput, error)
}

// Ensure the SDK client satisfies RDSAPI at compile time.
var _ RDSAPI = (*awsrds.Client)(nil)
