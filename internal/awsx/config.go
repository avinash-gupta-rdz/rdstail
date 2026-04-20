// Package awsx wraps AWS SDK v2 config loading so every caller uses the same
// creds chain (default chain + optional AssumeRole) and region handling.
package awsx

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Options parameterise NewConfig.
type Options struct {
	Region     string
	AssumeRole string // optional role ARN
	// ExternalID and RoleSessionName are optional modifiers when AssumeRole is set.
	ExternalID      string
	RoleSessionName string
}

// NewConfig returns an SDK v2 aws.Config using the default credentials chain.
// If opts.AssumeRole is set, credentials are wrapped in an AssumeRoleProvider.
func NewConfig(ctx context.Context, opts Options) (aws.Config, error) {
	if opts.Region == "" {
		return aws.Config{}, errors.New("awsx: region is required")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.Region))
	if err != nil {
		return aws.Config{}, fmt.Errorf("load default config: %w", err)
	}
	if opts.AssumeRole != "" {
		stsClient := sts.NewFromConfig(cfg)
		provider := stscreds.NewAssumeRoleProvider(stsClient, opts.AssumeRole, func(o *stscreds.AssumeRoleOptions) {
			if opts.ExternalID != "" {
				o.ExternalID = aws.String(opts.ExternalID)
			}
			if opts.RoleSessionName != "" {
				o.RoleSessionName = opts.RoleSessionName
			} else {
				o.RoleSessionName = "rdstail"
			}
		})
		cfg.Credentials = aws.NewCredentialsCache(provider)
	}
	return cfg, nil
}
