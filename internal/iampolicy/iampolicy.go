// Package iampolicy derives the minimal IAM policy a config needs, so users
// paste generated JSON instead of hand-assembling the README's policy plus
// its conditional add-ons (KMS, assume-role, deep-validate probes).
package iampolicy

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
)

// Policy is an AWS IAM policy document.
type Policy struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// Statement is one IAM policy statement.
type Statement struct {
	Sid      string   `json:"Sid,omitempty"`
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

// Options controls what the generated policy covers.
type Options struct {
	// Deep adds the extra permissions `validate --deep` probes use
	// (s3:ListBucket for HeadBucket).
	Deep bool
}

// Generate returns the policy for the principal running rdstail, plus
// human-readable notes for permissions that must live elsewhere (e.g. on an
// assumed role in another account).
func Generate(cfg *config.Config, opts Options) (Policy, []string) {
	var notes []string
	var stmts []Statement

	logResources := map[string]struct{}{}
	wildcardRegions := map[string]struct{}{}
	assumeRoles := map[string]struct{}{}
	rdsViaLocal := false

	for _, src := range cfg.Sources {
		if src.AssumeRole != "" {
			assumeRoles[src.AssumeRole] = struct{}{}
			notes = append(notes, fmt.Sprintf(
				"source (region %s) assumes role %s — this policy only grants sts:AssumeRole on it; the role itself needs rds:DescribeDBInstances, rds:DescribeEvents, rds:DescribeDBLogFiles, and rds:DownloadDBLogFilePortion",
				src.Region, src.AssumeRole))
			continue
		}
		rdsViaLocal = true
		if src.Discover != nil {
			// Tag filtering happens client-side after listing, so log-read
			// access can only be scoped to the region.
			wildcardRegions[src.Region] = struct{}{}
		}
		for _, id := range src.Instances {
			logResources[fmt.Sprintf("arn:aws:rds:%s:*:db:%s", src.Region, id)] = struct{}{}
		}
	}
	for region := range wildcardRegions {
		wildcard := fmt.Sprintf("arn:aws:rds:%s:*:db:*", region)
		for arn := range logResources {
			if strings.HasPrefix(arn, fmt.Sprintf("arn:aws:rds:%s:", region)) {
				delete(logResources, arn)
			}
		}
		logResources[wildcard] = struct{}{}
	}

	if rdsViaLocal {
		// DescribeDBInstances has no useful resource scoping for discovery
		// (the tag filter is applied after listing) and is also used by
		// `tail` and `validate --deep` for engine auto-detection.
		// DescribeEvents (reboot/failover detection) is likewise unscoped:
		// one call per region lists events for every instance.
		stmts = append(stmts, Statement{
			Sid:      "RDSDescribeInstances",
			Effect:   "Allow",
			Action:   []string{"rds:DescribeDBInstances", "rds:DescribeEvents"},
			Resource: []string{"*"},
		})
		stmts = append(stmts, Statement{
			Sid:      "RDSReadLogs",
			Effect:   "Allow",
			Action:   []string{"rds:DescribeDBLogFiles", "rds:DownloadDBLogFilePortion"},
			Resource: sorted(logResources),
		})
	}

	stmts = append(stmts, Statement{
		Sid:      "STSIdentity",
		Effect:   "Allow",
		Action:   []string{"sts:GetCallerIdentity"},
		Resource: []string{"*"},
	})

	putResources := map[string]struct{}{}
	probeBuckets := map[string]struct{}{}
	kmsResources := map[string]struct{}{}
	for _, s := range cfg.Sinks {
		if s.Type != config.SinkTypeS3 || s.S3 == nil {
			continue
		}
		objARN := fmt.Sprintf("arn:aws:s3:::%s/%s*", s.S3.Bucket, s.S3.Prefix)
		if s.S3.AssumeRole != "" {
			assumeRoles[s.S3.AssumeRole] = struct{}{}
			remote := fmt.Sprintf(
				"sink %q assumes role %s — this policy only grants sts:AssumeRole on it; the role (in the bucket's account) needs s3:PutObject on %s",
				s.Name, s.S3.AssumeRole, objARN)
			if s.S3.KMSKeyID != "" {
				remote += fmt.Sprintf(" and kms:Encrypt + kms:GenerateDataKey on %s", kmsARN(s.S3.Region, s.S3.KMSKeyID))
			}
			if s.S3.ExternalID != "" {
				remote += fmt.Sprintf("; its trust policy must accept sts:ExternalId %q", s.S3.ExternalID)
			}
			notes = append(notes, remote)
			continue
		}
		putResources[objARN] = struct{}{}
		if opts.Deep {
			probeBuckets[fmt.Sprintf("arn:aws:s3:::%s", s.S3.Bucket)] = struct{}{}
		}
		if s.S3.KMSKeyID != "" {
			kmsResources[kmsARN(s.S3.Region, s.S3.KMSKeyID)] = struct{}{}
		}
	}

	if len(putResources) > 0 {
		stmts = append(stmts, Statement{
			Sid:      "S3WriteLogs",
			Effect:   "Allow",
			Action:   []string{"s3:PutObject"},
			Resource: sorted(putResources),
		})
	}
	if len(probeBuckets) > 0 {
		stmts = append(stmts, Statement{
			Sid:      "S3DeepValidate",
			Effect:   "Allow",
			Action:   []string{"s3:ListBucket"},
			Resource: sorted(probeBuckets),
		})
	}
	if len(kmsResources) > 0 {
		stmts = append(stmts, Statement{
			Sid:      "KMSEncryptLogs",
			Effect:   "Allow",
			Action:   []string{"kms:Encrypt", "kms:GenerateDataKey"},
			Resource: sorted(kmsResources),
		})
	}
	if len(assumeRoles) > 0 {
		stmts = append(stmts, Statement{
			Sid:      "AssumeCrossAccountRoles",
			Effect:   "Allow",
			Action:   []string{"sts:AssumeRole"},
			Resource: sorted(assumeRoles),
		})
	}

	return Policy{Version: "2012-10-17", Statement: stmts}, notes
}

// kmsARN turns whatever the config holds (full ARN, alias, or bare key ID)
// into a policy resource. Bare aliases/IDs get a wildcard account since the
// account isn't known statically.
func kmsARN(region, key string) string {
	switch {
	case strings.HasPrefix(key, "arn:"):
		return key
	case strings.HasPrefix(key, "alias/"):
		return fmt.Sprintf("arn:aws:kms:%s:*:%s", region, key)
	default:
		return fmt.Sprintf("arn:aws:kms:%s:*:key/%s", region, key)
	}
}

// JSON renders the policy as indented AWS policy JSON.
func (p Policy) JSON() (string, error) {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Terraform renders the policy as an aws_iam_policy_document data source.
func (p Policy) Terraform() string {
	var b strings.Builder
	b.WriteString("data \"aws_iam_policy_document\" \"rdstail\" {\n")
	for i, s := range p.Statement {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString("  statement {\n")
		fmt.Fprintf(&b, "    sid       = %q\n", s.Sid)
		fmt.Fprintf(&b, "    effect    = %q\n", s.Effect)
		fmt.Fprintf(&b, "    actions   = [%s]\n", quoteJoin(s.Action))
		fmt.Fprintf(&b, "    resources = [%s]\n", quoteJoin(s.Resource))
		b.WriteString("  }\n")
	}
	b.WriteString("}\n")
	return b.String()
}

func quoteJoin(ss []string) string {
	qs := make([]string, len(ss))
	for i, s := range ss {
		qs[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(qs, ", ")
}

func sorted(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
