# RDS audit logs to immutable S3 — without the Lambda pipeline

SOC 2, PCI-DSS, and HIPAA all reduce to the same operational sentence: *put
the database audit trail somewhere nobody — including you — can alter it, and
be able to prove that to an assessor.* AWS's recommended path for this is
CloudWatch Logs → subscription filter → Lambda → CSV → S3
([AWS blog](https://aws.amazon.com/blogs/database/automate-postgresql-audit-log-extraction-and-analysis-with-amazon-s3/)):
four moving parts, CloudWatch ingestion fees on every audit byte, and a
Lambda you now own.

rdstail does the same job as one binary reading the RDS log API directly:

```
RDS instances ──(DescribeDBLogFiles / DownloadDBLogFilePortion)──▶ rdstail ──▶ S3 (Object Lock) ──lifecycle──▶ Glacier
```

No CloudWatch ingestion, no Lambda, no subscription filters. This page is the
end-to-end recipe plus the [evidence guide](#evidence-guide-for-your-assessor)
mapping rdstail's delivery guarantees to control language.

## 1. Turn on audit logging in RDS

**PostgreSQL (pgAudit)** — add to the DB parameter group:

| Parameter | Value |
|---|---|
| `shared_preload_libraries` | `pgaudit` |
| `pgaudit.log` | `ddl, role, write` (or what your policy requires) |

pgAudit entries are written into `postgresql.log`, so rdstail ships them with
**no extra configuration**. It also lifts the pgAudit CSV classification into
each record's `audit` object (see [what lands in S3](#what-lands-in-s3)).

**MySQL / MariaDB (MariaDB Audit Plugin)** — add the `MARIADB_AUDIT_PLUGIN`
option to the instance's option group and set `SERVER_AUDIT_EVENTS`
(e.g. `CONNECT,QUERY_DDL,QUERY_DML`). The plugin writes
`audit/server_audit.log*` files, which rdstail ingests **only** when the
source opts in:

```yaml
sources:
  - type: rds
    engine: mariadb
    region: ap-south-1
    instances: [prod-maria-writer]
    include_audit: true   # audit logs can be high-volume; explicit opt-in
```

## 2. Create the immutable bucket

Object Lock must be enabled at bucket creation — it cannot be added later.

```bash
aws s3api create-bucket \
  --bucket company-audit-archive \
  --region ap-south-1 \
  --create-bucket-configuration LocationConstraint=ap-south-1 \
  --object-lock-enabled-for-bucket

# Default retention: every new object is WORM-locked on arrival.
# COMPLIANCE mode cannot be shortened or removed by any principal, root
# included — check your retention policy before choosing it over GOVERNANCE.
aws s3api put-object-lock-configuration \
  --bucket company-audit-archive \
  --object-lock-configuration '{
    "ObjectLockEnabled": "Enabled",
    "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 365}}
  }'

# Tier locked objects to Glacier after 30 days (immutability is unaffected).
aws s3api put-bucket-lifecycle-configuration \
  --bucket company-audit-archive \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "audit-to-glacier",
      "Status": "Enabled",
      "Filter": {"Prefix": "rds-audit/"},
      "Transitions": [{"Days": 30, "StorageClass": "GLACIER"}]
    }]
  }'
```

Versioning is enabled automatically with Object Lock. Block public access is
on by default for new buckets; verify with `aws s3api get-public-access-block`.

## 3. Run rdstail

The complete config is [`examples/audit-archive.yaml`](../../examples/audit-archive.yaml).
Nothing about Object Lock changes rdstail's S3 sink — `PutObject` is the same
call; the bucket applies the lock. Generate the exact least-privilege policy
and verify end-to-end before starting:

```bash
rdstail iam-policy -c audit-archive.yaml     # least-privilege JSON for this config
rdstail validate -c audit-archive.yaml --deep
rdstail run -c audit-archive.yaml
```

## What lands in S3

One NDJSON object per batch, gzip-compressed, keyed
`<prefix>/<instance>/<engine>/<log-file>/YYYY/MM/DD/<unix-ms>-<batch-id>.ndjson.gz`.
A pgAudit record:

```json
{
  "instance_id": "prod-pg-writer",
  "engine": "postgres",
  "log_file": "error/postgresql.log.2026-07-29-06",
  "timestamp": "2026-07-29T06:12:03Z",
  "severity": "LOG",
  "message": "2026-07-29 06:12:03 UTC:10.0.0.9(5432):app@mydb:[12345]:LOG:  AUDIT: SESSION,1,1,WRITE,INSERT,TABLE,public.payments,\"INSERT INTO payments ...\",<not logged>",
  "audit": {"type": "SESSION", "class": "WRITE", "command": "INSERT",
            "object_type": "TABLE", "object_name": "public.payments"},
  "batch_id": "1f0c9e2a45d81b3c"
}
```

The raw line is always preserved verbatim in `message` — the `audit` object
is lifted from the CSV the engine already structured, never interpreted.
MariaDB audit records keep the plugin's CSV line verbatim in `message` with
the event's own timestamp parsed into `timestamp`.

## Evidence guide for your assessor

rdstail's delivery guarantees, stated in control language. Citations refer to
mechanisms an assessor can inspect in this repository and in your AWS account.

| Control question | Mechanism | Where to verify |
|---|---|---|
| *Is the audit trail complete?* (SOC 2 CC7.2/CC7.3, PCI-DSS 10.2) | At-least-once delivery: the checkpoint (`marker`) advances **only after S3 durably acknowledges the write**. A crash re-pulls everything since the last acknowledged batch. | `internal/pipeline/worker.go` (checkpoint-after-ACK); chaos tests in `internal/pipeline/chaos_test.go` |
| *Can delivered records be deduplicated?* | Every record carries a deterministic `batch_id` (hash of instance, file, marker range). Duplicates from crash-replay are exact re-sends, identifiable and removable at query time. | `pkg/logrecord`; dedupe query pattern in the roadmap's `athena-ddl` item |
| *Can records be lost on sink failure?* (PCI-DSS 10.5.3 — promptly back up) | No: a batch that exhausts retries is parked in the dead-letter queue, never dropped; `rdstail dlq replay` re-delivers after the outage and deletes only on ACK. | `internal/sink/dlq.go`; `rdstail dlq list/replay` |
| *Is the archive tamper-proof?* (SOC 2 CC6.1, PCI-DSS 10.5.2, HIPAA §164.312(c)) | S3 Object Lock in COMPLIANCE mode: WORM for the retention period, unmodifiable by any principal including root. | Bucket's `get-object-lock-configuration`; AWS-side control |
| *Is access to the trail restricted?* (PCI-DSS 10.5.1) | rdstail's role needs only `s3:PutObject` on the prefix — it cannot read, overwrite, or delete the archive. `rdstail iam-policy` emits the exact least-privilege document. | `rdstail iam-policy -c <config>`; IAM in your account |
| *Are timestamps reliable?* (PCI-DSS 10.4) | Record timestamps are the **database server's own event time** parsed from the line (RDS instances run UTC), not ingestion time; ingestion lag is observable via the `rdstail_ingestion_lag_seconds` metric. | `internal/parse`; `/metrics` endpoint |
| *Is the collector itself monitored?* (SOC 2 CC7.2) | Prometheus metrics: per-instance lag, sink errors, DLQ depth; `validate --deep` probes the full path non-destructively. | `docs/ops`; `/metrics` |

Two honest caveats for the audit file: (1) delivery is at-least-once, so the
archive can contain duplicate records (identifiable by `batch_id`) — WORM
storage makes suppressing them at write time impossible by design; (2)
rdstail ships what the engine logged — coverage of *which statements* are
audited is the pgAudit / audit-plugin configuration in step 1, and belongs in
the same evidence packet.
