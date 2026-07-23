# rdstail

> `tail -f` for AWS RDS logs — stream PostgreSQL / MySQL / MariaDB log files
> straight from the RDS API to **S3**, **Kafka**, or an **HTTP webhook**.
> Single static binary. At-least-once. No CloudWatch in the middle.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.22%2B-00ADD8.svg)](go.mod)
[![Status](https://img.shields.io/badge/status-beta-orange.svg)](#status)

---

## The problem

You run RDS. You want the Postgres / MySQL error, slow-query, and general
logs somewhere durable — S3 for archive, Kafka for realtime, a webhook for
your SIEM. AWS gives you exactly one path: **CloudWatch log export**.

CloudWatch export is:

- **Expensive** — you pay for ingestion + retention + export, even if all
  you do downstream is move the data elsewhere.
- **Slow** — tail latency is minutes, not seconds.
- **Rigid** — no native path to Kafka, custom webhooks, or a bucket in a
  different account without extra plumbing.
- **One more moving part** — and it fails silently when IAM drifts.

## The solution

rdstail is a single static binary that calls the RDS log API directly
(`DescribeDBLogFiles` + `DownloadDBLogFilePortion`), checkpoints progress
locally, and fans out every record to the sinks you care about. No
CloudWatch. One YAML file to configure. Crashes resume without re-shipping
the world. That's it.

Design priorities, in order:

1. **Correctness** — at-least-once delivery, durable checkpoints, explicit
   failure modes. Never silently drop a log line.
2. **Simplicity** — one YAML file, one binary, three commands.
3. **Cost** — minimise AWS API calls; no always-on CloudWatch cost.

## The cost math (worked example)

Say your fleet produces **100 GB of RDS logs per month** and you want them in
S3. Prices: us-east-1, mid-2026 — check the [CloudWatch](https://aws.amazon.com/cloudwatch/pricing/)
and [S3](https://aws.amazon.com/s3/pricing/) pages for your region.

**CloudWatch export path:**

| Item | Cost |
|---|---|
| Logs ingestion, Standard class ($0.50/GB, billed on *uncompressed* bytes) | **$50.00** |
| Logs storage ($0.03/GB-mo, 1-month retention) | $3.00 |
| Getting it to S3 (subscription → Firehose, export tasks) | extra, per-GB |
| **Total** | **≈ $53+/mo** |

**rdstail path:**

| Item | Cost |
|---|---|
| RDS log API calls (`DescribeDBLogFiles`, `DownloadDBLogFilePortion`) | $0 — not billed |
| Compute: rdstail on an existing node, or a dedicated t4g.micro | $0–6.14 |
| S3 PUTs (5 MiB batches → ~20k objects × $0.005/1k) | $0.10 |
| S3 storage (NDJSON+gzip ≈ 10:1 → ~10 GB × $0.023) | $0.23 |
| **Total** | **≈ $0.35–6.50/mo** |

Roughly **85–95% cheaper**, and the gap widens with volume: CloudWatch charges
per uncompressed GB ingested; rdstail compresses before the first byte is
billed. At 1 TB/mo the CloudWatch path passes $500 while rdstail stays under
$15. Cheaper still with S3 lifecycle rules (Glacier IR at $0.004/GB-mo vs
CloudWatch's $0.03).

**When CloudWatch is worth it anyway:** you actively use Logs Insights
queries, metric filters, or CloudWatch alarms on these logs. rdstail moves
logs; it deliberately doesn't replace those features (see
[Non-goals](#non-goals)). The Infrequent Access log class ($0.25/GB) halves
the ingestion bill if you stay.

---

## Quick start (30 seconds)

With AWS credentials in your environment ([IAM](#iam)), tail any instance
right now — no config file:

```bash
rdstail tail -i my-db-1 --region ap-south-1
# only errors:
rdstail tail -i my-db-1 --region ap-south-1 --min-severity ERROR
```

## Quick start — shipping (5 minutes)

You need Go 1.22+ and AWS credentials with the permissions in [IAM](#iam).

```bash
# 1. Install
go install github.com/avinash-gupta-rdz/rdstail/cmd/rdstail@latest

# 2. Write the smallest possible config
cat > rdstail.yaml <<'EOF'
sources:
  - type: rds
    engine: postgres
    region: ap-south-1
    instances: [my-db-1]

sinks:
  - name: s3-primary
    type: s3
    s3:
      bucket: my-log-bucket
      region: ap-south-1
      prefix: rds/

state:
  type: sqlite
  path: ./state.db

runtime:
  poll_interval: 10s
  start_from: end
EOF

# 3. Validate (schema only — no network)
rdstail validate -c rdstail.yaml

# 4. Deep-validate (hits AWS — confirms creds + bucket + instance)
rdstail validate -c rdstail.yaml --deep

# 5. Run
rdstail run -c rdstail.yaml
```

That's it. Logs start flowing into `s3://my-log-bucket/rds/`.

Kill the process and re-run — checkpoints resume from where you left off,
no replay explosion.

### Quick start — Docker

```bash
# Build locally (no pushed image yet)
docker build -f deploy/Dockerfile -t rdstail:dev .

# Run with mounted config + persistent state
docker run --rm \
  -v $PWD/rdstail.yaml:/etc/rdstail/config.yaml:ro \
  -v $PWD/state:/var/lib/rdstail \
  -p 9090:9090 \
  -e AWS_REGION=ap-south-1 \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  rdstail:dev run -c /etc/rdstail/config.yaml
```

### Quick start — verify it's working

```bash
# Scrape metrics
curl -s localhost:9090/metrics | grep '^rdstail_'

# Structured JSON logs (pipe to jq)
rdstail run -c rdstail.yaml 2>&1 | jq '{msg, instance, log_file, err}'

# Inspect the checkpoint store
sqlite3 ./state.db "SELECT instance_id, log_file, substr(marker,1,20), updated_at FROM checkpoints;"
```

---

## Features

- **Supported engines:** PostgreSQL, MySQL, MariaDB (Aurora variants work
  where the log-file naming matches RDS's).
- **Sinks:** S3 (NDJSON + gzip), Kafka (franz-go, `acks=all`, idempotent),
  HTTP webhook (JSON, optional gzip), stdout (NDJSON or raw text — pipe into
  vector, fluent-bit, or `jq` with zero sink infrastructure). Fan out to all
  of them at once.
- **State store:** SQLite by default (pure Go — no CGO, truly static binary);
  JSON-file fallback for dev.
- **At-least-once with dedupe-friendly batch IDs** — every record carries a
  deterministic `BatchID = sha256(instance|logfile|prevMarker|nextMarker)[:16]`
  so downstream can dedupe for exactly-once semantics.
- **Tag-based discovery with live refresh** — instead of hand-maintaining
  instance lists, give a source `discover.tags` and every matching RDS
  instance (engine auto-detected, Aurora included) is ingested. Set
  `refresh_interval: 5m` and rdstail tracks fleet churn live: Terraform
  creates a tagged instance, a worker starts; the instance is destroyed, its
  worker stops. Preview matches with `rdstail discover`.
- **Cross-chunk batching** — chunks pulled in one poll cycle coalesce into
  writes of up to `runtime.max_batch_bytes` (5 MiB default): fewer S3 PUT
  requests and fewer small files for Athena/Spark to choke on, with
  checkpoints still advancing only after a durable sink ACK.
- **Timestamps & severity that mean something** — every record carries the
  server-side event time and the engine's severity token (`ERROR`, `FATAL`,
  `WARNING`, …) parsed from the line, so downstream you can alert on
  `severity=FATAL` from Kafka or partition S3 by true event time. Best-effort
  with automatic fetch-time fallback; the raw line is never modified.
- **Per-sink severity routing** — give a sink `filter: {min_severity: ERROR}`
  and it receives only error-and-worse records while other sinks still get
  everything: archive all to S3, send only `ERROR+` to the webhook that feeds
  PagerDuty/Slack. Exact-match lists (`severities: [FATAL, PANIC]`) too.
- **Adaptive polling** — set `runtime.poll_interval_max` and idle instances
  back off exponentially (fewer paid RDS API calls on quiet databases), then
  snap back to `poll_interval` the moment logs flow. Watch it work via the
  `rdstail_poll_interval_seconds` gauge.
- **Graceful rotation handling** — detects truncation and new files;
  configurable `start_from: beginning | end`.
- **DLQ with replay** — terminal sink failures are parked in a `sinks_dlq`
  table instead of being dropped. `rdstail dlq list` shows what's parked,
  `rdstail dlq replay` re-delivers batches once the sink is fixed (rows are
  deleted only after a durable ACK), and `rdstail dlq purge --yes` drops them
  deliberately.
- **Observability** — Prometheus metrics on `/metrics`, `/healthz`, `/readyz`,
  plus structured JSON logs.
- **Multi-region, multi-account** — per-source `region` and `assume_role` for
  reading; per-S3-sink `assume_role` (+ `external_id`) for writing straight
  into a central log-archive account's bucket. No replication plumbing.
- **Security** — IAM roles (IRSA supported), SSE-AES256 default, SSE-KMS
  when `kms_key_id` set, TLS where the sink supports it.

## Status

**Beta.** All planned functionality is implemented and covered by unit + chaos
tests (the chaos test verifies `delivered ⊇ source` under 30% sink-flap), and
the full suite runs race-enabled in CI. See [CHANGELOG.md](CHANGELOG.md) for
what's landed since 0.1.0 — DLQ replay, adaptive polling, severity extraction
and routing, batching, tag discovery with live refresh, Kafka TLS/SASL,
`rdstail tail`, and more. Not yet burned in with a multi-day production soak —
that's the last box to tick before `v1.0.0`. Post-v1 direction lives in
[ROADMAP.md](ROADMAP.md).

---

## Install

### Go install

```bash
go install github.com/avinash-gupta-rdz/rdstail/cmd/rdstail@latest
```

The binary lands in `$GOBIN/rdstail` (or `$HOME/go/bin/rdstail`).

### From source

```bash
git clone https://github.com/avinash-gupta-rdz/rdstail.git
cd rdstail
make build        # produces bin/rdstail
```

### Pre-built releases

GoReleaser config is included (`.goreleaser.yml`) — tagging a release and
running `goreleaser release` produces archives for `linux/amd64`,
`linux/arm64`, `darwin/amd64`, `darwin/arm64`.

---

## Commands

| Command | Description |
|---|---|
| `tail -i INSTANCE --region REGION [--min-severity ERROR] [--format ndjson]` | Zero-config `tail -f` for one instance: engine auto-detected, raw lines to stdout, in-memory state (no resume). |
| `run -c PATH` | Start the shipper. Blocks until SIGINT/SIGTERM. |
| `validate -c PATH [--deep]` | Schema-only by default; `--deep` probes STS, RDS (one DescribeDBLogFiles per instance), S3 HeadBucket, HTTP HEAD, Kafka broker ping (with the sink's TLS/SASL settings), and the state-store. Non-zero exit on any probe failure. |
| `dlq list -c PATH [--sink NAME] [--limit N] [--json]` | Show dead-lettered batches, oldest first. `--json` emits one object per line including the full record payload. |
| `dlq replay -c PATH [--sink NAME] [--limit N] [--dry-run]` | Re-deliver parked batches through the configured sinks. A row is deleted only after the sink durably ACKs; failures leave it in place, so replay is always safe to re-run. Non-zero exit if any batch failed. |
| `dlq purge -c PATH --yes [--sink NAME \| --id N]` | Permanently drop parked batches **without** replaying. Refuses to run without `--yes`. |
| `discover -c PATH` | Preview which instances tag-based discovery would ingest (hits AWS; no pipeline started). |
| `state gc -c PATH [--older-than 720h] [--dry-run]` | Prune checkpoints for log files not seen recently (rotated out, departed instances). Safe while `run` is live. |
| `version` | Print version, commit, and build date. |

### DLQ lifecycle

When a sink write exhausts its retries (or fails permanently, e.g. HTTP 4xx),
the batch is parked in the state store's `sinks_dlq` table and the pipeline
moves on — checkpoints keep advancing, nothing is dropped. Once you've fixed
the sink (credentials, topic ACLs, endpoint), drain the queue:

```bash
rdstail dlq list -c rdstail.yaml                  # what's parked, and why
rdstail dlq replay -c rdstail.yaml --dry-run      # what would be re-sent
rdstail dlq replay -c rdstail.yaml                # re-send; delete on ACK
rdstail dlq replay -c rdstail.yaml --sink siem    # just one sink
```

Replay is at-least-once: records keep their original `BatchID`, so downstream
dedupe works exactly as it does for live traffic. Batches parked for a sink
that no longer exists in the config are skipped (and reported) — rename it
back or purge explicitly. Replay builds sinks without the DLQ wrapper, so a
still-broken sink fails loudly instead of silently re-parking.

Global flags:

- `--config, -c` — path to YAML config (required for `run`/`validate`).
- `--log-level` — `debug` / `info` / `warn` / `error` (default: `info`).

---

## Configuration

See `examples/` for a per-topology catalogue:

| File | Topology |
|---|---|
| `examples/config.yaml` | Full example: 2 sources, 2 sinks. |
| `examples/s3-only.yaml` | Postgres fleet → S3. |
| `examples/kafka-only.yaml` | MySQL fleet → Kafka with `topic_template`. |
| `examples/http-webhook.yaml` | Single instance → webhook with gzip. |
| `examples/fanout.yaml` | Every record written to both S3 **and** Kafka. |
| `examples/discover.yaml` | Tag-based discovery — no hand-maintained instance list. |
| `examples/stdout.yaml` | Pipe integration — NDJSON to stdout for vector/fluent-bit/jq. |

### Full config reference

```yaml
sources:                       # required; ≥ 1
  - type: rds                  # only "rds" in v1
    engine: postgres           # postgres | mysql | mariadb (required with
                               # explicit instances; optional with discover —
                               # then it acts as an engine filter)
    region: ap-south-1         # AWS region
    instances: [db-1, db-2]    # explicit DB identifiers (may be empty if
                               # discover is set; union of both is ingested)
    discover:                  # optional tag-based discovery
      tags:                    # AND semantics — every tag must match
        rdstail: "true"
        team: payments
      refresh_interval: 5m     # re-discover on this cadence and start/stop
                               # workers to match the fleet; 0 = startup-only
    assume_role: ""            # optional role ARN for cross-account

sinks:                         # required; ≥ 1; every sink receives every record
  - name: s3-primary           # unique per config
    type: s3                   # s3 | kafka | http
    s3:
      bucket: my-logs
      region: ap-south-1
      prefix: rds/
      assume_role: ""          # role ARN in the bucket's account → cross-
      external_id: ""          # account archive; optional trust condition
      kms_key_id: ""           # empty → SSE-AES256; set → SSE-KMS
      max_bytes: 5242880       # legacy hints; superseded by runtime.max_batch_*
      max_records: 10000
      max_age: 30s
    retry:
      max_attempts: 10
      initial_wait: 500ms
      max_wait: 60s
      multiplier: 2.0

  - name: kafka-hot
    type: kafka
    kafka:
      brokers: [kafka-1:9092, kafka-2:9092]
      topic: rds-logs          # OR topic_template: "rds-logs-{engine}"
      client_id: rdstail
      tls: false
      sasl_username: ""        # optional
      sasl_password: ""
      sasl_mechanism: plain    # plain | scram-sha-256 | scram-sha-512

  - name: pipe
    type: stdout               # NDJSON (or raw text) to stdout; rdstail's own
    stdout:                    # logs go to stderr, so stdout is clean data
      format: ndjson           # ndjson | text

  - name: siem
    type: http
    filter:                    # optional; narrows what THIS sink receives
      min_severity: ERROR      # keep ERROR/FATAL/PANIC (severity-less
                               # records drop) — OR use an exact list:
      # severities: [FATAL, PANIC]
    http:
      url: https://logs.example.com/ingest
      headers:
        Authorization: "Bearer ${TOKEN}"
      gzip: true
      timeout: 30s

state:
  type: sqlite                 # sqlite (default) | file
  path: /var/lib/rdstail/state.db

runtime:
  poll_interval: 10s           # ≥ 1s; base (fastest) poll rate
  poll_interval_max: 0s        # > poll_interval enables adaptive polling:
                               # idle instances back off exponentially to this
                               # ceiling, snap back to poll_interval on data
  poll_backoff_multiplier: 2.0 # idle growth factor (> 1)
  max_batch_bytes: 5242880     # coalesce chunks into one sink write up to 5 MiB
  max_batch_records: 10000     # ...or this many records; 0/0 = write per chunk
  checkpoint_retention: 0s     # >= 24h → auto-prune checkpoints for files not
                               # seen in this long (6h sweep); 0 = manual only
  max_workers: 5               # global cap on concurrently-draining log files
                               # across all instances; 1 = fully serial
  max_instances_concurrent: 0  # 0 → min(len(instances), hard cap)
  shutdown_timeout: 30s
  start_from: end              # end | beginning
  memory_budget_bytes: 268435456  # 256 MiB

metrics:
  enabled: true
  listen: :9090

logging:
  level: info                  # debug | info | warn | error
```

### Environment overrides

Every key can be overridden with `RDSTAIL_` + the dotted path, using
double-underscore for nesting:

```bash
RDSTAIL_RUNTIME__POLL_INTERVAL=5s \
RDSTAIL_LOGGING__LEVEL=debug \
  rdstail run -c rdstail.yaml
```

---

## Checkpoint semantics (the part that matters)

Per poll, per log file:

1. `prev = StateStore.Get(instance, logfile)` — on first sight `Marker=""`.
2. `DownloadDBLogFilePortion(Marker=prev.Marker)` → `data`, `nextMarker`, `pending`.
3. Parse lines, stamp each with `BatchID` derived from
   `sha256(instance|logfile|prev.Marker|nextMarker)`.
4. Chunks coalesce up to `runtime.max_batch_bytes` / `max_batch_records`, then
   `Sink.Write(...)` — **sink must ACK durably** (S3 2xx, Kafka `acks=all`, HTTP 2xx).
5. Only **then** `StateStore.Set(...)` advances the marker (to the last
   coalesced chunk's marker).
6. If `AdditionalDataPending`, loop to step 2 within the same poll cycle.

Crashes between (4) and (5) cause at most one duplicate batch on resume.
The `BatchID` on every record gives downstream consumers everything they need
to dedupe — you can upgrade to exactly-once-at-consumer with a single
`SELECT DISTINCT ON (batch_id)` (SQL) or a Kafka Streams dedupe.

> **Note on the checkpoint token:** AWS's `DownloadDBLogFilePortion` `Marker`
> is an opaque string, not a byte offset. rdstail stores it verbatim and
> tracks bytes separately for rotation heuristics only.

---

## Architecture

```
    RDS API
      │  DescribeDBLogFiles / DownloadDBLogFilePortion
      ▼
  ┌───────────────┐      ┌─────────────────────┐
  │    Fetcher    │◀────▶│  LogFileClassifier  │ (per engine)
  └──────┬────────┘      └─────────────────────┘
         │ *Chunk
         ▼
  ┌─────────────────┐      ┌───────────────┐
  │ InstanceWorker  │◀────▶│   State       │  (SQLite / JSON file)
  └───────┬─────────┘      └───────────────┘
          │ after Sink ACK
          ▼
  ┌─────────────────────────────────────────────┐
  │  Sink (metrics → retry → DLQ wrappers)      │
  │  ┌────────┬────────┬──────────┐             │
  │  │  S3    │ Kafka  │   HTTP   │             │
  │  └────────┴────────┴──────────┘             │
  └─────────────────────────────────────────────┘
```

Full design notes, invariants, and rotation-detection rules are in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Observability

### Prometheus metrics

All collectors are prefixed `rdstail_`:

| Metric | Type | Labels |
|---|---|---|
| `logs_processed_total` | counter | `instance, engine, log_file, sink_type` |
| `logs_failed_total` | counter | `instance, sink_type, reason` |
| `ingestion_lag_seconds` | gauge | `instance, log_file` |
| `api_calls_total` | counter | `operation, outcome` |
| `poll_interval_seconds` | gauge | `instance` |
| `dlq_depth` | gauge | `sink_name` |
| `batch_bytes` | histogram | `sink_type` |
| `sink_write_duration_seconds` | histogram | `sink_type` |
| `state_store_ops_total` | counter | `op, outcome` |

Cardinality is bounded: `log_file` is the basename-only, capped at 64 chars,
and configs with >500 instances are rejected by default.

### Structured logs

Single-line JSON (`slog`) with a consistent context: `instance`, `engine`,
`log_file` where applicable. Pipe through `jq`:

```bash
rdstail run -c rdstail.yaml 2>&1 | jq '{lvl: .level, msg, instance, log_file, err}'
```

### Dashboard & alerting

A ready-to-import Grafana dashboard ships in
[`docs/ops/grafana-dashboard.json`](docs/ops/grafana-dashboard.json) —
throughput, failures, DLQ depth, adaptive-poll behaviour, API health, and
sink latency, wired to a selectable Prometheus datasource.

Ready-made Prometheus rules ship in
[`docs/ops/prometheus-alerts.yml`](docs/ops/prometheus-alerts.yml) — DLQ
non-empty / growing, sink write failures, AWS API error ratio, scrape-down,
and a (deliberately cautious) ingestion-lag smoke signal. The one to keep:
**`RdstailDLQNotEmpty`** — it fires exactly when "we never drop a log line"
has turned into "your data is parked, waiting for `rdstail dlq replay`".

### Health endpoints

- `/healthz` — always 200 once the process is up.
- `/readyz` — 503 during boot, 200 after the scheduler is running.

---

## IAM

Minimum IAM policy for the default setup (one S3 sink, same account):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "rds:DescribeDBInstances",
        "rds:DescribeDBLogFiles",
        "rds:DownloadDBLogFilePortion"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::my-log-bucket/rds/*"
    },
    {
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    }
  ]
}
```

Add-ons by feature:

- `kms:Encrypt`, `kms:GenerateDataKey` — on the KMS key ARN, if using SSE-KMS.
- `sts:AssumeRole` — on the target role, if a source sets `assume_role`.
- `s3:HeadBucket` — if you want `validate --deep` to probe the bucket.

---

## Running in production

### systemd

A ready-to-install unit ships in
[`deploy/rdstail.service`](deploy/rdstail.service):

```ini
# /etc/systemd/system/rdstail.service
[Unit]
Description=rdstail — RDS log shipper
After=network-online.target

[Service]
Type=simple
User=rdstail
Group=rdstail
Environment=AWS_REGION=ap-south-1
ExecStart=/usr/local/bin/rdstail run -c /etc/rdstail/config.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

# Hardening
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
NoNewPrivileges=yes
ReadWritePaths=/var/lib/rdstail

[Install]
WantedBy=multi-user.target
```

### Kubernetes

A minimum deployment needs: an IRSA-annotated ServiceAccount with the IAM
policy above, a PVC for the SQLite file, a ConfigMap with the YAML, and a
Deployment with `replicas: 1` (rdstail is not clustered — multiple replicas
reading the same RDS instance will duplicate work).

### Capacity planning

A single rdstail instance comfortably handles ~100 RDS instances polled
every 10 s. Per-instance cost in AWS API calls is roughly
`ceil(new_log_bytes / 1 MB)` + 1 describe per poll. Set `max_workers` to
saturate your sink throughput — the default of 5 is fine for S3/webhook;
bump to 16+ for Kafka if you have the brokers to absorb it.

---

## Development

```bash
make test                 # unit + integration (fakes + localhost)
make cover                # HTML coverage report → coverage.html
make vet
make lint                 # golangci-lint (install separately)
make e2e                  # anything tagged //go:build e2e
```

Project tree:

```
cmd/rdstail/              CLI entrypoint
internal/app              runtime orchestrator
internal/cli              cobra command tree
internal/config           YAML schema, defaults, static validation
internal/logging          slog JSON setup
internal/metrics          Prometheus collectors + HTTP server
internal/awsx             AWS SDK v2 config helper
internal/parse            per-engine timestamp/severity extraction
internal/source/rds       fetcher, classifier, RDSAPI interface
internal/state/{sqlite,file,memory}
                          pluggable checkpoint stores
internal/sink             Sink interface, Fanout, retry/DLQ/metrics decorators
internal/sink/{s3,kafka,http,memory}
                          concrete sinks
internal/sink/factory     builds sinks from config
internal/pipeline         scheduler, per-instance worker, drain pool
internal/replay           DLQ replay engine for `dlq replay`
internal/validate         deep-probe logic for `validate --deep`
pkg/logrecord             the one exported type
docs/ARCHITECTURE.md      design notes
examples/                 per-topology configs
deploy/                   Dockerfile, systemd
```

### Adding a sink

1. Create `internal/sink/<name>/` with a type implementing `sink.Sink`.
2. Accept a narrow API interface (like `s3.S3API`) so tests can mock the client.
3. Wire a case in `internal/sink/factory/factory.go`.
4. Add a deep probe in `internal/validate/deep.go`.
5. Add an example config under `examples/`.

---

## Non-goals

The project is deliberately narrow. These are **not** on the roadmap:

- ❌ UI / dashboard / multi-tenant management
- ❌ Alerting
- ❌ Advanced log parsing — `LogRecord.Message` is always the raw line.
  (Lightweight metadata extraction — server-side timestamp + severity — IS
  in scope and shipped; structuring queries/fields/params is not.)
- ❌ Implicit ingestion — rdstail never tails an instance you didn't select.
  (Tag-based discovery exists, but it's opt-in per source and matches only
  the tags you configure.)
- ❌ Managed / SaaS offering

---

## Contributing

Contributions are welcome. Please:

1. Open an issue first for anything non-trivial.
2. Keep changes tightly scoped.
3. Write tests for new behaviour; correctness > features.
4. Run `make test vet` before opening a PR.
5. Sign your commits (`git commit -s`) and agree to the
   [Developer Certificate of Origin](https://developercertificate.org/).

---

## Security

If you believe you've found a security vulnerability, please email
**mine2technology@gmail.com** instead of opening a public issue. Disclosure
timeline is 90 days.

rdstail never logs the contents of AWS credentials or RDS log lines at levels
above `debug`. Be careful if you enable `--log-level debug` on a production
host — log lines may contain PII depending on your engine's log settings.

---

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.

---

## Naming

**rdstail** = `tail -f` + RDS. The familiar mental model in a single word.

The project was scaffolded under the working title `rds-log-shipper`; the
repository, module path, binary, metric prefix (`rdstail_`), and env-var
prefix (`RDSTAIL_`) were all renamed during the `0.x` cycle. If you find any
stale references, PRs welcome.
