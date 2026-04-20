# rdstail

> `tail -f` for AWS RDS logs — stream PostgreSQL / MySQL / MariaDB log files
> straight from the RDS API to **S3**, **Kafka**, or an **HTTP webhook**.
> Single static binary. At-least-once. No CloudWatch in the middle.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.22%2B-00ADD8.svg)](go.mod)
[![Status](https://img.shields.io/badge/status-beta-orange.svg)](#status)

---

## Why

CloudWatch log export for RDS is surprisingly expensive and surprisingly slow.
If all you want is "put my Postgres/MySQL logs in S3 (or Kafka, or a webhook)",
CloudWatch shouldn't be on the critical path.

**rdstail** pulls from the RDS log API directly
(`DescribeDBLogFiles` + `DownloadDBLogFilePortion`), checkpoints its progress
locally, and fans out to the sinks you care about. One goroutine per RDS
instance; one static binary to deploy.

Design priorities, in order:

1. **Correctness** — at-least-once delivery, durable checkpoints, explicit
   failure modes. Never silently drop a log line.
2. **Simplicity** — one YAML file, one binary, four commands. Boring on purpose.
3. **Cost** — minimise AWS API calls; no always-on CloudWatch cost.

---

## Features

- **Supported engines:** PostgreSQL, MySQL, MariaDB (Aurora variants work
  where the log-file naming matches RDS's).
- **Sinks:** S3 (NDJSON + gzip), Kafka (franz-go, `acks=all`, idempotent),
  HTTP webhook (JSON, optional gzip).
- **State store:** SQLite by default (pure Go — no CGO, truly static binary);
  JSON-file fallback for dev.
- **At-least-once with dedupe-friendly batch IDs** — every record carries a
  deterministic `BatchID = sha256(instance|logfile|prevMarker|nextMarker)[:16]`
  so downstream consumers can dedupe for exactly-once semantics.
- **Graceful rotation handling** — detects truncation (`size < prev`) and new
  files; configurable `start_from: beginning | end`.
- **DLQ** — terminal sink failures are parked in a `sinks_dlq` table instead
  of being dropped. Replay is a future `rdstail replay-dlq` command.
- **Observability** — Prometheus metrics on `/metrics`, `/healthz`, `/readyz`,
  plus structured JSON logs.
- **Multi-region, multi-account** — per-source `region` and optional
  `assume_role` ARN.
- **Security** — IAM roles (IRSA supported), SSE-AES256 by default, SSE-KMS
  when `kms_key_id` is set, TLS where the sink supports it.

---

## Status

**Beta.** All phases of the reference plan are implemented and covered by
unit + chaos tests (the chaos test verifies `delivered ⊇ source` under 30%
sink-flap). Not yet burned in with a multi-day production soak — that's the
last box to tick before `v1.0.0`.

---

## Install

### Go install

```bash
go install github.com/avinash-gupta-rdz/rdstail/cmd/rdstail@latest
```

The binary will be placed at `$GOBIN/rdstail` (or `$HOME/go/bin/rdstail`).

### From source

```bash
git clone https://github.com/avinash-gupta-rdz/rdstail.git
cd rdstail
make build        # produces bin/rdstail
```

### Docker

```bash
docker build -f deploy/Dockerfile -t rdstail:dev .
docker run --rm \
  -v $PWD/examples:/etc/rdstail \
  -v $PWD/state:/var/lib/rdstail \
  -p 9090:9090 \
  rdstail:dev run -c /etc/rdstail/config.yaml
```

### Pre-built releases

GoReleaser config is included (`.goreleaser.yml`) — cut a tag and
`goreleaser release` produces signed archives for `linux/amd64`, `linux/arm64`,
`darwin/amd64`, `darwin/arm64`.

---

## Quick start

1. **Write a config** (see `examples/` for four starting points):

   ```yaml
   # examples/s3-only.yaml
   sources:
     - type: rds
       engine: postgres
       region: ap-south-1
       instances: [prod-pg-writer, prod-pg-reader-1]

   sinks:
     - name: s3-primary
       type: s3
       s3:
         bucket: company-rds-logs
         region: ap-south-1
         prefix: rds/

   state:
     type: sqlite
     path: /var/lib/rdstail/state.db

   runtime:
     poll_interval: 15s
     max_workers: 4
     start_from: end

   metrics:
     enabled: true
     listen: :9090
   ```

2. **Validate** (schema only, no network):

   ```bash
   rdstail validate -c config.yaml
   ```

3. **Deep-validate** (optional; hits STS + RDS + S3 + HTTP):

   ```bash
   rdstail validate -c config.yaml --deep
   ```

4. **Run**:

   ```bash
   rdstail run -c config.yaml
   ```

5. **Scrape metrics**:

   ```bash
   curl -s localhost:9090/metrics | grep rdstail_
   ```

That's it. Kill the process and re-run: checkpoints resume, no replay explosion.

---

## Commands

| Command | Description |
|---|---|
| `run -c PATH` | Start the shipper. Blocks until SIGINT/SIGTERM. |
| `validate -c PATH [--deep]` | Schema-only by default; `--deep` probes STS, RDS (one DescribeDBLogFiles per instance), S3 HeadBucket, HTTP HEAD, and the state-store. Non-zero exit on any probe failure. |
| `version` | Print version, commit, and build date. |

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

### Config reference

```yaml
sources:                       # required; ≥ 1
  - type: rds                  # only "rds" in v1
    engine: postgres           # postgres | mysql | mariadb
    region: ap-south-1         # AWS region
    instances: [db-1, db-2]    # ≥ 1 DB identifier
    assume_role: ""            # optional role ARN for cross-account

sinks:                         # required; ≥ 1; every sink receives every record
  - name: s3-primary           # unique per config
    type: s3                   # s3 | kafka | http
    s3:
      bucket: my-logs
      region: ap-south-1
      prefix: rds/
      kms_key_id: ""           # empty → SSE-AES256; set → SSE-KMS
      max_bytes: 5242880       # batcher hints (currently advisory)
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

  - name: siem
    type: http
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
  poll_interval: 10s           # ≥ 1s
  max_workers: 5               # global sink-write pool
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
  rdstail run -c config.yaml
```

---

## Checkpoint semantics (the part that matters)

Per poll, per log file:

1. `prev = StateStore.Get(instance, logfile)` — on first sight `Marker=""`.
2. `DownloadDBLogFilePortion(Marker=prev.Marker)` → `data`, `nextMarker`, `pending`.
3. Parse lines, stamp each with `BatchID` derived from
   `sha256(instance|logfile|prev.Marker|nextMarker)`.
4. `Sink.Write(...)` — **sink must ACK durably** (S3 2xx, Kafka `acks=all`, HTTP 2xx).
5. Only **then** `StateStore.Set(...)` advances the marker.
6. If `AdditionalDataPending`, loop to step 2 within the same poll cycle.

Crashes between (4) and (5) cause at most one duplicate chunk on resume.
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
| `batch_bytes` | histogram | `sink_type` |
| `sink_write_duration_seconds` | histogram | `sink_type` |
| `state_store_ops_total` | counter | `op, outcome` |

Cardinality is bounded: `log_file` is the basename-only, capped at 64 chars,
and configs with >500 instances are rejected by default.

### Structured logs

Single-line JSON (`slog`) with a consistent context: `instance`, `engine`,
`log_file` where applicable. Pipe through `jq`:

```bash
rdstail run -c config.yaml 2>&1 | jq '{lvl: .level, msg, instance, log_file, err}'
```

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

A single rdstail instance comfortably handles ~100 RDS instances polled every
10 s. Per-instance cost in AWS API calls is roughly
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
cmd/rdstail/      CLI entrypoint
internal/app              runtime orchestrator
internal/cli              cobra command tree
internal/config           YAML schema, defaults, static validation
internal/logging          slog JSON setup
internal/metrics          Prometheus collectors + HTTP server
internal/awsx             AWS SDK v2 config helper
internal/source/rds       fetcher, classifier, RDSAPI interface
internal/state/{sqlite,file}
                          pluggable checkpoint stores
internal/sink             Sink interface, Fanout, retry/DLQ/metrics decorators
internal/sink/{s3,kafka,http,memory}
                          concrete sinks
internal/sink/factory     builds sinks from config
internal/pipeline         scheduler, per-instance worker, KeyedRunner
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
- ❌ Advanced log parsing (`LogRecord.Message` is the raw line; `Timestamp` is
  the fetch time, not the server-side log timestamp)
- ❌ Auto-discovery of RDS instances — you configure them explicitly
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
