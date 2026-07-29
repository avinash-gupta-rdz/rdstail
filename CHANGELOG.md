# Changelog

All notable changes to rdstail. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer.

## [Unreleased]

### Added

- **Incident-time toolkit** — `rdstail tail` grows `--since 1h` (replay a
  time window before following; rotated files outside the window are never
  downloaded), `--grep 'deadlock|timeout'` (RE2 filter on the raw line), and
  a repeatable `-i` to tail a primary and its replicas together. New
  **`rdstail dump`** command: one-shot fetch-and-exit of a time window
  (`dump -i my-db --since 24h -o incident-4231.ndjson.gz`) — NDJSON to
  stdout or a file, gzipped when the path ends in `.gz`, record count to
  stderr. No config, no state, read-only IAM.

## [0.3.0] — 2026-10-08

### Added

- **Distribution** — releases now publish static binaries (Linux/macOS ×
  amd64/arm64) under stable `releases/latest/download/rdstail_<os>_<arch>.tar.gz`
  URLs, a multi-arch image at `ghcr.io/avinash-gupta-rdz/rdstail`, a Homebrew
  cask (`brew install avinash-gupta-rdz/tap/rdstail`), and an `install.sh`
  that verifies the SHA-256 before installing. Archive names no longer carry
  the version; releases are published directly instead of as drafts.
- **`rdstail init`** — interactive onboarding: lists the RDS instances your
  ambient credentials can see, you pick instances and a sink (S3 bucket
  picker / Kafka / HTTP webhook / stdout), and a commented, validated
  `rdstail.yaml` is written with the exact next commands printed
  (`iam-policy`, `validate --deep`, `run`). Listing failures fall back to
  manual entry; mixed-engine selections split into one source per engine.
- **Integration recipes** — copy-paste paths from RDS logs to the tools you
  already run, in `docs/integrations/`: Datadog, Axiom, and Better Stack
  direct via the `http` sink (their intake APIs accept rdstail's batched
  JSON as-is), and Splunk HEC, Grafana Loki, and Elasticsearch/OpenSearch
  via `stdout` → vector / fluent-bit. Plus `examples/datadog.yaml`.
- **`${VAR}` expansion in configs** — environment-variable references
  anywhere in the YAML are substituted at load time, so API keys and SASL
  passwords stay out of the file. Unset references are left verbatim
  (visible, never a silent empty string); bare `$VAR` is untouched.
- **`rdstail cost-estimate`** — project *your* fleet's monthly log volume
  from `DescribeDBLogFiles` metadata (free; no log data downloaded) and price
  the CloudWatch export path against the rdstail path. Works from a config
  (`-c`, including tag discovery) or with no config at all
  (`--region [-i INSTANCE]...`). `--json` for scripting; list prices and the
  compression ratio overridable via flags.
- **`rdstail iam-policy`** — derive the least-privilege IAM policy from the
  config itself: RDS log reads scoped to the configured instances (region
  wildcard when tag discovery is used), `s3:PutObject` scoped to
  bucket+prefix, KMS and `sts:AssumeRole` statements only when the config
  uses them. `--deep` adds the `validate --deep` probe permissions,
  `--terraform` emits an `aws_iam_policy_document` data source. Permissions
  that belong on an assumed role in another account are reported as stderr
  notes instead of being wrongly granted to the local principal.
- **DLQ lifecycle commands** — `rdstail dlq list` (with `--json`),
  `rdstail dlq replay` (re-delivers parked batches; rows deleted only after a
  durable sink ACK; `--dry-run`, `--sink`, `--limit`), and
  `rdstail dlq purge --yes`. Replay builds sinks without the DLQ decorator so
  a still-broken sink fails loudly instead of silently re-parking.
- **Adaptive polling** — `runtime.poll_interval_max` + `poll_backoff_multiplier`:
  idle instances back off exponentially and snap back to `poll_interval` the
  moment data flows. New gauge `rdstail_poll_interval_seconds{instance}`.
- **Server-side timestamps & severity** — records now carry the engine's log
  timestamp and upper-cased severity token (PostgreSQL fixed RDS prefix,
  MySQL 5.7/8.0, MariaDB, slow-query `# Time:` headers). Continuation lines
  inherit the preceding timestamp within a chunk; fetch-time fallback
  otherwise. New `severity` field in the record JSON (omitted when empty).
- **Cross-chunk batching** — `runtime.max_batch_bytes` (5 MiB default) /
  `max_batch_records` (10k default) coalesce chunks within a poll cycle into
  fewer, larger sink writes. Checkpoints advance only at flush points, after
  the ACK; `0`/`0` restores per-chunk writes.
- **Tag-based instance discovery** — `sources[].discover.tags` (AND
  semantics) selects instances at startup; engine auto-detected from AWS
  (Aurora normalised); union with explicit `instances`, deduplicated. New
  `rdstail discover` command previews matches.
- **Periodic re-discovery** — `discover.refresh_interval` (≥ 30s) reconciles
  the worker set live: workers start for new fleet members, stop for departed
  ones, and crashed workers restart if still desired. Failed discovery passes
  keep the current set.
- **Kafka TLS/SASL** — `tls: true` and `sasl_mechanism: plain |
  scram-sha-256 | scram-sha-512` are now actually applied (previously the
  config fields were accepted but ignored). Kafka `validate --deep` probe
  pings brokers with the sink's exact connection settings.
- **Per-sink severity routing** — `sinks[].filter: {min_severity: ERROR}` or
  `{severities: [FATAL, PANIC]}`: archive everything to S3 while only
  error-and-worse reaches the alerting webhook.
- **Stdout sink** — `type: stdout` (`format: ndjson | text`) for pipe
  integration with vector / fluent-bit / jq; rdstail logs stay on stderr.
- **`rdstail tail`** — zero-config `tail -f` for one instance: engine
  auto-detected, in-memory state, `--min-severity`, `--format ndjson`,
  `--from-beginning`.
- **Cross-file parallelism** — `runtime.max_workers` now bounds concurrent
  file drains globally across instances (previously unused; `1` = the old
  fully-serial behaviour).
- **DLQ depth metric** — `rdstail_dlq_depth{sink_name}` gauge (30s refresh)
  plus ready-made Prometheus alert rules in `docs/ops/prometheus-alerts.yml`
  and a Grafana dashboard in `docs/ops/grafana-dashboard.json`.
- **Cross-account S3 sink** — `sinks[].s3.assume_role` (+ `external_id`)
  writes directly into a central log-archive account's bucket; the
  `validate --deep` probe assumes the same role.
- **Checkpoint GC** — `rdstail state gc [--older-than] [--dry-run]` prunes
  rows for rotated-out files / departed instances (previously the SQLite
  state grew forever); `runtime.checkpoint_retention` (≥ 24h) enables an
  automatic 6-hourly sweep.
- **CI / release automation** — GitHub Actions: race-enabled test + example
  validation on every push/PR; goreleaser release on `v*` tags.
- README: honest CloudWatch-vs-rdstail cost comparison; systemd unit shipped
  in `deploy/rdstail.service`.

### Changed

- `sinks[].s3.max_bytes` / `max_records` batching hints are superseded by the
  runtime-level `max_batch_*` keys (legacy fields remain accepted).
- The `Sink` contract now requires safety under concurrent `Write` calls for
  different (instance, logfile) keys.
- `Timestamp` on records is the server-side event time when parseable (was:
  always fetch time).

### Removed

- `pipeline.KeyedRunner` (never wired in): replaced by a global drain
  semaphore — same bound, no per-key goroutine growth on rotated file names.

## [0.1.0] — initial release

- RDS log tailing (`DescribeDBLogFiles` + `DownloadDBLogFilePortion`) for
  PostgreSQL / MySQL / MariaDB with marker checkpoints, rotation detection,
  at-least-once delivery, and dedupe-friendly batch IDs.
- Sinks: S3 (NDJSON + gzip), Kafka (`acks=all`), HTTP webhook; fanout.
- SQLite (default) and JSON-file state stores; DLQ table for terminal sink
  failures.
- Prometheus metrics, health endpoints, structured JSON logs.
- `run`, `validate [--deep]`, `version` commands; Dockerfile; goreleaser
  config.
