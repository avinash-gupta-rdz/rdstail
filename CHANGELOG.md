# Changelog

All notable changes to rdstail. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer.

## [0.5.0] — 2026-10-11

### Fixed

- **Discovery tailed only one database.** With discovery-only sources (no
  explicit `instances:`), `max_instances_concurrent` defaulted to 1, so only
  one discovered instance was ever tailed and the others silently waited
  forever. The default is now 0 (one worker per resolved instance), and a cap
  below the instance count is logged as a warning.

### Added

- **Discover everything**: `discover: {all: true}` ingests every supported
  instance in a region — no tagging required — and `discover.exclude_tags`
  opts databases out (`"*"` matches any value).
- **`regions: [...]`** on a source: one block covers several regions
  (expanded into one source per region at load time).
- **Engine auto-detection for explicit instances**: `engine:` is now
  optional everywhere; each instance's engine is read from
  `DescribeDBInstances`, so one source can mix MySQL, MariaDB and PostgreSQL.
- **`--shard i/n`** (or `runtime.shard`): split a fleet across n rdstail
  processes with no overlap and no coordination (rendezvous hashing; resizing
  moves only ~1/n of the databases). The 500-instance cap now applies per
  shard.

## [0.4.0] — 2026-10-10

### Upgrading from 0.3.x

- **Add `rds:DescribeEvents`** to rdstail's IAM policy (`rdstail iam-policy`
  prints it). It powers reboot/failover detection; without it rdstail logs a
  warning and falls back to detecting restarts from the server's start banner.
- **Multi-line entries are now one record**: `message` may contain newlines
  (slow-query blocks, multi-line SQL, stack traces). Consumers that assumed one
  line per record should split on `\n` if they need lines.
- New record field `truncated` (only set when a single line exceeded RDS's
  1 MB response limit).
- **State schema v2**: `state.db` migrates in place on first start and 0.3.x
  can't read it afterwards — back up `state.db` if you may need to downgrade.
  Existing checkpoints are kept; nothing is re-shipped or skipped.
- `start_from: end` now means "the end when rdstail started", and only applies
  to files present the first time an instance is seen; files that appear later
  are read from their first line.

### Fixed

Found by testing against live RDS MySQL 8.4 and PostgreSQL 16 instances
(including Multi-AZ failovers and a 40-process throttling test).

- **Data loss on long lines.** `DownloadDBLogFilePortion` caps a response at
  1 MB; when the cap lands mid-line RDS cuts the line, appends
  `[Your log message was truncated]`, and the returned marker skips the rest
  of it. rdstail shipped the cut line as if complete. Truncated responses are
  now re-fetched from the same marker limited to the lines that arrived
  whole, so the cut line is read intact by the next call (verified
  byte-for-byte on 35 MB hour files full of 300 KB statements). Only a single
  line larger than 1 MB is unrecoverable; it ships with `"truncated": true`
  and increments `rdstail_read_anomalies_total{kind="truncated_line"}`.
- **MySQL/MariaDB hourly rotation lost or stalled data.** On rotation the
  shrunken live file was treated as truncated and its marker reset, dropping
  whatever the previous hour wrote after the last poll. Worse, after two or
  more rotations (rdstail down > 1 h) the RDS marker chain sticks at the end
  of the old hour forever and the file silently stops shipping. Rotation is
  now handled explicitly: the checkpoint is handed to the rotated
  `*.log.YYYY-MM-DD.H` file, later hours are read from their beginning, and
  already-read hours cost no API calls. With `start_from: beginning`, rotated
  hours are no longer read a second time.
- **Purged log hours went unnoticed.** A checkpoint pointing at an hour RDS
  has already deleted silently read the live file at that byte offset. It is
  now reported (`rdstail_read_anomalies_total{kind="gap"}` + an ERROR log)
  and every retained hour after it is read.
- **New files lost their first lines.** Files that appear after startup —
  every hourly PostgreSQL file, a log type enabled at runtime, files created
  while rdstail was down — were skipped to the end under `start_from: end`.
  `start_from` now applies only to files present when an instance is first
  seen; anything later is read from the beginning.
- **MySQL error lines shipped twice.** RDS copies `mysql-error.log` into
  `mysql-error-running.log` every ~5 minutes and both were shipped. The
  realtime file is read first; the running log ships only lines the realtime
  file lost to truncation.
- **Multi-line entries were split into one record per line.** A slow-query
  entry (`# Time`, `# User@Host`, `# Query_time`, `SET timestamp`, the SQL),
  multi-line SQL and stack traces now ship as one record, rejoined across
  chunk boundaries and crash-resumes. Records may therefore contain newlines.
- **General-log timestamps.** The tab-separated MySQL general log is now
  parsed; records carried fetch time before.
- **Reboots and Multi-AZ failovers.** Both lose the last few KB of each log
  file and the server appends new lines from the shorter end. rdstail either
  re-shipped the whole file (when the file came back smaller) or silently
  skipped the first lines written after the restart (when it had already
  regrown). Restarts are now detected from RDS events (`DescribeEvents`, one
  call per region per minute) and from server start banners; each file is
  re-read from just before the restart and lines already shipped are dropped.
  Verified over a reboot and three forced failovers under load: no line that
  RDS kept was missed. Lines RDS itself lost at the restart are unrecoverable
  by any reader.
- **Throttling.** `ThrottlingException` and the SDK's client-side
  "retry quota exceeded" are counted as `outcome="throttled"`; the AWS SDK
  runs in adaptive retry mode (client-side rate limiting), and a throttled
  instance doubles its poll interval with ±20% jitter (up to 2 m). Previously
  some processes in a throttled fleet kept polling at full rate and starved
  the rest for tens of minutes.
- **`start_from: end` skipped data written after startup** when the first
  skip-to-end was delayed (throttling, errors). The tail is now anchored at
  each file's size at startup.
- **Startup downloaded every file in full to find its end** (183 MB for one
  busy PostgreSQL hour file). The tail marker is now built from the file size
  and confirmed in two small calls.
- `cost-estimate` no longer prints a meaningless percentage when both sides
  round to $0.00.

### Added

- **`runtime.parallel_reads_per_file`** (default 1 = off): read a large
  backlog in one log file with that many concurrent byte-range requests. RDS
  serves one file sequentially at only ~0.3–0.9 MB/s; ranges own the lines
  that start in them, are verified to tile the file, and are parsed as one, so
  output is identical to a sequential read (verified byte-for-byte on 349 MB).
  Measured gain on a db.t4g.small: ~1.4× (RDS per-call latency rises with
  concurrency); larger instance classes may gain more. Bounded by the
  account's RDS API rate limit.
- `rdstail_read_anomalies_total{instance,kind}` — `gap`, `stall` (a file
  keeps growing but its marker does not advance), `truncated_line`, and
  `restart` (a reboot/failover was detected). Alert on any increase.
- SQLite state schema v2 (`skip_continuation` column); v1 databases migrate
  in place on open.

- **Compliance archive pack** — audit logging productised end to end:
  MySQL/MariaDB audit-plugin files (`audit/server_audit.log*`) are ingested
  behind a per-source `include_audit: true` opt-in (off by default — audit
  logs can be high-volume) with the event timestamp parsed from the plugin's
  CSV; pgAudit entries (which always flowed inside `postgresql.log`) now get
  their classification CSV lifted into a structured `audit` record field
  (`type`, `class`, `command`, `object_type`, `object_name` — the statement
  stays verbatim in `message`); and
  [`docs/compliance/audit-logs-immutable-s3.md`](docs/compliance/audit-logs-immutable-s3.md)
  is the assessor-ready recipe: engine setup, S3 Object Lock + lifecycle
  topology, and an evidence guide mapping rdstail's guarantees to
  SOC 2 / PCI-DSS control language. Plus `examples/audit-archive.yaml`.
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
