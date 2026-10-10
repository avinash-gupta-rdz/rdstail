# rdstail — Architecture

## Data flow

```
                      ┌──────────────────┐
                      │   RDS instance   │
                      │ (log files API)  │
                      └────────┬─────────┘
                               │  DescribeDBLogFiles
                               │  DownloadDBLogFilePortion
                               ▼
                   ┌──────────────────────┐
                   │    Fetcher (RDS)     │◀── engine-specific
                   │  marker pagination   │    LogFileClassifier
                   │  rotation detection  │
                   └──────────┬───────────┘
                              │  *Chunk (records + markers)
                              ▼
                   ┌──────────────────────┐
                   │  InstanceWorker      │
                   │  per RDS instance    │
                   │  iterates files      │
                   │  serially            │
                   └─────┬──────────┬─────┘
                         │          │
                         │          │  (after sink ACK)
               ┌─────────▼─────┐  ┌─▼──────────────────┐
               │     Sink      │  │    StateStore      │
               │ (retry + DLQ  │  │  SQLite (default)  │
               │  + metrics)   │  │  JSON file (alt)   │
               └─────┬─────────┘  └────────────────────┘
                     │
                     ▼
         ┌────────┬─────────┬────────────┐
         │   S3   │  Kafka  │  HTTP      │
         │  sink  │  sink   │  webhook   │
         └────────┴─────────┴────────────┘
```

## Checkpoint semantics (at-least-once)

Per poll, per logfile:

1. `prev = StateStore.Get(instance, logfile)`; on first sight, `Marker=""`.
2. `DownloadDBLogFilePortion(Marker=prev.Marker)` → `data`, `nextMarker`, `pending`.
3. Parse to `[]LogRecord`, stamp each with
   `BatchID = sha256(instance|logfile|prev.Marker|nextMarker)[:16]`.
4. Records accumulate across chunks up to `runtime.max_batch_bytes` /
   `max_batch_records`; at a flush point, `Sink.Write(...)` — sink MUST ack
   durably (S3 2xx, Kafka `acks=all`, HTTP 2xx).
5. Only then `StateStore.Set(instance, logfile, {Marker: <last chunk's nextMarker>, ...})`.
6. If `AdditionalDataPending`, loop to (2) in the same poll.

Crashes between (4) and (5) cause at most one duplicate batch (bounded by the
batch thresholds) on resume. The per-chunk `BatchID` on each record — preserved
through coalescing — lets downstream consumers dedupe if they need exactly-once.

## Concurrency

- **One goroutine per RDS instance**, bounded across instances via
  `runtime.max_instances_concurrent`.
- **Cross-file parallelism** via `runtime.max_workers`: a single global
  semaphore bounds how many log files are being drained (fetch → write →
  checkpoint) concurrently across ALL instances. `max_workers: 1` keeps every
  instance serial. Per-file ordering needs no queueing machinery: each file is
  drained by exactly one goroutine within a poll, polls never overlap, and
  checkpoints are per-file — there is no cross-file state to race on.
- **Shared sink write path.** All sinks share the same `Sink` (single sink or a
  `Fanout` over many); implementations must tolerate concurrent `Write` calls
  for different files. Writes for one (instance, logfile) are always serial.
- **Graceful shutdown.** SIGINT/SIGTERM cancels the root ctx. Instance workers
  finish their current pull, flush their checkpoint, and exit. `runtime.shutdown_timeout`
  is the upper bound; after that the scheduler returns even if workers are stuck.

## Instance discovery

Sources may select instances by tag (`discover.tags`, AND semantics) instead
of — or in addition to — an explicit list. At startup, `DescribeDBInstances`
(already in the minimum IAM policy) is paginated, each instance's `TagList` is
matched, its `Engine` is normalised (`aurora-postgresql` → postgres,
`aurora`/`aurora-mysql` → mysql; unsupported engines are skipped), and the
union with explicit instances — deduplicated by (region, instance ID) —
becomes the worker set. A source-level `engine` additionally filters
discovery. `rdstail discover` previews matches without starting the pipeline.
The 500-instance cap is re-checked after every discovery pass.

With `discover.refresh_interval` set (≥ 30s), the scheduler re-resolves the
desired set on that cadence and **reconciles**: workers start for instances
that joined the fleet, workers for departed instances are cancelled (their
checkpoints remain, so a returning instance resumes), and a worker that
previously exited on error is restarted if its instance is still desired. A
failed re-discovery pass logs a warning and keeps the current worker set —
never tears down workers on a flaky API call. Explicit `instances` are part of
every refresh result, so they are never reconciled away.

## Log metadata extraction

`internal/parse` lifts two attributes from each raw line, best-effort, per
engine — the event **timestamp** and the **severity** token. The raw line is
never modified; `LogRecord.Message` stays verbatim.

- **PostgreSQL** — RDS pins `log_line_prefix` to `%t:%r:%u@%d:[%p]:` (not
  user-changeable), so the prefix regex is reliable:
  `2026-07-21 10:15:32 UTC:10.0.1.5(53422):app@orders:[12345]:ERROR:  ...`
  Severities: DEBUG1-5, LOG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC, plus
  continuation tokens (STATEMENT, DETAIL, HINT, CONTEXT).
- **MySQL** — `2026-07-21T10:15:32.835618Z 8 [Warning] [MY-010055] [Server] …`
  (5.7 form without err-code also matches). Severities normalised upper-case:
  SYSTEM, ERROR, WARNING, NOTE. Slow-query `# Time:` headers yield a timestamp
  with no severity.
- **MariaDB** — `2026-07-21 10:15:36 0 [Note] …`.

Rules: a matching line gets the server-side timestamp and severity; a
non-matching line (stack trace, wrapped query text) inherits the nearest
preceding timestamped line's time **within the same chunk** and carries no
severity; if nothing has matched yet, fetch time is used — the pre-extraction
behaviour. Unknown engines skip extraction entirely.

## Adaptive polling

Per-instance, opt-in via `runtime.poll_interval_max > poll_interval`:

- A poll that ships ≥ 1 record resets the interval to `poll_interval` (the
  fast/base rate).
- An idle poll multiplies the interval by `poll_backoff_multiplier` (default
  2.0), capped at `poll_interval_max`.
- The current interval is exported as `rdstail_poll_interval_seconds{instance}`.

Worst-case added latency after an idle stretch is one (backed-off) interval —
the first poll that finds data immediately snaps back to base, and
`AdditionalDataPending` chunks are always drained within the same poll. Each
instance backs off independently, so one chatty database doesn't keep a fleet
of quiet ones polling fast. With `poll_interval_max` unset (default), polling
is fixed-interval exactly as before.

## Rotation handling

On every poll, `DescribeDBLogFiles` is called, then rotation is planned before
any file is drained (`InstanceWorker.planRotation`).

**MySQL/MariaDB hourly rotation.** RDS renames the live general, slow-query and
error-running logs to `<base>.YYYY-MM-DD.H` every hour. Their markers are
`YYYY-MM-DD.H:<byte offset>`; the hour ID names the file the offset belongs to.
Observed on RDS MySQL 8.4: on the base name the marker chain follows one
rotation but sticks at the old hour's end (pending=false, no data) after two
or more; a marker for a purged hour silently reads the live file at that
offset; a rotated file read by name honours the offset and ignores the hour
ID. So rotation is explicit:

| Base checkpoint | Rotated `<base>.H` files | Base file |
|---|---|---|
| In the live hour | Older hours — skipped, no API call | Continue |
| In rotated hour H | H continues from the checkpoint offset; later hours from 0 | Restart at `0` |
| In a purged hour | `gap` anomaly; every retained hour from 0 | Restart at `0` |
| None (first run) | Backfilled only when the base starts from the beginning | `start_from` |

A tracked rotated file is drained until its offset reaches its (immutable) size.

**Other files.**

| Condition | Action |
|---|---|
| File not in state store | `runtime.start_from` applies only to files present at the first discovery of an instance with no checkpoints; any later file is new data → `Marker="0"`. `end` → `SkipToEnd` once, persist tail marker. |
| `file.Size < prev.FileSize` | Truncation in place (e.g. `mysql-error.log`). Reset `Marker="0"`. |
| File no longer returned | Assumed rotated out. Its checkpoint row is pruned by `rdstail state gc` or automatically when `runtime.checkpoint_retention` is set (rows untouched longer than the retention; 6-hourly sweep). |
| File grows but marker doesn't move for 3 polls | `stall` anomaly + ERROR log. |

**MySQL error log.** RDS appends `mysql-error.log` to `mysql-error-running.log`
every ~5 minutes and truncates it. The realtime file is drained first; the
running log is drained afterwards and skips any line already acknowledged from
the realtime file (an in-memory hash set, one hour TTL), so it only contributes
lines the realtime file lost to truncation. After a restart the set is empty:
up to ~5 minutes of error lines may be shipped twice (at-least-once).

## Multi-line records and truncation

A line the engine parser recognises (timestamp or severity) starts a record;
other lines are continuations appended to it (capped at 1 MiB), so a
slow-query entry or multi-line statement is one record. A record that may
continue into the next chunk is held back at mid-pagination flushes and the
checkpoint is set to the marker of the chunk it began in, with
`skip_continuation` recording that the chunk opens with continuation lines of
an already-shipped record. A resume therefore re-pulls at most one chunk and
never ships a fragment.

When RDS truncates a response at 1 MB (`[Your log message was truncated]`),
the chunk is re-fetched with `NumberOfLines` set to the lines that arrived
whole. A single line over 1 MB ships cut with `truncated: true`.

## Reboots and Multi-AZ failovers

Observed live: after a reboot or failover every log file comes back a few KB
shorter (unflushed writes are lost) and the server appends new lines from that
shorter end. A stored offset past the new end would skip those lines;
resetting to 0 re-ships the file. Instead:

- **Detection.** The scheduler polls `DescribeEvents` (one call per region per
  minute for the whole fleet) for `failover` events and `availability` events
  mentioning restart/reboot/shutdown, and calls `InstanceWorker.NotifyRestart`.
  Workers also notice MySQL's `ready for connections` / PostgreSQL's
  `database system is ready to accept connections` in lines they are about to
  ship, and a file whose size dropped below its checkpoint. Notices within
  5 minutes of each other are one restart.
- **Recovery.** On its next drain each file rewinds 256 KB before the
  checkpoint it held a minute before the restart (each worker keeps a short
  in-memory history of checkpoints), drops a possibly partial first line, and
  skips records whose hash is in its in-memory ring of recently shipped
  records (the last 2 MB per file, at most 50k records). Each restart is counted in
  `rdstail_read_anomalies_total{kind="restart"}`.
- Lines RDS lost at the restart are gone for every reader. After an rdstail
  restart the ring is empty, so a rewind may re-ship up to 256 KB.

## Parallel range reads

`runtime.parallel_reads_per_file: N` (> 1) reads a backlog of ≥ N MB in waves of
N concurrent 2 MB byte ranges (markers are `<prefix>:<offset>`). A range owns
the lines that **start** in it: it reads from one byte early, discards through
the first newline, and reads past its end until the next line start, so ranges
tile the file exactly; the worker verifies the tiling (else falls back to
sequential for that wave). The wave's text is parsed as one, so grouping and
order equal a sequential read; the wave ships and checkpoints at the start of
its last record, which may continue into the next wave. A line over 1 MB, or
any byte/offset mismatch, falls back to sequential reading. Throughput scales
with N until the account's RDS API rate limit (≈ 14 calls/s per region).

## State store

- **SQLite** (default) — `modernc.org/sqlite` (pure Go, no CGO). Schema has
  `schema_version`, `checkpoints` (PK: `instance_id, log_file`), and `sinks_dlq`.
  WAL journaling + `synchronous=NORMAL` + 5 s busy timeout. `SetMaxOpenConns(1)`
  serialises writes because SQLite allows only one writer.
- **File** (dev fallback) — JSON document on disk, written via temp-file +
  atomic `rename`. Mutex-synchronised. No DLQ support.

## Sinks

All built sinks are wrapped (innermost-first) as:
`metrics → retry → DLQ → filter`. So:

- A sink with `filter` set sees only matching records; a fully-filtered batch
  ACKs immediately without touching metrics/retry/DLQ, so checkpoints advance
  normally. `min_severity` ranks DEBUG < LOG/INFO/NOTICE/NOTE/SYSTEM <
  WARNING < ERROR < FATAL < PANIC across engines; records without a rankable
  severity (continuation lines, unparsed formats) never pass `min_severity`
  — an alert route drops them by design, while unfiltered sinks still archive
  them.

- Retries are visible in `sink_write_duration_seconds`.
- Only post-retry *terminal* failures end up in the `sinks_dlq` table (or in
  `logs_failed_total`).
- 4xx responses from HTTP and `PermanentError` wraps are short-circuited past
  retry directly to DLQ.

### DLQ replay (`rdstail dlq`)

`internal/replay` is the recovery half of the DLQ contract. `rdstail dlq replay`
pages through `sinks_dlq` oldest-first (keyset pagination on the row ID), and
per row:

1. Build the sink named on the row (lazily, first use) — wrapped with the
   config's retry policy but **not** the DLQ decorator, so a still-broken sink
   fails loudly instead of re-parking the batch behind a nil error.
2. Unmarshal the stored `[]LogRecord` payload and `Write` it.
3. `DLQDelete` the row **only after** the sink ACKs. A crash between write and
   delete re-sends the batch on the next replay — at-least-once, dedupable via
   the records' original `BatchID`.

Failed and undecodable rows stay in place; rows for sinks no longer in the
config are skipped and reported. Replay is idempotent to re-run and safe to run
while `rdstail run` is live (SQLite WAL + busy timeout handle the second
writer). `dlq list` / `dlq purge --yes` complete the lifecycle.

### S3
- NDJSON + gzip per batch.
- Cross-account: `assume_role` (+ optional `external_id`) assumes a role in
  the bucket's account for the writes; the deep probe uses the same identity.
- Key: `{prefix}/{instance}/{engine}/{logfile}/{YYYY/MM/DD}/{unix-ms}-{batch_id}.ndjson.gz`.
- Single `PutObject` per batch (no multipart — batches are bounded by
  `runtime.max_batch_bytes`, 5 MiB default).
- SSE: `AES256` default, `aws:kms` if `kms_key_id` is set.

### Kafka
- `franz-go` producer, `acks=all`, idempotent, zstd compression.
- Connection security: `tls: true` for TLS; `sasl_username`/`sasl_password`
  with `sasl_mechanism: plain | scram-sha-256 | scram-sha-512` (default plain).
  The same options drive the `validate --deep` broker ping.
- Key: `instance|logfile` (partition affinity → per-file order preserved).
- Topic: explicit `topic` OR `topic_template` with `{engine}`/`{instance}` substitutions.

### Stdout
- One line per record to standard output: NDJSON (default) or `text` (raw
  message only). Flushed per batch; `Write` ACKs once the pipe accepts the
  bytes. rdstail's own logs go to stderr, so stdout stays clean for data —
  designed for `rdstail run | vector` / fluent-bit / `jq`.

### HTTP webhook
- `POST application/json` (optional `Content-Encoding: gzip`).
- Adds headers `X-Batch-Id`, `X-Instance-Id`, `X-Log-File`.
- `2xx` → success; `4xx` → permanent (DLQ); `5xx`/network → retryable.

## Observability

- Prometheus collectors (process-wide registry). HTTP server on
  `metrics.listen` exposes `/metrics`, `/healthz`, `/readyz`.
- Key metrics:
  - `rdstail_logs_processed_total{instance, engine, log_file, sink_type}`
  - `rdstail_logs_failed_total{instance, sink_type, reason}`
  - `rdstail_ingestion_lag_seconds{instance, log_file}`
  - `rdstail_api_calls_total{operation, outcome}`
  - `rdstail_poll_interval_seconds{instance}`
  - `rdstail_batch_bytes{sink_type}`
  - `rdstail_sink_write_duration_seconds{sink_type}`
  - `rdstail_state_store_ops_total{op, outcome}`
  - `rdstail_dlq_depth{sink_name}` — parked batches, refreshed every 30s;
    example alert rules in `docs/ops/prometheus-alerts.yml`
- Cardinality bound: `log_file` label is basename-only and capped at 64 chars.

## IAM policy (minimum)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": [
        "rds:DescribeDBInstances",
        "rds:DescribeDBLogFiles",
        "rds:DownloadDBLogFilePortion"
      ], "Resource": "*" },
    { "Effect": "Allow", "Action": [
        "s3:PutObject"
      ], "Resource": "arn:aws:s3:::my-log-bucket/rds/*" },
    { "Effect": "Allow", "Action": [
        "sts:GetCallerIdentity"
      ], "Resource": "*" }
  ]
}
```

Add `kms:Encrypt` / `kms:GenerateDataKey` on the KMS key ARN if using SSE-KMS;
add `sts:AssumeRole` for cross-account.

## Known limitations (v1)

- **Metadata extraction only, not parsing.** Timestamp + severity are lifted
  from lines that match the engine's known format (see "Log metadata
  extraction"); query text, fields, and parameters stay opaque. Structuring
  log content remains out-of-scope per PRD §3.
- **Batching is per poll-cycle.** Chunks coalesce across the pagination loop
  (`runtime.max_batch_bytes` / `max_batch_records`), but a batch never spans
  poll cycles — a file with little new data per poll still produces one small
  object per poll. Cross-poll buffering would delay checkpoints across polls
  and is deliberately out of scope.
