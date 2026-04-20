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
4. `Sink.Write(...)` — sink MUST ack durably (S3 2xx, Kafka `acks=all`, HTTP 2xx).
5. Only then `StateStore.Set(instance, logfile, {Marker: nextMarker, ...})`.
6. If `AdditionalDataPending`, loop to (2) in the same poll.

Crashes between (4) and (5) cause at most one duplicate chunk on resume. The
`BatchID` on each record lets downstream consumers dedupe if they need exactly-once.

## Concurrency

- **One goroutine per RDS instance.** Iterates that instance's log files serially
  — preserves per-file ordering, bounds per-instance API concurrency.
- **Bounded parallelism across instances** via `runtime.max_instances_concurrent`.
- **Shared sink write path.** All sinks share the same `Sink` (single sink or a
  `Fanout` over many). Each `Write` is synchronous from the worker's point of view;
  `runtime.max_workers` governs how many concurrent writes the decorators allow
  (via `KeyedRunner` in the sink pool — currently simple sequential within an
  instance).
- **Graceful shutdown.** SIGINT/SIGTERM cancels the root ctx. Instance workers
  finish their current pull, flush their checkpoint, and exit. `runtime.shutdown_timeout`
  is the upper bound; after that the scheduler returns even if workers are stuck.

## Rotation handling

On every poll, `DescribeDBLogFiles` is called. For each file:

| Condition | Action |
|---|---|
| File not in state store | Apply `runtime.start_from`: `beginning` → `Marker="0"`; `end` → `SkipToEnd` once, persist tail marker. |
| `file.Size < prev.FileSize` | Truncation (rotate-in-place). Reset `Marker="0"`. |
| File no longer returned | Assumed rotated out. State remains; will eventually be manually GC'd. |

## State store

- **SQLite** (default) — `modernc.org/sqlite` (pure Go, no CGO). Schema has
  `schema_version`, `checkpoints` (PK: `instance_id, log_file`), and `sinks_dlq`.
  WAL journaling + `synchronous=NORMAL` + 5 s busy timeout. `SetMaxOpenConns(1)`
  serialises writes because SQLite allows only one writer.
- **File** (dev fallback) — JSON document on disk, written via temp-file +
  atomic `rename`. Mutex-synchronised. No DLQ support.

## Sinks

All built sinks are wrapped (innermost-first) as:
`metrics → retry → DLQ`. So:

- Retries are visible in `sink_write_duration_seconds`.
- Only post-retry *terminal* failures end up in the `sinks_dlq` table (or in
  `logs_failed_total`).
- 4xx responses from HTTP and `PermanentError` wraps are short-circuited past
  retry directly to DLQ.

### S3
- NDJSON + gzip per batch.
- Key: `{prefix}/{instance}/{engine}/{logfile}/{YYYY/MM/DD}/{unix-ms}-{batch_id}.ndjson.gz`.
- Single `PutObject` per batch (no multipart — batches are ~1 MB).
- SSE: `AES256` default, `aws:kms` if `kms_key_id` is set.

### Kafka
- `franz-go` producer, `acks=all`, idempotent, zstd compression.
- Key: `instance|logfile` (partition affinity → per-file order preserved).
- Topic: explicit `topic` OR `topic_template` with `{engine}`/`{instance}` substitutions.

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
  - `rdstail_batch_bytes{sink_type}`
  - `rdstail_sink_write_duration_seconds{sink_type}`
  - `rdstail_state_store_ops_total{op, outcome}`
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

- **No advanced parsing.** `LogRecord.Timestamp` is the fetch time, not the
  server-side log line timestamp (engine-specific parsing is explicitly
  out-of-scope per PRD §3).
- **No adaptive polling.** Fixed `poll_interval`. Phase 9 adds backoff-on-empty
  and speed-up-on-pending.
- **No stateful S3 batcher.** One `PutObject` per RDS chunk (~1 MB). A batcher
  that coalesces across chunks is a future enhancement.
- **`max_workers` is currently unused** as a concurrency knob because each
  instance worker writes serially. The plumbing for a shared write pool lives in
  `pipeline.KeyedRunner` and will activate when we add cross-file parallelism.
- **Kafka `--deep` probe** is not implemented; broker client needs the full
  TLS/SASL plumbing.
