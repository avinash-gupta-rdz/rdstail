# Integration recipes

rdstail already reaches almost every log backend through two sinks people
overlook: `http` (batched JSON to any endpoint) and `stdout` (NDJSON to any
agent). These recipes are copy-paste paths from RDS logs to the tools you
already run — **no CloudWatch in the middle, no glue code**.

| Backend | Route | Recipe |
|---|---|---|
| Datadog | `http` sink, direct | [datadog.md](datadog.md) |
| Axiom | `http` sink, direct | [axiom.md](axiom.md) |
| Better Stack | `http` sink, direct | [betterstack.md](betterstack.md) |
| Splunk (HEC) | `stdout` → vector | [splunk.md](splunk.md) |
| Grafana Loki | `stdout` → vector | [loki.md](loki.md) |
| Elasticsearch / OpenSearch | `stdout` → vector | [elastic.md](elastic.md) |
| vector (anything it ships to) | `stdout` pipe | [vector.md](vector.md) |
| fluent-bit (anything it ships to) | `stdout` pipe | [fluent-bit.md](fluent-bit.md) |

## How the two routes work

**Direct (`http` sink):** each batch is one `POST` with a JSON **array** of
records, `Content-Type: application/json`, optional `Content-Encoding: gzip`,
plus your configured headers. Vendors whose ingest API accepts a JSON array
(Datadog, Axiom, Better Stack, and most SIEM webhooks) need nothing between
rdstail and them. Retries with backoff are built in; 4xx responses park the
batch in the [DLQ](../../README.md#dlq-lifecycle) instead of blocking, so a
revoked API key never wedges the pipeline.

**Pipe (`stdout` sink):** rdstail writes one JSON record per line to stdout
(its own logs go to stderr), and vector / fluent-bit speak whatever framing
the backend demands — HEC envelopes for Splunk, streams for Loki, `_bulk`
for Elasticsearch. Use this route when the backend needs a protocol, not
just JSON.

## The record on arrival

Every record, on either route, looks like:

```json
{
  "instance_id": "prod-pg-writer",
  "engine": "postgres",
  "log_file": "error/postgresql.log.2026-07-29-12",
  "timestamp": "2026-07-29T12:04:05.123456Z",
  "severity": "ERROR",
  "message": "ERROR:  deadlock detected",
  "marker": "4:12345",
  "batch_id": "a1b2c3d4e5f60708"
}
```

`timestamp` is the engine's server-side time when parseable (fetch time
otherwise), `severity` is the engine's own level token (omitted when the
line has none), `message` is the raw line, `marker` is AWS's opaque
pagination cursor for the chunk, and `batch_id` dedupes at-least-once
deliveries downstream.

## Secrets

Configs support `${VAR}` expansion at load time — every recipe reads its
API key from the environment so nothing sensitive lands in the YAML:

```yaml
headers:
  Authorization: Bearer ${AXIOM_TOKEN}
```

References to unset variables are left verbatim — the literal `${NAME}`
shows up at the backend as a failed auth header (parking batches safely in
the DLQ) instead of silently becoming an empty string.
