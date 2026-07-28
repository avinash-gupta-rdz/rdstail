# RDS logs → Elasticsearch / OpenSearch

Elasticsearch ingests through the `_bulk` API's action/document NDJSON
pairs — not a plain JSON array — so this recipe pipes rdstail's `stdout`
sink through vector's `elasticsearch` sink (which also speaks OpenSearch).

## Vendor side (2 steps)

1. Create (or let the first bulk write create) a daily index pattern, e.g.
   `rds-logs-*`.
2. An API key or basic-auth user with write access to it.

## rdstail side

Use the exact `rdstail.yaml` from [vector.md](vector.md) (stdout sink,
ndjson).

## vector side

```yaml
# vector.yaml
sources:
  rdstail:
    type: stdin
    decoding:
      codec: json

transforms:
  rds_ts:                        # string → real timestamp, so the daily
    type: remap                  # index is the event's day, not arrival day
    inputs: [rdstail]
    source: |
      .timestamp = parse_timestamp!(.timestamp, "%+")

sinks:
  es:
    type: elasticsearch
    inputs: [rds_ts]
    endpoints: ["https://es.example.com:9200"]
    api_version: auto            # works for OpenSearch too
    bulk:
      index: "rds-logs-%Y.%m.%d"
    auth:
      strategy: basic
      user: rdstail-writer
      password: ${ES_PASSWORD}
    buffer:
      type: disk
      max_size: 268435488
```

## Run

```bash
export ES_PASSWORD=...
rdstail run -c rdstail.yaml | vector --config vector.yaml
```

## What arrives

One document per log line in `rds-logs-YYYY.MM.dd`:

```json
{
  "timestamp": "2026-07-29T12:04:05.123456Z",
  "instance_id": "prod-pg-writer",
  "engine": "postgres",
  "severity": "ERROR",
  "message": "ERROR:  deadlock detected",
  "marker": "4:12345",
  "batch_id": "a1b2c3d4e5f60708"
}
```

Map `timestamp` as the index pattern's time field in Kibana/Dashboards.
For exactly-once semantics at query time, aggregate on `batch_id` +
`message`, or set vector's `id_key` to a fingerprint you compute in a
`remap` transform to make replays idempotent upserts.
