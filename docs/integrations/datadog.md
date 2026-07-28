# RDS logs → Datadog (without CloudWatch)

Datadog's [logs intake](https://docs.datadoghq.com/api/latest/logs/#send-logs)
accepts exactly what rdstail's `http` sink sends: a gzip'd JSON array. No
Lambda forwarder, no CloudWatch ingestion fee.

## Vendor side (2 steps)

1. Create an API key: **Organization Settings → API Keys**.
2. Optional, for `severity` to drive the status color: **Logs →
   Pipelines → your pipeline → add a Status Remapper** on the `severity`
   attribute.

## rdstail side

```yaml
sources:
  - type: rds
    engine: postgres
    region: us-east-1
    instances: [prod-pg-writer]

sinks:
  - name: datadog
    type: http
    http:
      # EU site: http-intake.logs.datadoghq.eu — match your Datadog site.
      url: https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=rdstail&ddtags=env:prod
      headers:
        DD-API-KEY: ${DD_API_KEY}
      gzip: true

state:
  type: sqlite
  path: ./state.db

runtime:
  start_from: end
  # Datadog caps a payload at 1000 log events / ~5 MB uncompressed. These
  # thresholds trip *after* the chunk that crosses them, so a flush can
  # overshoot by up to one pulled chunk (~1 MB) — leave headroom.
  max_batch_records: 500
  max_batch_bytes: 3145728
```

```bash
export DD_API_KEY=...           # expanded into the config at load time
rdstail run -c datadog.yaml
```

## What arrives

Each record lands as one log event. Datadog's default date remapper picks up
the `timestamp` attribute (server-side event time), `message` is the raw log
line, and `instance_id` / `engine` / `log_file` / `severity` / `marker` /
`batch_id` arrive as event attributes — facet `instance_id` to slice by
database.

```
status:ERROR
message   ERROR:  deadlock detected
@instance_id prod-pg-writer  @engine postgres  @severity ERROR
```

If your Datadog site ignores `ddsource` as a v2 query parameter (v2's
documented one is `ddtags`), set the source with a pipeline processor
instead — one click, same result.

## Notes

- A revoked key means HTTP 403 → the batch parks in the DLQ; fix the key and
  `rdstail dlq replay`. Nothing is lost, nothing blocks.
- Only ship what you'd pay Datadog to index:
  `filter: {min_severity: WARNING}` on the sink, with a parallel S3 sink for
  the full archive, is the usual fanout.
