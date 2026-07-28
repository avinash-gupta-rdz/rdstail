# RDS logs → Axiom

Axiom's [ingest API](https://axiom.co/docs/send-data/ingest) takes a JSON
array of events — the `http` sink's native output.

## Vendor side (2 steps)

1. Create a dataset, e.g. `rds-logs` (**Settings → Datasets**).
2. Create an API token with ingest permission for it (**Settings → API
   Tokens**).

## rdstail side

```yaml
sources:
  - type: rds
    engine: postgres
    region: us-east-1
    instances: [prod-pg-writer]

sinks:
  - name: axiom
    type: http
    http:
      # timestamp-field makes Axiom use the record's server-side event time
      # as _time instead of arrival time.
      url: https://api.axiom.co/v1/datasets/rds-logs/ingest?timestamp-field=timestamp
      headers:
        Authorization: Bearer ${AXIOM_TOKEN}
      gzip: true

state:
  type: sqlite
  path: ./state.db

runtime:
  start_from: end
```

```bash
export AXIOM_TOKEN=xaat-...
rdstail run -c axiom.yaml
```

## What arrives

One event per log line: `_time` from the record's `timestamp`, and
`instance_id`, `engine`, `log_file`, `severity`, `message`, `batch_id` as
fields.

```
_time                       severity  instance_id     message
2026-07-29T12:04:05.123Z    ERROR     prod-pg-writer  ERROR:  deadlock detected
```

APL to dedupe at-least-once deliveries, if you ever replay:

```
['rds-logs'] | summarize arg_max(_time, *) by batch_id, message
```
