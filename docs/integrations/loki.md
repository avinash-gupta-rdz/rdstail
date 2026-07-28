# RDS logs → Grafana Loki

Loki ingests via its own push protocol (streams + label sets), not plain
JSON arrays — so this recipe pipes rdstail's `stdout` sink through vector's
`loki` sink. Labels come from record fields; the raw line stays the log
body.

## Vendor side

Nothing to create — just the push endpoint (`http://<loki>:3100`, or your
Grafana Cloud logs URL + API key).

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
  rds_ts:                        # string → real timestamp, so Loki stores
    type: remap                  # event time rather than arrival time
    inputs: [rdstail]
    source: |
      .timestamp = parse_timestamp!(.timestamp, "%+")

sinks:
  loki:
    type: loki
    inputs: [rds_ts]
    endpoint: http://loki.example.com:3100
    encoding:
      codec: text            # ship the raw line as the log body
    labels:
      job: rdstail
      instance: "{{ instance_id }}"
      engine: "{{ engine }}"
      severity: "{{ severity }}"
    remove_label_fields: false
```

Keep labels low-cardinality: `instance_id`, `engine`, `severity` are safe;
never label on `message`, `log_file`, or `batch_id` (unbounded values =
unbounded streams).

## Run

```bash
rdstail run -c rdstail.yaml | vector --config vector.yaml
```

## What arrives (LogQL)

```logql
{job="rdstail", instance="prod-pg-writer", severity=~"ERROR|FATAL"}
    |= "deadlock"
```

For Grafana Cloud, set `endpoint` to your zone's logs URL and add
`auth: {strategy: basic, user: "<tenant-id>", password: ${GRAFANA_CLOUD_API_KEY}}`
to the sink.
