# RDS logs → vector → anywhere

The `stdout` sink writes one JSON record per line to stdout (rdstail's own
logs stay on stderr), so vector's `stdin` source ingests it with zero glue.
From there, any of vector's [sinks](https://vector.dev/docs/reference/configuration/sinks/)
apply — this is the route for backends that need a protocol rather than
plain JSON (Splunk HEC, Loki, Elasticsearch, Kafka with custom framing,
Chronicle, …).

## rdstail side

```yaml
# rdstail.yaml
sources:
  - type: rds
    engine: postgres
    region: us-east-1
    instances: [prod-pg-writer]

sinks:
  - name: pipe
    type: stdout
    stdout:
      format: ndjson

state:
  type: sqlite
  path: ./state.db

runtime:
  start_from: end

metrics:
  enabled: false   # keep the process pipe-friendly
```

## vector side

```yaml
# vector.yaml
sources:
  rdstail:
    type: stdin
    decoding:
      codec: json

transforms:
  # JSON decoding leaves `timestamp` a string; promote it to the event's
  # real time so downstream sinks use server-side event time, not arrival.
  rds_ts:
    type: remap
    inputs: [rdstail]
    source: |
      .timestamp = parse_timestamp!(.timestamp, "%+")

sinks:
  # pick a real sink: splunk_hec_logs, loki, elasticsearch, ...
  console:
    type: console
    inputs: [rds_ts]
    encoding:
      codec: json
```

## Run

```bash
rdstail run -c rdstail.yaml | vector --config vector.yaml
```

Every record arrives in vector as a structured event with `instance_id`,
`engine`, `log_file`, `timestamp`, `severity`, `message`, `marker`, and
`batch_id` fields — use `remap` transforms to reshape, route, or drop
before shipping.

Under systemd, run the pipe as one unit:

```ini
ExecStart=/bin/sh -c '/usr/local/bin/rdstail run -c /etc/rdstail.yaml | /usr/bin/vector --config /etc/vector/vector.yaml'
```

Delivery note: rdstail checkpoints when the *stdout write* succeeds, so
end-to-end durability past the pipe is vector's job — enable vector's disk
buffers on the final sink if the backend being down must not drop lines.

Concrete backend recipes on this route: [splunk.md](splunk.md),
[loki.md](loki.md), [elastic.md](elastic.md).
