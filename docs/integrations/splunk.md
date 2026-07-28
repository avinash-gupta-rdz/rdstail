# RDS logs → Splunk (HEC)

Splunk's HTTP Event Collector wants events wrapped in its own envelope
(`{"event": ..., "sourcetype": ...}`), not a bare JSON array — so this
recipe pipes rdstail's `stdout` sink through vector, whose
`splunk_hec_logs` sink speaks the envelope natively. Honest trade: one
extra process, zero custom code.

## Vendor side (2 steps)

1. Enable HEC and create a token: **Settings → Data Inputs → HTTP Event
   Collector → New Token** (pick or create an index, e.g. `rds`).
2. Note the HEC endpoint, usually `https://<splunk-host>:8088`.

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
  rds_ts:                        # string → real timestamp, so _time is
    type: remap                  # event time rather than arrival time
    inputs: [rdstail]
    source: |
      .timestamp = parse_timestamp!(.timestamp, "%+")

sinks:
  splunk:
    type: splunk_hec_logs
    inputs: [rds_ts]
    endpoint: https://splunk.example.com:8088
    default_token: ${SPLUNK_HEC_TOKEN}
    index: rds
    sourcetype: rdstail:json
    host_key: instance_id        # Splunk "host" = the RDS instance
    timestamp_key: timestamp     # server-side event time, not arrival time
    encoding:
      codec: json
    buffer:
      type: disk                 # survive Splunk outages
      max_size: 268435488
```

## Run

```bash
export SPLUNK_HEC_TOKEN=...
rdstail run -c rdstail.yaml | vector --config vector.yaml
```

## What arrives

```
index=rds sourcetype=rdstail:json severity=ERROR
  host = prod-pg-writer
  _time = the engine's log timestamp
  message = ERROR:  deadlock detected
```

Dedupe at-least-once deliveries in SPL when needed:
`... | dedup batch_id message`.
