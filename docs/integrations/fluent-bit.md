# RDS logs → fluent-bit → anywhere

fluent-bit's `stdin` input consumes rdstail's `stdout` sink directly (JSON
maps are ingested as structured records), and its
[outputs](https://docs.fluentbit.io/manual/pipeline/outputs) cover
Elasticsearch, Splunk, Loki, Stackdriver, Kafka, S3, and more. Same idea as
[vector.md](vector.md) — pick whichever agent you already operate.

## rdstail side

Use the exact `rdstail.yaml` from [vector.md](vector.md) (stdout sink,
ndjson).

## fluent-bit side

```ini
# fluent-bit.conf
[SERVICE]
    Flush        5

[INPUT]
    Name         stdin
    Tag          rds

# Example output — swap for es / splunk / loki / kafka / stackdriver ...
[OUTPUT]
    Name         loki
    Match        rds
    Host         loki.example.com
    Port         3100
    Labels       job=rdstail, $instance_id, $severity
    Line_format  key_value
```

## Run

```bash
rdstail run -c rdstail.yaml | fluent-bit -c fluent-bit.conf
```

Record fields (`instance_id`, `engine`, `log_file`, `timestamp`,
`severity`, `message`, `marker`, `batch_id`) arrive as the fluent-bit
record map —
use `modify` / `nest` filters to reshape before the output, and a `grep`
filter (e.g. `Regex severity ERROR|FATAL`) to ship only what matters.
