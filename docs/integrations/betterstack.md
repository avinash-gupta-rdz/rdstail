# RDS logs → Better Stack (Logtail)

Better Stack's [HTTP ingest](https://betterstack.com/docs/logs/http-rest-api/)
accepts a JSON array with a bearer source token — a direct fit for the
`http` sink.

## Vendor side (1 step)

Create a source of type **HTTP** (**Logs → Sources → Connect source**) and
copy its token — and note the **ingesting host** shown on the source page:
newer sources get a per-source host (`sXXXX.…betterstackdata.com`) instead
of the shared `in.logs.betterstack.com`.

## rdstail side

```yaml
sources:
  - type: rds
    engine: mysql
    region: eu-west-1
    instances: [prod-mysql]

sinks:
  - name: betterstack
    type: http
    http:
      # Use the ingesting host shown on YOUR source's page.
      url: https://in.logs.betterstack.com
      headers:
        Authorization: Bearer ${BETTERSTACK_SOURCE_TOKEN}

state:
  type: sqlite
  path: ./state.db

runtime:
  start_from: end
```

```bash
export BETTERSTACK_SOURCE_TOKEN=...
rdstail run -c betterstack.yaml
```

## What arrives

One row per log line with `message`, `severity`, `instance_id`, `engine`,
`log_file`, and `batch_id` as columns/JSON fields. Better Stack stamps `dt`
at arrival; the engine's server-side event time is preserved in the
`timestamp` field — sort or chart on that when the distinction matters
(e.g. after a `dlq replay`).
