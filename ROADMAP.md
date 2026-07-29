# Roadmap

One roadmap, two kinds of work: **product** (adoption, usability, new use
cases — what makes rdstail *used*) and **engine** (correctness and coverage —
what makes it *trustworthy*). Items are evidence-backed candidates, not
promises; priorities shift with real-world feedback. See
[CHANGELOG.md](CHANGELOG.md) for what already shipped, and
[Non-goals, revisited](#non-goals-revisited) for the boundaries.

## Positioning

**rdstail is the fastest, cheapest path from an RDS log line to wherever you
actually want it.** One binary, one YAML file, 85–95% cheaper than CloudWatch
export. Every roadmap item below must strengthen that sentence — anything
that dilutes "one binary, one YAML file" doesn't ship.

## Who we're building for

| Persona | Today's pain | What "value" means to them |
|---|---|---|
| **The solo DevOps / platform engineer** (primary) | CloudWatch bill line-item they can't defend; wants logs in S3/their stack, not a pipeline project | Working in 5 minutes, forgettable in production |
| **The on-call engineer at 2 a.m.** | Getting at RDS logs *right now* means console click-ops or CloudWatch Insights | One command that tails/greps the DB's logs from their laptop |
| **The compliance owner** | SOC 2 / PCI / HIPAA needs audit logs in immutable storage; AWS's blessed path is a 4-part Lambda pipeline | A documented, auditable topology they can hand to an assessor |
| **The observability team** | Standardising on OTLP / Datadog / Loki / Splunk; RDS logs are the awkward corner | RDS logs arrive shaped like everything else, no glue code |

## North-star metric

**Time-to-first-log-line: under 5 minutes** from "found the repo" to a log
record landing in the user's sink — measured honestly, including install,
IAM, and config. Secondary signals (no telemetry, ever — proxies only):
release-asset downloads, Docker pulls, Homebrew installs, stars, inbound
issues that are feature requests rather than "how do I even start."

---

## Gate to v1.0.0

- [ ] **Multi-day production soak** on a real fleet: no checkpoint drift, no
      memory growth, DLQ behaviour under real sink outages. The code paths are
      all unit/chaos/race-tested; v1.0 is an operational claim, not a feature.
- [ ] Tag `v0.2.0` first so soak feedback lands against a fixed reference
      (the release workflow builds archives on tag push).

## Now — remove every adoption blocker (with the v1.0 gate)

The product works; almost nobody can *get* it yet. `go install` and
build-from-source filter the audience down to Go developers, which is not the
audience.

### 1. Real distribution channels ⬅ single highest-leverage item

Tooling landed in-repo; everything below publishes automatically on the next
tag push:

- ~~**Docker image pushed to ghcr.io**~~ — ✅ goreleaser builds multi-arch
  (amd64/arm64) distroless images and manifests
  (`ghcr.io/avinash-gupta-rdz/rdstail:{version,latest}`).
- ~~**Homebrew tap** and a `curl | sh` install script with checksum
  verification~~ — ✅ formula publishes to `avinash-gupta-rdz/homebrew-tap`;
  `install.sh` verifies SHA-256 against the release's `checksums.txt`.
- **Remaining (operator actions):** push the `v0.2.0` tag; create the
  `homebrew-tap` repo and set the `HOMEBREW_TAP_GITHUB_TOKEN` secret (until
  then the brew upload is skipped and releases stay green).
- deb/rpm via nfpm when the first request lands.

### 2. ~~`rdstail init` — from zero to a working config interactively~~ — ✅ shipped

`rdstail init` uses ambient AWS credentials to list RDS instances in the
region, lets the user pick instances and a sink (S3 bucket picker / Kafka
brokers / webhook URL / stdout), and writes a commented, validated
`rdstail.yaml` — then prints the exact next commands (`iam-policy`,
`validate --deep`, `run`). Degrades to manual entry without credentials.
The wizard is the 5-minute claim made real for people who don't read docs.

### 3. ~~`rdstail iam-policy`~~ — ✅ shipped

IAM is where AWS onboarding goes to die. Given a config, emit the *minimal*
IAM policy JSON it needs — including the conditional add-ons (KMS,
assume-role, deep-validate probes) assembled by hand before. Landed as
`rdstail iam-policy -c config.yaml [--deep] [--terraform]`.

### 4. ~~`rdstail cost-estimate`~~ — ✅ shipped

The README's cost table is hypothetical; this makes it personal: read the
fleet's actual log volume via `DescribeDBLogFiles` (sizes are free to read),
project a month, and print *your* CloudWatch-export cost next to *your*
rdstail cost — the screenshot people paste into Slack to justify adopting the
tool. Landed as `rdstail cost-estimate [--region R [-i ID]... | -c config]
[--json]`, no config file required.

### 5. ~~Integration recipes for the tools people already use~~ — ✅ shipped

The `http` and `stdout` sinks already reach almost everything; nobody knew
that. Landed in [`docs/integrations/`](docs/integrations/README.md):
**Datadog, Axiom, Better Stack** direct via the HTTP sink; **Splunk HEC,
Grafana Loki, Elasticsearch/OpenSearch** via stdout → **vector /
fluent-bit**. Each recipe: full YAML, the vendor-side setup in two lines,
and what the record looks like on arrival — each one a search-engine landing
page ("rds logs to datadog without cloudwatch"). Shipped alongside `${VAR}`
config expansion so the recipes' API keys come from the environment.

---

## Next — new use cases on the same engine (v1.0 → v1.2)

### 6. ~~Incident-time toolkit: rdstail for humans, not just pipelines~~ — ✅ shipped

`rdstail tail` proved there's a second product inside: an *interactive* RDS
log tool needing no config, no state, no sinks. Landed:

- `rdstail tail --since 1h --grep 'deadlock|timeout'` — replay a window
  before following (files rotated out before the window are never
  downloaded), filter server-side of your eyeballs. Multiple `-i` flags tail
  a primary and its replica together.
- `rdstail dump -i my-db --since 24h -o incident-4231.ndjson.gz` — one-shot
  fetch-and-exit for postmortems, support tickets, and "attach the DB logs to
  the Jira." No pipeline, no S3, no IAM beyond read.

This is the wedge use case: the on-call engineer who used `tail` during an
incident is the same person who proposes the shipper to their team on Monday.

### 7. ~~Compliance archive pack (audit-log support, productised)~~ — ✅ shipped

Not "audit-log support" as a feature flag — a documented end-to-end recipe an
assessor can read (AWS's own recommended pipeline is CloudWatch →
subscription filter → Lambda → CSV → S3 — four moving parts and CloudWatch
ingestion fees for what rdstail does in one binary). Landed:

- MySQL/MariaDB audit-plugin files (`audit/server_audit.log*`) ingest behind
  a per-source `include_audit: true` opt-in (audit logs can be high-volume),
  with the event timestamp parsed from the plugin's CSV.
- pgAudit `AUDIT:` CSV classification fields (type, class, command,
  object type/name) are lifted into a structured `audit` record field the
  way severity is extracted — the statement stays verbatim in `message`.
- [`docs/compliance/audit-logs-immutable-s3.md`](docs/compliance/audit-logs-immutable-s3.md)
  — *"RDS audit logs to immutable S3 — without the Lambda pipeline"*: engine
  setup, Object Lock + lifecycle topology, and the evidence guide mapping
  at-least-once / BatchID dedupe / DLQ-never-drops to SOC 2 / PCI-DSS
  control language. Config: `examples/audit-archive.yaml`.

### 8. `rdstail athena-ddl` — make the S3 archive instantly queryable

Shipping logs to S3 is half the value; querying them is the other half.
Generate the Glue/Athena `CREATE EXTERNAL TABLE` for the sink's exact
prefix/layout, plus the `row_number() over (partition by batch_id)` dedupe
view that upgrades at-least-once to exactly-once-at-query-time. The demo
becomes: ship logs, paste one DDL, run SQL over your Postgres errors — five
minutes, end to end.

### 9. OTLP sink

OTLP is becoming the lingua franca for log transport; collectors and vendors
increasingly prefer receiving it natively (e.g.
[vector#24515](https://github.com/vectordotdev/vector/issues/24515),
[Fluent Bit's OTLP output](https://docs.fluentbit.io/manual/data-pipeline/outputs/opentelemetry)).
A `type: otlp` sink speaking OTLP/HTTP maps cleanly (severity →
SeverityText, timestamp → TimeUnixNano, message → Body, the rest →
attributes), makes rdstail a first-class citizen of every OTel-standardised
stack, and collapses several of the recipe integrations into one config
line. The single sink with the widest audience-per-effort ratio. Until then
the shipped pattern is `stdout` → any collector — document it prominently.

### 10. Day-2 answers in one command

- `rdstail status -c rdstail.yaml` — the operator's glance without Grafana:
  per-instance lag, last successful poll, current adaptive interval, DLQ
  depth, sink health. Reads the state store + `/metrics`; plain text and
  `--json`.
- `rdstail doctor -c rdstail.yaml` — `validate --deep` plus interpretation:
  each failing probe comes with its most likely cause and fix ("bucket policy
  denies PutObject from this role — add …"). Doctor is the difference between
  an issue filed and a problem self-served.

---

## Later — widen the funnel (post-v1.2, demand-gated)

- **Slow-query lens** — opt-in structured extraction for slow-query logs
  (query time, rows examined, normalised fingerprint) and a
  `rdstail slowlog top` digest — the pt-query-digest workflow with no agent
  on the DB host. Deliberately bounded: extraction of *log metadata*, not a
  query-analysis product.
- **Deploy modules** — Terraform module and Helm chart (promote the moment
  two users ask — for platform teams the module *is* the adoption path),
  ECS/Fargate task definition recipe. Until then the K8s story (single
  replica + PVC + IRSA) fits in ten README lines.
- **Aurora as a headline** — Aurora clusters expose logs per *instance* with
  slightly different naming and `DescribeDBClusters` semantics. Discovery
  already normalises Aurora engines; a dedicated soak against Aurora (incl.
  Serverless v2) plus cluster-level discovery (`discover.cluster_tags`)
  turns "works where naming matches" into "Aurora supported" — most of the
  modern RDS market.
- **Launch & community motion** — this is roadmap work, not marketing
  garnish: a v1.0 write-up of the cost math + architecture (HN /
  r/devops / r/aws), PRs to awesome-aws / awesome-postgres lists, GitHub
  issue templates + Discussions (✅ shipped), a `docs/comparison.md` (vs
  CloudWatch export, vs Firehose, vs DIY Lambda) that answers the evaluation
  question before it's asked, and a "used by" section seeded by the first
  production soak partners.

---

## Engine candidates (correctness & coverage)

Ordered by (pain × audience) / effort.

### Cross-poll batch buffering

Batching currently coalesces within a poll cycle; trickle-writing files still
produce one small S3 object per poll. Buffering across polls (checkpoint
advances only on flush; crash re-pulls the buffered range — at-least-once
preserved) would cut small objects further at the cost of bounded delivery
latency (`max_age` becomes real). Medium complexity; needs the soak data
first to see how much it matters in practice.

### Exactly-once S3 layout helper

Downstream dedupe already works via `BatchID`; item 8 (`athena-ddl`) is the
productised form. An S3-object naming contract that makes duplicate objects
idempotent overwrites would let S3-only users claim exactly-once without
even the dedupe view.

### Windows builds

No demand signal yet; goreleaser makes it a two-line change when one appears.

---

## Non-goals, revisited

The discipline in [README non-goals](README.md#non-goals) is a feature —
users trust narrow tools. This roadmap keeps **no SaaS, no UI, no alerting
engine, no implicit ingestion** untouched. Two boundaries flex, deliberately:

- *"No advanced parsing"* already flexed once (severity/timestamps). Audit
  fields and slow-query metadata follow the same rule: extract what the
  engine already structured, never interpret the message body.
- *Distribution, wizards, and generators* (`init`, `iam-policy`,
  `athena-ddl`, recipes) are not scope creep — they're the product doing its
  own onboarding. A tool whose pitch is "simplicity" cannot outsource
  simplicity to its README.

**Permanent non-goal:** metric filters / alerting / dashboards on log
content — rdstail feeds the tools that do this well.

## How we'll know it's working

| Theme | Signal |
|---|---|
| Distribution | Release downloads + Docker pulls trending up release-over-release |
| Onboarding | "How do I start" issues → near zero; time-to-first-log ≤ 5 min in a cold-start screen recording |
| Incident toolkit | `tail`/`dump` mentioned in issues/posts independently of the shipper |
| Compliance pack | Inbound from audit-driven users citing the Object Lock doc |
| Integrations | ≥ 3 recipes with organic traffic; OTLP sink usage in bug reports |
| Advocacy | The cost-estimate screenshot appearing in places we didn't post it |
