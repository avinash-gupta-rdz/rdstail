# Product Roadmap

This is the **adoption and value** roadmap: how rdstail gets from "technically
excellent log shipper" to "the default way people move RDS logs." It is
deliberately not about internals — engine work (soak, batching, Aurora
semantics) lives in [ROADMAP.md](ROADMAP.md). The two run in parallel: that
one makes the product *trustworthy*, this one makes it *used*.

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
IAM, and config. Every "Now" item below attacks a segment of that timeline.
Secondary signals (no telemetry, ever — proxies only): release-asset
downloads, Docker pulls, Homebrew installs, stars, inbound issues that are
feature requests rather than "how do I even start."

---

## Now — remove every adoption blocker (with the v1.0 gate)

The product works; almost nobody can *get* it yet. `go install` and
build-from-source filter the audience down to Go developers, which is not the
audience.

### 1. Real distribution channels ⬅ single highest-leverage item

- Tag `v0.2.0` and let the existing GoReleaser workflow publish **binary
  archives** for linux/darwin × amd64/arm64.
- **Docker image pushed to ghcr.io** — the README currently says "no pushed
  image yet"; for the K8s/ECS crowd this is the product.
- **Homebrew tap** (`brew install rdstail`) and a `curl | sh` install script
  with checksum verification.
- deb/rpm via nfpm when the first request lands.

### 2. `rdstail init` — from zero to a working config interactively

Today the quickstart asks the user to hand-write YAML. Instead:
`rdstail init` uses ambient AWS credentials to list RDS instances in the
region, lets the user pick instances and a sink (S3 bucket picker / Kafka
brokers / webhook URL / stdout), and writes a validated `rdstail.yaml` —
then prints the exact next command (`rdstail run -c rdstail.yaml`). The
wizard is the 5-minute claim made real for people who don't read docs.

### 3. `rdstail iam-policy` — IAM as a product feature, not a docs section

IAM is where AWS onboarding goes to die. Given a config, emit the *minimal*
IAM policy JSON it needs — including the conditional add-ons (KMS,
assume-role, HeadBucket) the README currently asks users to assemble by hand.
`--terraform` flag emits it as an `aws_iam_policy_document`. Pairs with
error-message work: when an AWS call fails with AccessDenied, the error
should name the missing action and say "run `rdstail iam-policy` to see the
full policy."

### 4. `rdstail cost-estimate` — the sales pitch as a command

The README's cost table is hypothetical. Make it personal: read the fleet's
actual log volume via `DescribeDBLogFiles` (sizes are free to read), project
a month, and print *your* CloudWatch-export cost next to *your* rdstail cost.
This is the screenshot people paste into Slack to justify adopting the tool —
it converts the strongest argument (money) from a claim into evidence.

### 5. Integration recipes for the tools people already use

The `http` and `stdout` sinks already reach almost everything; nobody knows
that. Ship copy-paste recipes (docs + `examples/`) for: **Datadog, Splunk
HEC, Grafana Loki, Elastic, Axiom, Better Stack** via the HTTP sink, and
**vector / fluent-bit** via stdout. Each recipe: full YAML, the vendor-side
setup in two lines, and what the record looks like on arrival. Zero code —
this is pure discoverability, and each recipe is a new search-engine landing
page ("rds logs to datadog without cloudwatch").

---

## Next — new use cases on the same engine (v1.0 → v1.2)

### 6. Incident-time toolkit: rdstail for humans, not just pipelines

`rdstail tail` proved there's a second product inside: an *interactive* RDS
log tool needing no config, no state, no sinks. Extend it:

- `rdstail tail --since 1h --grep 'deadlock|timeout'` — start from a point in
  time, filter server-side of your eyeballs. Multiple `-i` flags to tail a
  primary and its replica together.
- `rdstail dump -i my-db --since 24h -o incident-4231.ndjson.gz` — one-shot
  fetch-and-exit for postmortems, support tickets, and "attach the DB logs to
  the Jira." No pipeline, no S3, no IAM beyond read.

This is the wedge use case: the on-call engineer who used `tail` during an
incident is the same person who proposes the shipper to their team on Monday.

### 7. Compliance archive pack (promotes ROADMAP.md item 1 to a product)

Not "audit-log support" as a feature flag — a documented end-to-end recipe an
assessor can read: audit-plugin file opt-in (`include_audit: true`), pgAudit
`AUDIT:` field extraction into structured record fields, an **S3 Object Lock
+ lifecycle topology doc**, and a one-page "evidence guide" mapping rdstail's
guarantees (at-least-once, BatchID dedupe, DLQ-never-drops) to the control
language of SOC 2 / PCI-DSS. Title the doc what the buyer searches for:
*"RDS audit logs to immutable S3 — without the Lambda pipeline."*

### 8. `rdstail athena-ddl` — make the S3 archive instantly queryable

Shipping logs to S3 is half the value; querying them is the other half.
Generate the Glue/Athena `CREATE EXTERNAL TABLE` for the sink's exact
prefix/layout, plus the `row_number() over (partition by batch_id)` dedupe
view that upgrades at-least-once to exactly-once-at-query-time (ROADMAP.md
item 5, productised as a command instead of a docs page). The demo becomes:
ship logs, paste one DDL, run SQL over your Postgres errors — five minutes,
end to end.

### 9. OTLP sink (ROADMAP.md item 2 — adoption framing)

`type: otlp` makes rdstail a first-class citizen of every OTel-standardised
stack and collapses five of the recipe integrations into one config line.
The mapping is already clean (severity → SeverityText, timestamp →
TimeUnixNano, message → Body). This is the single sink with the widest
audience-per-effort ratio.

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
  query-analysis product. This softens the "no advanced parsing" non-goal
  the same way severity extraction already did — because it's where the log
  data's dollar value is highest.
- **Deploy modules** — Terraform module and Helm chart (currently deferred in
  ROADMAP.md; promote the moment two users ask — for platform teams the
  module *is* the adoption path), ECS/Fargate task definition recipe.
- **Aurora as a headline** (ROADMAP.md item 4) — once cluster discovery and
  the Aurora soak land, "works where naming matches" becomes "Aurora
  supported," which is most of the modern RDS market.
- **Launch & community motion** — this is roadmap work, not marketing
  garnish: a v1.0 write-up of the cost math + architecture (HN /
  r/devops / r/aws), PRs to awesome-aws / awesome-postgres lists, GitHub
  issue templates + Discussions, a `docs/comparison.md` (vs CloudWatch
  export, vs Firehose, vs DIY Lambda) that answers the evaluation question
  before it's asked, and a "used by" section seeded by the first production
  soak partners.

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

## How we'll know it's working

| Theme | Signal |
|---|---|
| Distribution | Release downloads + Docker pulls trending up release-over-release |
| Onboarding | "How do I start" issues → near zero; time-to-first-log ≤ 5 min in a cold-start screen recording |
| Incident toolkit | `tail`/`dump` mentioned in issues/posts independently of the shipper |
| Compliance pack | Inbound from audit-driven users citing the Object Lock doc |
| Integrations | ≥ 3 recipes with organic traffic; OTLP sink usage in bug reports |
| Advocacy | The cost-estimate screenshot appearing in places we didn't post it |
