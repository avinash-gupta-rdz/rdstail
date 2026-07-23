# Roadmap

What's next, and why. Items are evidence-backed candidates, not promises —
priorities shift with real-world feedback. See [CHANGELOG.md](CHANGELOG.md)
for what already shipped, and the README's [Non-goals](README.md#non-goals)
for what will never be here.

## Gate to v1.0.0

- [ ] **Multi-day production soak** on a real fleet: no checkpoint drift, no
      memory growth, DLQ behaviour under real sink outages. The code paths are
      all unit/chaos/race-tested; v1.0 is an operational claim, not a feature.
- [ ] Tag `v0.2.0` first so soak feedback lands against a fixed reference
      (the release workflow builds archives on tag push).

## Post-v1 candidates

Ordered by (pain × audience) / effort, with the evidence that put them here.

### 1. Audit-log support (compliance audience)

**Pain:** pgAudit/MariaDB-audit users heading into SOC 2 / PCI-DSS / HIPAA
audits need audit records in an immutable store. AWS's own recommended
pipeline for this is CloudWatch → subscription filter → Lambda → CSV → S3
([AWS blog](https://aws.amazon.com/blogs/database/automate-postgresql-audit-log-extraction-and-analysis-with-amazon-s3/),
[sample repo](https://github.com/aws-samples/sample-rds-pg-audit-log-s3)) —
four moving parts and CloudWatch ingestion fees for what rdstail does in one
binary.

**Work:**
- MySQL/MariaDB audit-plugin files (`audit/server_audit.log*`) are currently
  **rejected** by the classifier — accept them behind a per-source opt-in
  (`include_audit: true`) since audit logs can be high-volume.
- pgAudit lines already flow (they live in `postgresql.log`); optionally
  extract the `AUDIT:` CSV fields (session/object, command, object name) into
  structured record fields the way severity is extracted today.
- Document an S3 Object Lock bucket topology for immutability.

### 2. OTLP log sink (observability-ecosystem audience)

**Pain:** OTLP is becoming the lingua franca for log transport; collectors and
vendors increasingly prefer receiving it natively (e.g.
[vector#24515](https://github.com/vectordotdev/vector/issues/24515),
[Fluent Bit's OTLP output](https://docs.fluentbit.io/manual/data-pipeline/outputs/opentelemetry)).

**Work:** a `type: otlp` sink speaking OTLP/HTTP (LogRecord maps cleanly:
severity → SeverityText, timestamp → TimeUnixNano, message → Body, the rest →
attributes). Until then the shipped pattern is `stdout` → any collector —
document it more prominently.

### 3. Cross-poll batch buffering

Batching currently coalesces within a poll cycle; trickle-writing files still
produce one small S3 object per poll. Buffering across polls (checkpoint
advances only on flush; crash re-pulls the buffered range — at-least-once
preserved) would cut small objects further at the cost of bounded delivery
latency (`max_age` becomes real). Medium complexity; needs the soak data
first to see how much it matters in practice.

### 4. Aurora-specific coverage

Aurora clusters expose logs per *instance* with slightly different naming and
`DescribeDBClusters` semantics. Discovery already normalises Aurora engines;
a dedicated soak against Aurora (incl. Serverless v2) plus cluster-level
discovery (`discover.cluster_tags`) would make the "Aurora works where naming
matches" caveat disappear.

### 5. Exactly-once S3 layout helper

Downstream dedupe already works via `BatchID`; a documented Athena/Glue table
DDL + a `SELECT DISTINCT ON (batch_id)` view (or an S3-object naming contract
that makes duplicate objects idempotent overwrites) would let S3-only users
claim exactly-once without running a dedupe job.

## Explicitly deferred

- **Windows builds** — no demand signal yet; goreleaser makes it a two-line
  change when one appears.
- **Helm chart** — the K8s story (single replica + PVC + IRSA) fits in ten
  README lines today; a chart earns its maintenance cost only with users.
- **Metric filters / alerting / dashboards on log content** — permanent
  non-goal; rdstail feeds the tools that do this well.
