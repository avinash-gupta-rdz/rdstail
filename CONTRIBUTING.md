# Contributing to rdstail

Thanks for considering a contribution. rdstail is a deliberately narrow tool
— the bar for new code is "does this strengthen *one binary, one YAML file,
never drop a log line*." Correctness beats features.

## Before you start

1. **Open an issue first** for anything non-trivial — a bug report, or a
   short proposal for a feature. It saves you writing code that conflicts
   with the [non-goals](README.md#non-goals) or the
   [roadmap](ROADMAP.md).
2. Small fixes (typos, docs, obvious bugs) can go straight to a PR.

## Development

You need Go 1.22+. No CGO, no external services for the standard suite.

```bash
make build        # produces bin/rdstail
make test         # unit + integration (fakes + localhost) — must pass
make vet          # go vet — must pass
make lint         # golangci-lint (install separately)
make cover        # HTML coverage report → coverage.html
make e2e          # anything tagged //go:build e2e
```

## Pull request checklist

- [ ] `make test vet` passes locally (CI runs the suite race-enabled).
- [ ] New behaviour has tests. Bug fixes have a regression test that fails
      without the fix.
- [ ] Changes are tightly scoped — one concern per PR.
- [ ] User-visible changes have a line in `CHANGELOG.md` under
      `[Unreleased]`, and README/examples updated if config or CLI changed.
- [ ] Commits are signed off (`git commit -s`) — this asserts agreement with
      the [Developer Certificate of Origin](https://developercertificate.org/).

## Architecture orientation

Start with [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — it documents the
invariants (checkpoint-after-ACK, rotation detection, DLQ semantics) that
every change must preserve. The project tree is mapped in the
[README](README.md#development).

Adding a sink has a documented recipe: see
[README → Adding a sink](README.md#adding-a-sink).

## Security issues

Do **not** open a public issue — see [SECURITY.md](SECURITY.md).
