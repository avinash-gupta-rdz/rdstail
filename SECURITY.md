# Security Policy

## Supported versions

Only the latest release (and `main`) receives security fixes.

## Reporting a vulnerability

If you believe you've found a security vulnerability in rdstail, please
email **mine2technology@gmail.com** instead of opening a public issue.
Include:

- A description of the issue and its impact.
- Steps to reproduce (a config snippet and command line are ideal).
- Any suggested fix, if you have one.

You'll get an acknowledgement within 72 hours. The disclosure timeline is
**90 days** from report to public disclosure, shorter if a fix ships sooner.
Credit is given in the changelog unless you prefer otherwise.

## Scope notes

- rdstail never logs the contents of AWS credentials, and never logs RDS log
  line contents at levels above `debug`. Anything violating that is a
  security bug — report it.
- RDS log lines may contain PII depending on your engine settings
  (`log_statement`, slow-query logs, audit plugins). Treat sinks and the
  `--log-level debug` output accordingly; data-handling downstream of a sink
  is out of scope.
- Dependency vulnerabilities: report them if rdstail's usage is actually
  affected; otherwise Dependabot/renovate PRs are welcome.
