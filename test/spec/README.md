# Canonical implementation-spec acceptance suite

This directory is the authoritative mapping for §9 of
`mysql-boss-implementation-spec.md`. Each stable acceptance-criterion number
appears exactly once:

- `ac01-08.core.acceptance.test.ts`: AC 1–8
- `ac09-19.retry-dlq.acceptance.test.ts`: AC 9–19
- `ac20-28.cron-priority.acceptance.test.ts`: AC 20–28
- `ac29-38.lifecycle-archive.acceptance.test.ts`: AC 29–38

The suite runs against a real MySQL 8.0 server. When `MYSQL_HOST` is not set,
Vitest starts an ephemeral `mysql:8.0` container through Testcontainers. CI may
provide its own MySQL service through the standard `MYSQL_*` environment
variables.

Additional non-AC coverage lives in focused suites:

- `test/business-logic/`: deterministic unit tests for defensive and calendar
  branches.
- `test/regressions*.test.ts`: public API and previously reported bug
  regressions.
- `test/table-prefix.test.ts`: prefixed-schema integration coverage.
