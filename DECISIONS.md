# DECISIONS.md

Spec ambiguities and implementation decisions. Each entry states what was ambiguous, options considered, what was chosen and why, and which AC it affects.

## Resolved

### Archive payload inclusion (§7 #1)

Per implementation prompt: default to full payload in the archive. The slim-log variant is not built.

**Affects:** AC 35–38

## Deferred ideas

(Items explicitly rejected or deferred by the spec — logged here as a fence against scope creep.)

- Table-per-queue (§7 #4)
- Adaptive polling (§7 #2)
- Priority aging (§7 #5)
- Seconds-field or L/W/# cron extensions (§7 #6)
- Multi-queue single-statement claims (§7 #8)
- LISTEN/NOTIFY emulation
- CLI
- ORM layer
- Dashboard
