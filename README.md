# mysql-boss

A [pg-boss](https://github.com/timgit/pg-boss)-inspired job queue built for **MySQL 8.0.1+**. Zero runtime dependencies beyond [`mysql2`](https://github.com/sidorares/node-mysql2).

Correctness first, throughput second. Uses `FOR UPDATE SKIP LOCKED` for non-blocking, contention-safe job claims with lease-based crash recovery.

## Features

- **At-least-once delivery** with lease-based crash recovery
- **Exactly-one-holder guarantee** — only one worker holds a job at any time
- **Retries** with fixed or exponential backoff
- **Dead-letter queue** for permanently failed jobs, with replay
- **Cron scheduling** — 5-field Vixie cron with timezone support (DST-aware)
- **Priority queues** — higher-priority jobs dequeue first
- **Archive** — completed jobs move to a separate table for audit/history
- **Singleton jobs** — deduplicate by key within a queue
- **Graceful shutdown** with configurable drain timeout

## Requirements

- **Node.js** >= 20
- **MySQL** >= 8.0.1 (uses `FOR UPDATE SKIP LOCKED`, descending indexes, `DEFAULT (expression)`)

## Install

```bash
npm install mysql-boss mysql2
```

`mysql2` is a peer dependency — you provide the connection pool.

## Quick start

```typescript
import mysql from "mysql2/promise";
import { MysqlBoss } from "mysql-boss";

// 1. Create a mysql2 pool (you own it — mysql-boss never closes it)
const pool = mysql.createPool({
  host: "localhost",
  user: "root",
  password: "secret",
  database: "myapp",
  waitForConnections: true,
  connectionLimit: 10,
});

// 2. Create the boss
const boss = new MysqlBoss({ pool });

// 3. Run migrations (idempotent — safe to call on every startup)
await boss.migrate();

// 4. Register a worker
boss.work("send-email", async (job, { signal }) => {
  const { to, subject, body } = job.payload;
  await sendEmail(to, subject, body);
  // Returning without throwing = success → job archived
});

// 5. Enqueue a job
await boss.enqueue("send-email", {
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up.",
});

// 6. Shut down gracefully on SIGTERM
process.on("SIGTERM", async () => {
  await boss.stop();
  await pool.end();
});
```

### JavaScript (CommonJS)

```javascript
const mysql = require("mysql2/promise");
const { MysqlBoss } = require("mysql-boss");

const pool = mysql.createPool({ /* ... */ });
const boss = new MysqlBoss({ pool });

await boss.migrate();

boss.work("process-order", async (job) => {
  console.log("Processing order:", job.payload.orderId);
});

await boss.enqueue("process-order", { orderId: "abc-123" });
```

---

## API reference

### `new MysqlBoss(options)`

Creates a new instance. Validates configuration synchronously — throws `ConfigError` on invalid options.

```typescript
import type { Pool } from "mysql2/promise";

interface MysqlBossOptions {
  pool: Pool;                    // Required. Your mysql2 promise pool.
  pollIntervalMs?: number;       // Default: 2000. How often to poll for jobs (ms). ±20% jitter applied.
  batchSize?: number;            // Default: 10. Jobs claimed per poll (1–100).
  concurrency?: number;          // Default: batchSize. Max in-flight jobs per worker.
  leaseSeconds?: number;         // Default: 300. How long a job is locked before recovery.
  heartbeatSeconds?: number;     // Default: 100. Lease renewal interval.
  sweepIntervalMs?: number;      // Default: 60000. Stale-job recovery check interval (ms).
  tickIntervalMs?: number;       // Default: 30000. Cron schedule check interval (ms).
  archiveRetentionDays?: number; // Default: 14. Days to keep archived (completed) jobs. Integer >= 1.
  drainTimeoutMs?: number;       // Default: 30000. Max time to wait for in-flight jobs during stop().
}
```

**Constraints:**
- `leaseSeconds` must be >= `3 * heartbeatSeconds` (throws `ConfigError`)
- `batchSize` must be between 1 and 100 (throws `ConfigError`)
- `archiveRetentionDays` must be a positive integer (throws `ConfigError`)

```typescript
// Example: low-latency config for small jobs
const boss = new MysqlBoss({
  pool,
  pollIntervalMs: 500,
  batchSize: 20,
  concurrency: 20,
  leaseSeconds: 60,
  heartbeatSeconds: 15,
});
```

---

### `boss.migrate()`

```typescript
await boss.migrate(): Promise<void>
```

Creates all required tables (`jobs`, `jobs_archive`, `jobs_dead`, `schedules`) if they don't exist. **Idempotent** — safe to call on every app startup.

---

### `boss.enqueue(queue, payload, options?)`

```typescript
await boss.enqueue<T>(queue: string, payload: T, opts?: EnqueueOptions): Promise<string | null>
```

Adds a job to the queue. Returns the job ID as a string, or `null` if deduplicated by `singletonKey`.

```typescript
interface EnqueueOptions {
  priority?: number;        // Default: 0. Range: -32768 to 32767. Higher runs first.
  singletonKey?: string;    // Dedupe key (max 191 chars). Must not start with "cron:".
  retryLimit?: number;      // Default: 2. Max retries after initial attempt (so 3 total runs max).
  retryDelaySecs?: number;  // Default: 30. Base delay between retries (seconds).
  retryBackoff?: boolean;   // Default: false. true = exponential backoff (delay * 2^n, capped at 24h).
  runAt?: Date;             // Default: now (DB clock). Schedule for future execution.
}
```

#### Examples

**Basic enqueue:**

```typescript
const jobId = await boss.enqueue("send-email", {
  to: "user@example.com",
  subject: "Hello",
});
console.log(`Enqueued job ${jobId}`);
```

**With priority (higher = more urgent):**

```typescript
await boss.enqueue("notifications", { userId: 42 }, { priority: 10 });
await boss.enqueue("notifications", { userId: 99 }, { priority: 0 });
// userId 42 will be processed first
```

**Delayed / scheduled job:**

```typescript
const inOneHour = new Date(Date.now() + 60 * 60 * 1000);
await boss.enqueue("send-reminder", { userId: 42 }, { runAt: inOneHour });
```

**Singleton (deduplicate):**

```typescript
// Only one "daily-report" job per queue at a time
const id = await boss.enqueue("reports", { type: "daily" }, {
  singletonKey: "daily-report",
});
// Second call returns null — job already exists
const duplicate = await boss.enqueue("reports", { type: "daily" }, {
  singletonKey: "daily-report",
});
console.log(duplicate); // null
```

**With retries and exponential backoff:**

```typescript
await boss.enqueue("call-api", { url: "https://api.example.com" }, {
  retryLimit: 5,           // Retry up to 5 times after the first attempt (6 total)
  retryDelaySecs: 10,      // Start at 10s
  retryBackoff: true,      // 10s, 20s, 40s, 80s, 160s (capped at 24h)
});
```

---

### `boss.work(queue, handler)`

```typescript
boss.work<T>(queue: string, handler: JobHandler<T>): void
```

Registers a handler for a queue and starts polling. **One handler per queue** — calling `work()` twice for the same queue throws.

```typescript
type JobHandler<T = unknown> = (
  job: ActiveJob<T>,
  ctx: { signal: AbortSignal },
) => Promise<void>;

interface ActiveJob<T = unknown> {
  id: string;          // Job ID (BIGINT rendered as string)
  queue: string;       // Queue name
  payload: T;          // The payload you enqueued
  retryCount: number;  // How many retries have been consumed so far
  retryLimit: number;  // Max retries allowed
}
```

**Handler outcomes:**

| Handler does | Result |
|---|---|
| Resolves (returns) | Job is **completed** and moved to `jobs_archive` |
| Rejects (throws) with retries remaining | Job returns to queue with backoff delay |
| Rejects (throws) with retries exhausted | Job moves to `jobs_dead` (dead-letter queue) |

#### Examples

**Simple handler:**

```typescript
boss.work("send-email", async (job) => {
  await sendEmail(job.payload.to, job.payload.subject);
});
```

**Typed payload:**

```typescript
interface EmailPayload {
  to: string;
  subject: string;
  body: string;
}

boss.work<EmailPayload>("send-email", async (job) => {
  // job.payload is typed as EmailPayload
  await sendEmail(job.payload.to, job.payload.subject, job.payload.body);
});
```

**Using the abort signal:**

The `signal` is aborted in two cases:
1. **Lease lost** — another worker reclaimed the job (the handler is a zombie)
2. **Shutdown** — `boss.stop()` was called and drain timeout expired

```typescript
boss.work("long-task", async (job, { signal }) => {
  for (const chunk of splitIntoChunks(job.payload.data)) {
    if (signal.aborted) {
      throw new Error("Aborted");
    }
    await processChunk(chunk);
  }
});
```

**With fetch (Node 20+):**

```typescript
boss.work("call-api", async (job, { signal }) => {
  const response = await fetch(job.payload.url, { signal });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const data = await response.json();
  await saveResult(job.id, data);
});
```

---

### `boss.schedule(name, queue, cron, options?)`

```typescript
await boss.schedule(name: string, queue: string, cron: string, opts?: ScheduleOptions): Promise<void>
```

Creates or updates a cron schedule. Jobs are enqueued automatically on each tick.

```typescript
interface ScheduleOptions {
  timezone?: string;        // Default: "UTC". Any IANA timezone (e.g. "America/New_York").
  payload?: unknown;        // Payload for each spawned job.
  priority?: number;        // Default: 0.
  retryLimit?: number;      // Default: 2.
  retryDelaySecs?: number;  // Default: 30.
  retryBackoff?: boolean;   // Default: false.
}
```

**Cron format** — standard 5-field Vixie cron:

```
 ┌───────── minute (0–59)
 │ ┌─────── hour (0–23)
 │ │ ┌───── day of month (1–31)
 │ │ │ ┌─── month (1–12 or Jan–Dec)
 │ │ │ │ ┌─ day of week (0–6 or Sun–Sat, 0 = Sunday)
 │ │ │ │ │
 * * * * *
```

Supports: values, ranges (`1-5`), steps (`*/15`), lists (`1,3,5`), and month/day-of-week names.

**DOM/DOW OR-semantics:** when both day-of-month and day-of-week are specified (not `*`), the job fires if **either** matches (Vixie cron behavior).

#### Examples

```typescript
// Every hour on the hour
await boss.schedule("hourly-cleanup", "cleanup", "0 * * * *");

// Every weekday at 9:00 AM Eastern
await boss.schedule("morning-report", "reports", "0 9 * * 1-5", {
  timezone: "America/New_York",
  payload: { type: "morning" },
});

// Every 15 minutes with high priority
await boss.schedule("health-check", "monitoring", "*/15 * * * *", {
  priority: 10,
  retryLimit: 0,  // No retries for health checks
});
```

**Catch-up policy:** if the system was down across multiple occurrences, only **one** catch-up job is created (not one per missed tick).

**Updating a schedule:** calling `schedule()` with the same `name` updates the existing schedule in place.

---

### `boss.unschedule(name)`

```typescript
await boss.unschedule(name: string): Promise<void>
```

Removes a cron schedule by name. Already-enqueued jobs from this schedule are not affected.

```typescript
await boss.unschedule("hourly-cleanup");
```

---

### `boss.listDead(query)`

```typescript
await boss.listDead(query): Promise<DeadJob[]>
```

Lists jobs in the dead-letter queue (jobs that exhausted all retries).

```typescript
interface DeadJob<T = unknown> {
  id: string;
  queue: string;
  payload: T;
  priority: number;
  retryCount: number;
  createdAt: Date;
  failedAt: Date;
  lastError: { message: string; stack?: string; at: string } | null;
}
```

```typescript
const deadJobs = await boss.listDead({
  queue: "send-email",
  after: new Date("2025-01-01"),  // Optional: filter by failed_at range
  before: new Date(),              // Optional
  limit: 20,                       // Default: 50
  offset: 0,                       // Default: 0
});

for (const job of deadJobs) {
  console.log(`Job ${job.id} failed: ${job.lastError?.message}`);
}
```

---

### `boss.replayDead(ids)`

```typescript
await boss.replayDead(ids: string[]): Promise<number>
```

Re-enqueues dead jobs as fresh jobs (new IDs, counters reset). Returns the count of replayed jobs.

Throws `SingletonCollisionError` if a replayed job collides with a live singleton — the dead row is preserved and nothing is replayed.

```typescript
// Replay specific failed jobs
const replayed = await boss.replayDead(["42", "43", "44"]);
console.log(`Replayed ${replayed} jobs`);
```

```typescript
// Replay all dead jobs for a queue
const deadJobs = await boss.listDead({ queue: "send-email" });
if (deadJobs.length > 0) {
  const replayed = await boss.replayDead(deadJobs.map((j) => j.id));
  console.log(`Replayed ${replayed} of ${deadJobs.length} dead jobs`);
}
```

---

### `boss.getArchivedJob(id)`

```typescript
await boss.getArchivedJob(id: string): Promise<ArchivedJob | null>
```

Looks up a completed job by its original ID. Returns `null` if not found (either never existed or already pruned by retention).

```typescript
interface ArchivedJob<T = unknown> {
  id: string;
  queue: string;
  payload: T;
  priority: number;
  retryCount: number;
  createdAt: Date;
  startedAt: Date;
  completedAt: Date;
  durationMs: number;  // Handler execution time in milliseconds
}
```

```typescript
const archived = await boss.getArchivedJob("12345");
if (archived) {
  console.log(`Job completed in ${archived.durationMs}ms`);
}
```

---

### `boss.listArchive(query)`

```typescript
await boss.listArchive(query): Promise<ArchivedJob[]>
```

Lists completed jobs using keyset pagination (pass the last row's `completedAt` as `before` for the next page).

```typescript
// First page
const page1 = await boss.listArchive({
  queue: "send-email",
  limit: 20,
});

// Next page (use last row's completedAt as cursor)
if (page1.length === 20) {
  const page2 = await boss.listArchive({
    queue: "send-email",
    before: page1[page1.length - 1].completedAt,
    limit: 20,
  });
}
```

---

### `boss.stop(options?)`

```typescript
await boss.stop(opts?: { drainTimeoutMs?: number }): Promise<void>
```

Gracefully shuts down all workers. **Idempotent** — safe to call multiple times. The instance is unusable after `stop()`.

**Shutdown sequence:**

1. Stops all polling loops and the cron ticker (no new claims)
2. Waits for in-flight handlers up to `drainTimeoutMs` (default: 30s)
3. If handlers are still running at timeout: aborts their `AbortSignal` and releases jobs back to the queue **without incrementing retry count** (a drain is not a failure)
4. Stops heartbeat and sweep timers

```typescript
// Wire up to process signals
process.on("SIGTERM", async () => {
  console.log("Shutting down...");
  await boss.stop({ drainTimeoutMs: 10_000 });
  await pool.end();
  console.log("Shutdown complete");
});
```

---

### Error classes

All predictable failures throw typed errors you can catch:

```typescript
import { ConfigError, ValidationError, SingletonCollisionError } from "mysql-boss";
```

| Error | When |
|---|---|
| `ConfigError` | Invalid constructor options (bad `batchSize`, `leaseSeconds < 3 * heartbeatSeconds`, etc.) |
| `ValidationError` | Invalid enqueue arguments (`priority` out of range, `singletonKey` too long or starts with `cron:`) |
| `SingletonCollisionError` | `replayDead()` collides with a live singleton job |

---

## Delivery guarantee

mysql-boss provides **at-least-once delivery**. This means:

- A job will be processed **at least once** — crash recovery ensures no job is lost
- A job **may be processed more than once** if a worker crashes after completing work but before the completion transaction commits
- **Your handlers must be idempotent** — safe to run multiple times with the same input

### Why "at least once"?

If a worker finishes processing a job but crashes before the archive-move transaction commits, the job's lease will eventually expire. The sweep recovers it, and another worker re-processes it. The work was done twice, but the job record stays consistent.

**Making handlers idempotent:**

```typescript
boss.work("charge-customer", async (job) => {
  // Use an idempotency key to prevent double-charging
  const idempotencyKey = `charge-${job.id}`;
  await paymentProvider.charge(job.payload.amount, { idempotencyKey });
});
```

---

## Architecture

### Tables

| Table | Purpose | Size profile |
|---|---|---|
| `jobs` | Hot table — only pending and in-flight jobs | Backlog-sized (small) |
| `jobs_archive` | Completed jobs — audit/history | Grows with throughput, pruned by retention |
| `jobs_dead` | Failed jobs (retries exhausted) | Small unless something is broken |
| `schedules` | Cron schedule definitions | Tiny |

Completed jobs are **moved** out of `jobs` into `jobs_archive` on success (and into `jobs_dead` on permanent failure). This keeps the hot `jobs` table small and the dequeue index fast — MySQL has no partial indexes, so terminal rows left in `jobs` would bloat every claim scan.

### How claims work

```
Worker A                          Worker B
   |                                 |
   |-- BEGIN                         |-- BEGIN
   |-- SELECT ... FOR UPDATE         |-- SELECT ... FOR UPDATE
   |   SKIP LOCKED                   |   SKIP LOCKED
   |   -> locks rows 1, 2, 3        |   -> skips 1, 2, 3
   |                                 |   -> locks rows 4, 5, 6
   |-- UPDATE SET state='active'     |-- UPDATE SET state='active'
   |-- COMMIT                        |-- COMMIT
```

Workers never block each other. `SKIP LOCKED` means if a row is already claimed, the next worker silently skips it and takes the next available job.

### Crash recovery

Every claimed job has a `lease_expires_at` timestamp. If a worker crashes:

1. Its database connections die — all InnoDB row locks are released
2. The `lease_expires_at` data column remains
3. A periodic **sweep** finds jobs where `lease_expires_at < NOW()` and `state = 'active'`
4. The sweep resets them to `available` (with `retry_count` incremented) or moves them to the DLQ

### Fencing

Every post-claim operation (`complete`, `fail`, `heartbeat`) includes `AND locked_by = UUID_TO_BIN(?)`. A zombie worker whose lease expired cannot affect a job now owned by someone else.

---

## Archive retention and partitioning

The `jobs_archive` table is pruned automatically based on `archiveRetentionDays` (default: 14 days). Pruning runs in batches of 5,000 rows to bound lock time.

### For high-throughput or long-retention setups

If you keep archives for 90+ days or process millions of jobs, consider partitioning the `jobs_archive` table by `completed_at` (monthly partitions). This turns retention cleanup from batched DELETEs into instant `ALTER TABLE ... DROP PARTITION`:

```sql
ALTER TABLE jobs_archive
PARTITION BY RANGE (TO_DAYS(completed_at)) (
  PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
  PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
  -- add partitions monthly
  PARTITION pmax VALUES LESS THAN MAXVALUE
);
```

mysql-boss's batched `DELETE` still works against a partitioned table — partitioning is a deployment choice, not a library change.

---

## Connection pool sizing

mysql-boss uses short-lived connections: one per claim transaction, released immediately after COMMIT. Handlers never hold connections. A reasonable starting point:

```
connectionLimit = concurrency + 2 (sweep + tick)
```

For a single-process worker with `concurrency: 10`, a pool of 12–15 connections is plenty.

All connections are configured to `READ COMMITTED` isolation and `UTC` timezone automatically.

---

## TypeScript and JavaScript support

mysql-boss ships dual **ESM** and **CommonJS** builds with full type definitions.

**ESM (TypeScript / modern Node.js):**

```typescript
import { MysqlBoss } from "mysql-boss";
import type { ActiveJob, EnqueueOptions } from "mysql-boss";
```

**CommonJS (JavaScript):**

```javascript
const { MysqlBoss } = require("mysql-boss");
```

Both resolve automatically via the `exports` field in `package.json`.

---

## License

[MIT](LICENSE)
