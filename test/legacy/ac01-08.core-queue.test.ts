import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ConfigError, MysqlBoss, ValidationError } from "../../src/index.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
	const boss = new MysqlBoss({ pool });
	await boss.migrate();
}, 30_000);

afterAll(async () => {
	await pool.end();
}, 10_000);

beforeEach(async () => {
	await cleanTables(pool);
});

describe("Legacy coverage: migrate() idempotent", () => {
	it("running migrate() twice produces no error", async () => {
		const boss = new MysqlBoss({ pool });
		await boss.migrate();
		await boss.migrate();
	});

	it("tables exist after migration", async () => {
		const boss = new MysqlBoss({ pool });
		await boss.migrate();

		const [tables] = await pool.query<RowDataPacket[]>(
			"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('jobs', 'jobs_archive', 'jobs_dead', 'schedules') ORDER BY TABLE_NAME",
		);
		const names = tables.map((r) => r.TABLE_NAME);
		expect(names).toEqual(["jobs", "jobs_archive", "jobs_dead", "schedules"]);
	});
});

describe("Legacy coverage: runAt future unclaimable", () => {
	it("a job with runAt in the future is not claimed", async () => {
		const boss = await createBoss(pool);
		const futureDate = new Date(Date.now() + 60 * 60 * 1000);
		await boss.enqueue("test-q", { data: "future" }, { runAt: futureDate });

		// Try to claim — should get nothing
		boss.work("test-q", async () => {
			throw new Error("Should not be called");
		});

		// Wait briefly for a poll cycle
		await new Promise((r) => setTimeout(r, 500));

		// Verify job is still available (not claimed)
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT state FROM jobs WHERE queue = 'test-q'",
		);
		expect(rows).toHaveLength(1);
		expect(rows[0].state).toBe("available");

		await boss.stop();
	});
});

describe("Legacy coverage: exactly-once under contention", () => {
	it("4 workers x 1000 jobs, each handler executes exactly once", async () => {
		const boss1 = await createBoss(pool, {
			pollIntervalMs: 50,
			batchSize: 20,
			concurrency: 20,
		});

		// Create side-effect table with unique constraint
		await pool.query(`
			CREATE TABLE IF NOT EXISTS side_effects (
				job_id BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (job_id)
			) ENGINE=InnoDB
		`);
		await pool.query("DELETE FROM side_effects");

		const JOB_COUNT = 1000;

		// Enqueue all jobs
		for (let i = 0; i < JOB_COUNT; i++) {
			await boss1.enqueue("contention-q", { idx: i });
		}

		// Create 4 worker bosses
		const workers: MysqlBoss[] = [];
		const workerPools: Pool[] = [];
		const errors: Error[] = [];

		for (let w = 0; w < 4; w++) {
			const workerPool = await createPool();
			const worker = new MysqlBoss({
				pool: workerPool,
				pollIntervalMs: 50,
				batchSize: 20,
				concurrency: 20,
				leaseSeconds: 30,
				heartbeatSeconds: 10,
				sweepIntervalMs: 5000,
				drainTimeoutMs: 10_000,
			});
			await worker.migrate();

			worker.work("contention-q", async (job) => {
				await pool.query("INSERT INTO side_effects (job_id) VALUES (?)", [
					BigInt(job.id),
				]);
			});

			workers.push(worker);
			workerPools.push(workerPool);
		}

		// Wait for all jobs to be processed
		const deadline = Date.now() + 60_000;
		while (Date.now() < deadline) {
			const [rows] = await pool.query<RowDataPacket[]>(
				"SELECT COUNT(*) as cnt FROM side_effects",
			);
			if (rows[0].cnt >= JOB_COUNT) break;
			await new Promise((r) => setTimeout(r, 200));
		}

		// Stop all workers
		for (const w of workers) {
			await w.stop();
		}
		for (const workerPool of workerPools) {
			await workerPool.end();
		}
		await boss1.stop();

		// Assert exactly JOB_COUNT side effects
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM side_effects",
		);
		expect(rows[0].cnt).toBe(JOB_COUNT);

		// Verify no jobs remain in the hot table
		const [remaining] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs WHERE queue = 'contention-q'",
		);
		expect(remaining[0].cnt).toBe(0);

		await pool.query("DROP TABLE IF EXISTS side_effects");
	}, 90_000);
});

describe("Legacy coverage: claim returns at most batchSize jobs", () => {
	it("claims correct batch with correct state transitions", async () => {
		const boss = await createBoss(pool, { batchSize: 3 });

		// Enqueue 10 jobs
		for (let i = 0; i < 10; i++) {
			await boss.enqueue("batch-q", { idx: i });
		}

		// Manually claim using internal SQL to inspect
		const processed: string[] = [];
		const boss2 = await createBoss(pool, {
			batchSize: 3,
			concurrency: 3,
			pollIntervalMs: 100,
		});

		boss2.work("batch-q", async (job) => {
			processed.push(job.id);
			// Slow handler so we can inspect state
			await new Promise((r) => setTimeout(r, 500));
		});

		// Wait for first batch to start
		await new Promise((r) => setTimeout(r, 300));

		// Check active jobs
		const [active] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs WHERE queue = 'batch-q' AND state = 'active'",
		);
		// Should have at most batchSize (3) active
		expect(active[0].cnt).toBeLessThanOrEqual(3);

		// Verify active jobs have locked_by and lease_expires_at set
		const [activeRows] = await pool.query<RowDataPacket[]>(
			"SELECT locked_by, lease_expires_at FROM jobs WHERE queue = 'batch-q' AND state = 'active'",
		);
		for (const row of activeRows) {
			expect(row.locked_by).not.toBeNull();
			expect(row.lease_expires_at).not.toBeNull();
		}

		await boss2.stop();
	});
});

describe("Legacy coverage: concurrent claims no overlap", () => {
	it("two concurrent claims never return overlapping job ids", async () => {
		const boss = await createBoss(pool);

		// Enqueue 20 jobs
		for (let i = 0; i < 20; i++) {
			await boss.enqueue("overlap-q", { idx: i });
		}

		const claimedByWorker1: string[] = [];
		const claimedByWorker2: string[] = [];

		const pool2 = await createPool();
		const boss2 = new MysqlBoss({
			pool: pool2,
			pollIntervalMs: 50,
			batchSize: 10,
			concurrency: 10,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
			sweepIntervalMs: 5000,
			drainTimeoutMs: 5000,
		});
		await boss2.migrate();

		boss.work("overlap-q", async (job) => {
			claimedByWorker1.push(job.id);
		});

		boss2.work("overlap-q", async (job) => {
			claimedByWorker2.push(job.id);
		});

		// Wait for processing
		await new Promise((r) => setTimeout(r, 3000));

		await boss.stop();
		await boss2.stop();
		await pool2.end();

		// Check no overlap
		const overlap = claimedByWorker1.filter((id) =>
			claimedByWorker2.includes(id),
		);
		expect(overlap).toHaveLength(0);

		// All 20 should be processed
		expect(claimedByWorker1.length + claimedByWorker2.length).toBe(20);
	});
});

describe("Legacy coverage: singletonKey deduplication", () => {
	it("duplicate singletonKey on same queue returns null", async () => {
		const boss = await createBoss(pool);

		const id1 = await boss.enqueue(
			"singleton-q",
			{ v: 1 },
			{ singletonKey: "dedup" },
		);
		expect(id1).not.toBeNull();

		const id2 = await boss.enqueue(
			"singleton-q",
			{ v: 2 },
			{ singletonKey: "dedup" },
		);
		expect(id2).toBeNull();

		// Only one job exists
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs WHERE queue = 'singleton-q'",
		);
		expect(rows[0].cnt).toBe(1);
	});

	it("same singletonKey on different queue inserts", async () => {
		const boss = await createBoss(pool);

		const id1 = await boss.enqueue(
			"queue-a",
			{ v: 1 },
			{ singletonKey: "shared" },
		);
		const id2 = await boss.enqueue(
			"queue-b",
			{ v: 2 },
			{ singletonKey: "shared" },
		);

		expect(id1).not.toBeNull();
		expect(id2).not.toBeNull();
		expect(id1).not.toBe(id2);
	});
});

describe("Legacy coverage: fenced completion", () => {
	it("a completion attempt by a worker whose lease was reassigned affects 0 rows", async () => {
		const boss = await createBoss(pool, {
			leaseSeconds: 3,
			heartbeatSeconds: 1,
		});

		await boss.enqueue("fence-q", { data: "test" });

		let capturedJobId: string | null = null;
		const completionBlocked = new Promise<void>((resolve) => {
			boss.work("fence-q", async (job) => {
				capturedJobId = job.id;
				// Don't return — simulate long-running handler

				// Wait long enough for lease to expire
				await new Promise((r) => setTimeout(r, 5000));
				resolve();
			});
		});

		// Wait for claim
		await new Promise((r) => setTimeout(r, 500));
		expect(capturedJobId).not.toBeNull();

		// Manually expire the lease and reassign to simulate another worker claiming
		await pool.query(
			"UPDATE jobs SET locked_by = UUID_TO_BIN('00000000-0000-0000-0000-000000000001'), lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL 300 SECOND WHERE id = ?",
			[capturedJobId],
		);

		// The original handler will finish and try to complete — the fence prevents it
		await boss.stop();

		// Verify the job is still in the jobs table (not archived) since the fenced
		// completion should have failed
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs WHERE id = ?",
			[capturedJobId],
		);
		expect(rows[0].cnt).toBe(1);

		const [archived] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs_archive WHERE id = ?",
			[capturedJobId],
		);
		expect(archived[0].cnt).toBe(0);
	});
});

describe("Legacy coverage: READ COMMITTED + UTC timezone", () => {
	it("every pooled connection used by the library runs READ COMMITTED with UTC", async () => {
		const boss = await createBoss(pool);

		// Trigger a connection via enqueue
		await boss.enqueue("iso-q", { data: "test" });

		// Check session isolation level on a fresh connection from the pool
		const conn = await pool.getConnection();
		try {
			const [isoRows] = await conn.query<RowDataPacket[]>(
				"SELECT @@SESSION.transaction_isolation AS iso",
			);
			expect(isoRows[0].iso).toBe("READ-COMMITTED");

			const [tzRows] = await conn.query<RowDataPacket[]>(
				"SELECT @@SESSION.time_zone AS tz",
			);
			expect(tzRows[0].tz).toBe("+00:00");
		} finally {
			conn.release();
		}

		await boss.stop();
	});
});
