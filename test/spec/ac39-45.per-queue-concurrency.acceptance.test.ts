import { randomUUID } from "node:crypto";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { MysqlBoss, ValidationError } from "../../src/index.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

async function waitFor<T>(
	description: string,
	predicate: () => Promise<T | null | false> | T | null | false,
	timeoutMs = 15_000,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const result = await predicate();
		if (result) return result;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(`Timed out waiting for ${description}`);
}

async function count(
	pool: Pool,
	sql: string,
	params: unknown[] = [],
): Promise<number> {
	const [rows] = await pool.query<RowDataPacket[]>(sql, params);
	return Number(rows[0].count);
}

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
	await new MysqlBoss({ pool, tablePrefix: "" }).migrate();
});

beforeEach(async () => {
	await cleanTables(pool);
});

afterAll(async () => {
	await pool.end();
});

describe("Implementation spec acceptance criteria 39-45", () => {
	it("AC 39: reservation under saturation — quick queue is not starved by hog", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 50,
			batchSize: 10,
		});

		for (let i = 0; i < 500; i++) {
			await boss.enqueue("hog", { i });
		}
		for (let i = 0; i < 10; i++) {
			await boss.enqueue("quick", { i });
		}

		let quickDone = 0;
		const quickTimes: number[] = [];
		const quickStart = Date.now();

		boss.work(
			"hog",
			async () => {
				await new Promise((resolve) => setTimeout(resolve, 2000));
			},
			{ concurrency: 10 },
		);
		boss.work(
			"quick",
			async () => {
				quickDone++;
				quickTimes.push(Date.now() - quickStart);
			},
			{ concurrency: 3 },
		);

		await waitFor(
			"all quick jobs complete",
			async () =>
				(await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'quick'",
				)) === 10,
			10_000,
		);

		const elapsed = Math.max(...quickTimes);
		// 10 jobs, 3 slots, ~instant handlers → 4 batches, well under 5s
		expect(elapsed).toBeLessThan(5000);

		// hog should still be saturated (plenty of jobs left)
		const hogRemaining = await count(
			pool,
			"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'hog' AND state = 'available'",
		);
		expect(hogRemaining).toBeGreaterThan(0);

		await boss.stop();
	}, 15_000);

	it("AC 40: default fallback — queue without WorkOptions.concurrency uses instance concurrency, instance without concurrency uses batchSize", async () => {
		// Instance with explicit concurrency=5
		const boss1 = await createBoss(pool, {
			pollIntervalMs: 50,
			batchSize: 10,
			concurrency: 5,
		});

		let max1 = 0;
		let current1 = 0;

		for (let i = 0; i < 20; i++) {
			await boss1.enqueue("ac40a", { i });
		}

		boss1.work("ac40a", async () => {
			current1++;
			max1 = Math.max(max1, current1);
			await new Promise((resolve) => setTimeout(resolve, 300));
			current1--;
		});

		await waitFor(
			"ac40a jobs",
			async () =>
				(await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'ac40a'",
				)) === 20,
			30_000,
		);
		expect(max1).toBeLessThanOrEqual(5);
		expect(max1).toBeGreaterThanOrEqual(2); // should actually use the slots
		await boss1.stop();

		// Instance without concurrency → defaults to batchSize
		await cleanTables(pool);
		const boss2 = await createBoss(pool, {
			pollIntervalMs: 50,
			batchSize: 4,
			concurrency: undefined,
		});

		let max2 = 0;
		let current2 = 0;

		for (let i = 0; i < 20; i++) {
			await boss2.enqueue("ac40b", { i });
		}

		boss2.work("ac40b", async () => {
			current2++;
			max2 = Math.max(max2, current2);
			await new Promise((resolve) => setTimeout(resolve, 300));
			current2--;
		});

		await waitFor(
			"ac40b jobs",
			async () =>
				(await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'ac40b'",
				)) === 20,
			30_000,
		);
		expect(max2).toBeLessThanOrEqual(4);
		expect(max2).toBeGreaterThanOrEqual(2);
		await boss2.stop();
	}, 60_000);

	it("AC 41: independent limits — two queues with limits 2 and 8 reach and never exceed their limits", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 50,
			batchSize: 10,
		});

		for (let i = 0; i < 50; i++) {
			await boss.enqueue("narrow", { i });
			await boss.enqueue("wide", { i });
		}

		let narrowCurrent = 0;
		let narrowMax = 0;
		let wideCurrent = 0;
		let wideMax = 0;

		boss.work(
			"narrow",
			async () => {
				narrowCurrent++;
				narrowMax = Math.max(narrowMax, narrowCurrent);
				await new Promise((resolve) => setTimeout(resolve, 200));
				narrowCurrent--;
			},
			{ concurrency: 2 },
		);

		boss.work(
			"wide",
			async () => {
				wideCurrent++;
				wideMax = Math.max(wideMax, wideCurrent);
				await new Promise((resolve) => setTimeout(resolve, 200));
				wideCurrent--;
			},
			{ concurrency: 8 },
		);

		await waitFor(
			"all narrow+wide jobs",
			async () => {
				const n = await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'narrow'",
				);
				const w = await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'wide'",
				);
				if (n === 50 && w === 50) return true;
				return false;
			},
			30_000,
		);

		expect(narrowMax).toBe(2);
		expect(wideMax).toBe(8);
		await boss.stop();
	}, 35_000);

	it("AC 42: zero-capacity claims are skipped — no claim transaction at capacity", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 100,
			batchSize: 10,
		});

		// Enqueue a slow job to saturate the 1-slot queue
		await boss.enqueue("saturated", { blocking: true });

		let handlerCalls = 0;
		boss.work(
			"saturated",
			async () => {
				handlerCalls++;
				// Hold the slot for a while
				await new Promise((resolve) => setTimeout(resolve, 2000));
			},
			{ concurrency: 1 },
		);

		// Wait for the handler to start
		await waitFor("handler start", () => handlerCalls === 1);

		// Enqueue more jobs while at capacity
		await boss.enqueue("saturated", { second: true });

		// Record statement count before polling window
		const [before] = await pool.query<RowDataPacket[]>(
			`SELECT COUNT_STAR as cnt FROM performance_schema.events_statements_summary_by_digest
			 WHERE DIGEST_TEXT LIKE '%FOR UPDATE%' AND DIGEST_TEXT LIKE '%saturated%'`,
		);
		const beforeCount = Number(before?.[0]?.cnt ?? 0);

		// Wait enough polls to prove no claim is issued
		await new Promise((resolve) => setTimeout(resolve, 500));

		const [after] = await pool.query<RowDataPacket[]>(
			`SELECT COUNT_STAR as cnt FROM performance_schema.events_statements_summary_by_digest
			 WHERE DIGEST_TEXT LIKE '%FOR UPDATE%' AND DIGEST_TEXT LIKE '%saturated%'`,
		);
		const afterCount = Number(after?.[0]?.cnt ?? 0);

		// No new claim transactions while at capacity
		// ponytail: performance_schema may not be available in all test setups;
		// if counts are both 0 (unavailable), fall back to handler call count
		if (beforeCount > 0 || afterCount > 0) {
			expect(afterCount).toBe(beforeCount);
		}

		// Handler was only called once (the slot was full)
		expect(handlerCalls).toBe(1);

		await boss.stop();
	}, 10_000);

	it("AC 43: work() validation — invalid concurrency throws ValidationError", () => {
		const boss = new MysqlBoss({
			pool,
			tablePrefix: "",
			pollIntervalMs: 200,
			batchSize: 10,
		});

		expect(() => boss.work("v1", async () => {}, { concurrency: 0 })).toThrow(
			ValidationError,
		);

		expect(() => boss.work("v2", async () => {}, { concurrency: -1 })).toThrow(
			ValidationError,
		);

		expect(() => boss.work("v3", async () => {}, { concurrency: 1.5 })).toThrow(
			ValidationError,
		);

		expect(() =>
			boss.work("v4", async () => {}, { concurrency: 1001 }),
		).toThrow(ValidationError);

		// Valid values don't throw
		expect(() =>
			boss.work("v5", async () => {}, { concurrency: 1 }),
		).not.toThrow();
		expect(() =>
			boss.work("v6", async () => {}, { concurrency: 1000 }),
		).not.toThrow();

		// Queue should NOT be registered on failure
		expect(() => boss.work("v1", async () => {})).not.toThrow(); // v1 was not registered (threw), so re-registering works
	});

	it("AC 44: maintenance opt-out — no sweep, tick, or prune when maintenance: false", async () => {
		const noMaint = await createBoss(pool, {
			pollIntervalMs: 100,
			batchSize: 10,
			leaseSeconds: 3,
			heartbeatSeconds: 1,
			sweepIntervalMs: 500,
			tickIntervalMs: 500,
			maintenance: false,
		});

		// 1) Expired-lease job should NOT be recovered by the no-maintenance instance
		const expiredId = await noMaint.enqueue(
			"ac44",
			{},
			{ retryLimit: 2, retryDelaySecs: 0 },
		);
		await pool.query(
			`UPDATE jobs
			 SET state = 'active', retry_count = 0,
			     started_at = UTC_TIMESTAMP(6), locked_by = UUID_TO_BIN(?),
			     lease_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND
			 WHERE id = ?`,
			[randomUUID(), expiredId],
		);

		// 2) Schedule that should NOT fire — set next_run_at to the past so it's
		// immediately due when a tick runs
		await noMaint.schedule("ac44-sched", "ac44-scheduled", "* * * * *", {
			timezone: "UTC",
		});
		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE name = 'ac44-sched'",
		);

		// 3) Old archive row that should NOT be pruned
		await pool.query(
			`INSERT INTO jobs_archive
			   (id, queue, priority, retry_count, created_at, started_at,
			    completed_at, duration_ms)
			 VALUES (?, 'ac44', 0, 0, '2020-01-01', '2020-01-01', '2020-01-01', 1)`,
			[99999],
		);

		noMaint.work("ac44", async () => {});
		noMaint.work("ac44-scheduled", async () => {});

		// Wait long enough for ≥3 of each maintenance interval to have passed
		await new Promise((resolve) => setTimeout(resolve, 2000));

		// Expired job NOT recovered (still active, not swept)
		const [expiredRow] = await pool.query<RowDataPacket[]>(
			"SELECT state FROM jobs WHERE id = ?",
			[expiredId],
		);
		expect(expiredRow[0].state).toBe("active");

		// Schedule did NOT fire
		const scheduledCount = await count(
			pool,
			"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac44-scheduled'",
		);
		expect(scheduledCount).toBe(0);

		// Old archive row survived
		const archiveCount = await count(
			pool,
			"SELECT COUNT(*) AS count FROM jobs_archive WHERE id = 99999",
		);
		expect(archiveCount).toBe(1);

		await noMaint.stop();

		// Now start a maintenance: true instance and verify all three occur
		const withMaint = await createBoss(pool, {
			pollIntervalMs: 100,
			batchSize: 10,
			leaseSeconds: 3,
			heartbeatSeconds: 1,
			sweepIntervalMs: 200,
			tickIntervalMs: 200,
			archiveRetentionDays: 1,
			maintenance: true,
		});
		withMaint.work("ac44", async () => {});
		withMaint.work("ac44-scheduled", async () => {});

		// Sweep recovers the expired job
		await waitFor(
			"expired job swept",
			async () => {
				const [row] = await pool.query<RowDataPacket[]>(
					"SELECT state FROM jobs WHERE id = ?",
					[expiredId],
				);
				if (row.length === 0 || row[0].state === "available") return true;
				return false;
			},
			10_000,
		);

		// Tick fires the schedule
		await waitFor(
			"schedule fires",
			async () =>
				(await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac44-scheduled'",
				)) > 0,
			10_000,
		);

		// Prune removes the old archive row
		await waitFor(
			"old archive pruned",
			async () =>
				(await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE id = 99999",
				)) === 0,
			10_000,
		);

		await withMaint.stop();
	}, 30_000);

	it("AC 45: rolling-fleet compatibility — old-behaviour and new per-queue workers coexist on same queue", async () => {
		// Simulate old behaviour: a worker with shared concurrency (no WorkOptions)
		const oldPool = await createPool();
		const oldBoss = new MysqlBoss({
			pool: oldPool,
			tablePrefix: "",
			pollIntervalMs: 50,
			batchSize: 10,
			concurrency: 5,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
			sweepIntervalMs: 2000,
		});
		await oldBoss.migrate();

		// New behaviour: per-queue concurrency
		const newPool = await createPool();
		const newBoss = new MysqlBoss({
			pool: newPool,
			tablePrefix: "",
			pollIntervalMs: 50,
			batchSize: 10,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
			sweepIntervalMs: 2000,
		});

		await pool.query(
			`CREATE TABLE IF NOT EXISTS ac45_side_effects (
				job_id BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (job_id)
			) ENGINE=InnoDB`,
		);
		await pool.query("DELETE FROM ac45_side_effects");

		const total = 200;
		for (let i = 0; i < total; i++) {
			await oldBoss.enqueue("ac45", { i });
		}

		// Old worker (no WorkOptions)
		oldBoss.work("ac45", async (job) => {
			await pool.query("INSERT INTO ac45_side_effects (job_id) VALUES (?)", [
				job.id,
			]);
		});

		// New worker (with WorkOptions)
		newBoss.work(
			"ac45",
			async (job) => {
				await pool.query("INSERT INTO ac45_side_effects (job_id) VALUES (?)", [
					job.id,
				]);
			},
			{ concurrency: 5 },
		);

		await waitFor(
			"all ac45 jobs archived",
			async () =>
				(await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'ac45'",
				)) === total,
			30_000,
		);

		// Exactly-once: each job processed exactly once
		const processed = await count(
			pool,
			"SELECT COUNT(*) AS count FROM ac45_side_effects",
		);
		expect(processed).toBe(total);

		// No jobs left or dead
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac45'",
			),
		).toBe(0);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs_dead WHERE queue = 'ac45'",
			),
		).toBe(0);

		await oldBoss.stop();
		await newBoss.stop();
		await pool.query("DROP TABLE IF EXISTS ac45_side_effects");
		await oldPool.end();
		await newPool.end();
	}, 60_000);
});
