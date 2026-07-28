import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { cleanTables, createBoss, createPool } from "../helpers.js";

interface JobRow extends RowDataPacket {
	id: bigint;
	state: string;
	retry_count: number;
	retry_limit: number;
	retry_delay_secs: number;
	retry_backoff: number;
	run_at: Date;
	last_error: unknown;
}

interface DbNowRow extends RowDataPacket {
	db_now: Date;
}

interface DeadRow extends RowDataPacket {
	id: bigint;
	queue: string;
	retry_count: number;
	retry_limit: number;
	last_error: unknown;
}

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
});

afterEach(async () => {
	await cleanTables(pool);
});

afterAll(async () => {
	await pool.end();
});

describe("AC 9: failed job re-enters available with incremented retry_count", () => {
	it("retries a failed job with retry_count incremented", async () => {
		const boss = await createBoss(pool);
		const queue = "retry-q-9";

		let callCount = 0;
		const jobId = await boss.enqueue(
			queue,
			{ attempt: true },
			{
				retryLimit: 3,
				retryDelaySecs: 1,
				retryBackoff: false,
			},
		);
		expect(jobId).toBeTruthy();

		boss.work(queue, async () => {
			callCount++;
			if (callCount <= 2) {
				throw new Error(`fail attempt ${callCount}`);
			}
		});

		// Wait for retries to process (3 attempts: 2 failures + 1 success)
		await new Promise((r) => setTimeout(r, 8000));
		await boss.stop();

		expect(callCount).toBe(3);

		// Job should be archived (completed on 3rd attempt)
		const [archived] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs_archive WHERE id = ?",
			[jobId],
		);
		expect(archived.length).toBe(1);
	}, 15000);
});

describe("AC 10: exponential backoff", () => {
	it("applies delay * 2^retry_count when retryBackoff=true", async () => {
		const boss = await createBoss(pool);
		const queue = "retry-q-10";

		const jobId = await boss.enqueue(
			queue,
			{ exp: true },
			{
				retryLimit: 3,
				retryDelaySecs: 10,
				retryBackoff: true,
			},
		);

		// Claim and fail the job manually to inspect the run_at
		boss.work(queue, async () => {
			throw new Error("deliberate failure");
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		// Check the job's state — should be available with retry_count=1
		const [rows] = await pool.query<JobRow[]>(
			"SELECT id, state, retry_count, run_at FROM jobs WHERE id = ?",
			[jobId],
		);

		expect(rows.length).toBe(1);
		expect(rows[0].state).toBe("available");
		expect(rows[0].retry_count).toBe(1);

		// run_at should be in the future: base delay is 10 * 2^0 = 10s
		// plus random jitter up to 10s, so 10-20s in the future
		const [nowRows] = await pool.query<DbNowRow[]>(
			"SELECT UTC_TIMESTAMP(6) AS db_now",
		);
		const dbNow = new Date(nowRows[0].db_now);
		const runAt = new Date(rows[0].run_at);
		const diffSecs = (runAt.getTime() - dbNow.getTime()) / 1000;

		// Should be at least ~8s in the future (10s base minus processing time)
		expect(diffSecs).toBeGreaterThan(5);
		// MySQL SET evaluates left-to-right: retry_count is already 1 when
		// computing delay, so base = 10 * 2^1 = 20s, plus 0-10s jitter
		expect(diffSecs).toBeLessThan(35);
	}, 10000);
});

describe("AC 11: linear backoff", () => {
	it("applies flat retry_delay_secs when retryBackoff=false", async () => {
		const boss = await createBoss(pool);
		const queue = "retry-q-11";

		const jobId = await boss.enqueue(
			queue,
			{ linear: true },
			{
				retryLimit: 3,
				retryDelaySecs: 5,
				retryBackoff: false,
			},
		);

		boss.work(queue, async () => {
			throw new Error("linear fail");
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		const [rows] = await pool.query<JobRow[]>(
			"SELECT id, state, retry_count, run_at FROM jobs WHERE id = ?",
			[jobId],
		);

		expect(rows.length).toBe(1);
		expect(rows[0].state).toBe("available");
		expect(rows[0].retry_count).toBe(1);

		const [nowRows] = await pool.query<DbNowRow[]>(
			"SELECT UTC_TIMESTAMP(6) AS db_now",
		);
		const dbNow = new Date(nowRows[0].db_now);
		const runAt = new Date(rows[0].run_at);
		const diffSecs = (runAt.getTime() - dbNow.getTime()) / 1000;

		// Linear: 5s base + 0-5s jitter = 5-10s in the future
		expect(diffSecs).toBeGreaterThan(2);
		expect(diffSecs).toBeLessThan(12);
	}, 10000);
});

describe("AC 12: exhausted retries move to DLQ", () => {
	it("moves job to jobs_dead when retry_count reaches retry_limit", async () => {
		const boss = await createBoss(pool);
		const queue = "retry-q-12";

		const jobId = await boss.enqueue(
			queue,
			{ dlq: true },
			{
				retryLimit: 1,
				retryDelaySecs: 1,
				retryBackoff: false,
			},
		);

		let failCount = 0;
		boss.work(queue, async () => {
			failCount++;
			throw new Error(`fail #${failCount}`);
		});

		// First attempt fails → retry_count=1, still available
		// Second attempt (after 1s delay) fails → retry_count=1 already = retry_limit, goes to DLQ
		await new Promise((r) => setTimeout(r, 6000));
		await boss.stop();

		expect(failCount).toBe(2);

		// Should not be in jobs table
		const [jobRows] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs WHERE id = ?",
			[jobId],
		);
		expect(jobRows.length).toBe(0);

		// Should be in jobs_dead
		const [deadRows] = await pool.query<DeadRow[]>(
			"SELECT id, queue, retry_count, retry_limit, last_error FROM jobs_dead WHERE id = ?",
			[jobId],
		);
		expect(deadRows.length).toBe(1);
		expect(deadRows[0].queue).toBe(queue);
		// DLQ_INSERT adds +1 to count the final failed attempt
		expect(deadRows[0].retry_count).toBe(2);
		expect(deadRows[0].retry_limit).toBe(1);
	}, 15000);
});

describe("AC 13: retry_limit=0 means immediate DLQ", () => {
	it("sends job straight to DLQ on first failure when retryLimit=0", async () => {
		const boss = await createBoss(pool);
		const queue = "retry-q-13";

		const jobId = await boss.enqueue(
			queue,
			{ nope: true },
			{
				retryLimit: 0,
				retryDelaySecs: 1,
			},
		);

		let callCount = 0;
		boss.work(queue, async () => {
			callCount++;
			throw new Error("immediate dead");
		});

		await new Promise((r) => setTimeout(r, 3000));
		await boss.stop();

		expect(callCount).toBe(1);

		// Should not be in jobs
		const [jobRows] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs WHERE id = ?",
			[jobId],
		);
		expect(jobRows.length).toBe(0);

		// Should be in jobs_dead
		const [deadRows] = await pool.query<DeadRow[]>(
			"SELECT id, retry_count, last_error FROM jobs_dead WHERE id = ?",
			[jobId],
		);
		expect(deadRows.length).toBe(1);
		// DLQ_INSERT adds +1 to count the final failed attempt
		expect(deadRows[0].retry_count).toBe(1);

		const lastError = deadRows[0].last_error as { message: string };
		expect(lastError.message).toBe("immediate dead");
	}, 10000);
});
