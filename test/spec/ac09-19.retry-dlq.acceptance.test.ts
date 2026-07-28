import { randomUUID } from "node:crypto";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { claimJobs } from "../../src/claim.js";
import { deadLetterJob, replayDead } from "../../src/dlq.js";
import { SingletonCollisionError } from "../../src/errors.js";
import { failJob } from "../../src/fail.js";
import { MysqlBoss } from "../../src/index.js";
import { DLQ_INSERT } from "../../src/sql.js";
import { sweepStaleJobs } from "../../src/sweep.js";
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

async function insertDead(
	pool: Pool,
	id: number,
	queue: string,
	singletonKey: string | null = null,
	failedAt = "2026-01-01 00:00:00.000",
): Promise<void> {
	await pool.query(
		`INSERT INTO jobs_dead
		   (id, queue, priority, payload, singleton_key, retry_count, retry_limit,
		    retry_delay_secs, retry_backoff, created_at, failed_at, last_error)
		 VALUES (?, ?, 4, JSON_OBJECT('id', ?), ?, 3, 2, 1, 0,
		         '2025-01-01 00:00:00.000', ?,
		         JSON_OBJECT('message', 'dead', 'at', '2026-01-01T00:00:00.000Z'))`,
		[id, queue, id, singletonKey, failedAt],
	);
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

describe("Implementation spec acceptance criteria 9-19", () => {
	it("AC 9: fixed retry increments atomically and schedules from the DB clock", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue(
			"ac9",
			{},
			{ retryLimit: 3, retryDelaySecs: 4, retryBackoff: false },
		);
		const workerId = randomUUID();
		await claimJobs(pool, "ac9", workerId, 1, 30);
		expect(
			await failJob(pool, id!, workerId, {
				message: "fixed",
				at: new Date().toISOString(),
			}),
		).toBe(true);

		const [rows] = await pool.query<RowDataPacket[]>(
			`SELECT state, retry_count,
			        TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), run_at) AS delay
			 FROM jobs WHERE id = ?`,
			[id],
		);
		expect(rows[0].state).toBe("available");
		expect(rows[0].retry_count).toBe(1);
		expect(Number(rows[0].delay)).toBeGreaterThanOrEqual(3);
		expect(Number(rows[0].delay)).toBeLessThanOrEqual(8);
		await boss.stop();
	});

	it("AC 10: exponential retries follow 2^n and cap at 86,400 seconds", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue(
			"ac10",
			{},
			{ retryLimit: 30, retryDelaySecs: 2, retryBackoff: true },
		);
		const workerId = randomUUID();

		for (let retryCount = 0; retryCount < 4; retryCount++) {
			await pool.query(
				"UPDATE jobs SET state = 'available', run_at = UTC_TIMESTAMP(6) WHERE id = ?",
				[id],
			);
			await claimJobs(pool, "ac10", workerId, 1, 30);
			await failJob(pool, id!, workerId, {
				message: `failure-${retryCount}`,
				at: new Date().toISOString(),
			});
			const [rows] = await pool.query<RowDataPacket[]>(
				`SELECT retry_count,
				        TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), run_at) AS delay
				 FROM jobs WHERE id = ?`,
				[id],
			);
			const base = 2 * 2 ** retryCount;
			expect(rows[0].retry_count).toBe(retryCount + 1);
			expect(Number(rows[0].delay)).toBeGreaterThanOrEqual(base - 1);
			expect(Number(rows[0].delay)).toBeLessThanOrEqual(base + 2);
		}

		await pool.query(
			`UPDATE jobs
			 SET state = 'available', run_at = UTC_TIMESTAMP(6), retry_count = 20
			 WHERE id = ?`,
			[id],
		);
		await claimJobs(pool, "ac10", workerId, 1, 30);
		await failJob(pool, id!, workerId, {
			message: "capped",
			at: new Date().toISOString(),
		});
		const [capped] = await pool.query<RowDataPacket[]>(
			"SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), run_at) AS delay FROM jobs WHERE id = ?",
			[id],
		);
		expect(Number(capped[0].delay)).toBeGreaterThanOrEqual(86_398);
		expect(Number(capped[0].delay)).toBeLessThanOrEqual(86_400);
		await boss.stop();
	});

	it("AC 11: total executions never exceed retryLimit + 1 including lease expiry", async () => {
		const boss = await createBoss(pool, { pollIntervalMs: 20 });
		const id = await boss.enqueue(
			"ac11",
			{},
			{ retryLimit: 1, retryDelaySecs: 0 },
		);
		await pool.query(
			`UPDATE jobs
			 SET state = 'active', started_at = UTC_TIMESTAMP(6),
			     locked_by = UUID_TO_BIN(?),
			     lease_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND
			 WHERE id = ?`,
			[randomUUID(), id],
		);
		await sweepStaleJobs(pool);

		let handlerExecutions = 0;
		boss.work("ac11", async () => {
			handlerExecutions++;
			throw new Error("final failure");
		});
		const dead = await waitFor("AC 11 dead job", async () => {
			const rows = await boss.listDead({ queue: "ac11" });
			return rows[0] ?? null;
		});
		expect(handlerExecutions).toBe(1);
		expect(dead.retryCount).toBe(2);
		await boss.stop();
	});

	it("AC 12: last_error is replaced on every handler failure", async () => {
		const boss = await createBoss(pool, { pollIntervalMs: 20 });
		const id = await boss.enqueue(
			"ac12",
			{},
			{ retryLimit: 2, retryDelaySecs: 0 },
		);
		const observed: string[] = [];
		let attempt = 0;
		boss.work("ac12", async () => {
			attempt++;
			if (attempt > 1) {
				const [rows] = await pool.query<RowDataPacket[]>(
					"SELECT JSON_UNQUOTE(JSON_EXTRACT(last_error, '$.message')) AS message FROM jobs WHERE id = ?",
					[id],
				);
				observed.push(rows[0].message);
			}
			if (attempt <= 2) throw new Error(`failure-${attempt}`);
		});
		await waitFor("AC 12 archive", () => boss.getArchivedJob(id!));
		expect(observed).toEqual(["failure-1", "failure-2"]);
		await boss.stop();
	});

	it("AC 13: failure by a worker that lost its lease affects no rows", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue("ac13", {}, { retryLimit: 3 });
		const firstWorker = randomUUID();
		await claimJobs(pool, "ac13", firstWorker, 1, 30);
		await pool.query(
			"UPDATE jobs SET locked_by = UUID_TO_BIN(?) WHERE id = ?",
			[randomUUID(), id],
		);
		expect(
			await failJob(pool, id!, firstWorker, {
				message: "zombie",
				at: new Date().toISOString(),
			}),
		).toBe(false);
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT state, retry_count, last_error FROM jobs WHERE id = ?",
			[id],
		);
		expect(rows[0].state).toBe("active");
		expect(rows[0].retry_count).toBe(0);
		expect(rows[0].last_error).toBeNull();
		await boss.stop();
	});

	it("AC 14: DLQ move is never externally visible as both or neither", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue("ac14", {}, { retryLimit: 0 });
		const workerId = randomUUID();
		await claimJobs(pool, "ac14", workerId, 1, 30);

		const movingConnection = await pool.getConnection();
		const observer = await pool.getConnection();
		try {
			await observer.query(
				"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			);
			await movingConnection.beginTransaction();
			await movingConnection.query(DLQ_INSERT, [
				JSON.stringify({
					message: "fault",
					at: new Date().toISOString(),
				}),
				id,
				workerId,
			]);

			const [during] = await observer.query<RowDataPacket[]>(
				`SELECT
				   (SELECT COUNT(*) FROM jobs WHERE id = ?) AS hot_count,
				   (SELECT COUNT(*) FROM jobs_dead WHERE id = ?) AS dead_count`,
				[id, id],
			);
			expect([during[0].hot_count, during[0].dead_count]).toEqual([1, 0]);

			await movingConnection.rollback();
			const [afterFault] = await observer.query<RowDataPacket[]>(
				`SELECT
				   (SELECT COUNT(*) FROM jobs WHERE id = ?) AS hot_count,
				   (SELECT COUNT(*) FROM jobs_dead WHERE id = ?) AS dead_count`,
				[id, id],
			);
			expect([afterFault[0].hot_count, afterFault[0].dead_count]).toEqual([
				1, 0,
			]);
		} finally {
			movingConnection.release();
			observer.release();
		}

		expect(
			await deadLetterJob(pool, id!, workerId, {
				message: "final",
				at: new Date().toISOString(),
			}),
		).toBe(true);
		expect(
			await count(pool, "SELECT COUNT(*) AS count FROM jobs WHERE id = ?", [
				id,
			]),
		).toBe(0);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs_dead WHERE id = ?",
				[id],
			),
		).toBe(1);
		await boss.stop();
	});

	it("AC 15: listDead filters its window and paginates by queue", async () => {
		for (let index = 0; index < 5; index++) {
			await insertDead(
				pool,
				1_500 + index,
				"ac15",
				null,
				`2026-01-0${index + 1} 00:00:00.000`,
			);
		}
		await insertDead(pool, 1_600, "other");
		const boss = await createBoss(pool);
		const first = await boss.listDead({
			queue: "ac15",
			after: new Date("2026-01-01T12:00:00Z"),
			before: new Date("2026-01-06T00:00:00Z"),
			limit: 2,
			offset: 0,
		});
		const second = await boss.listDead({
			queue: "ac15",
			after: new Date("2026-01-01T12:00:00Z"),
			before: new Date("2026-01-06T00:00:00Z"),
			limit: 2,
			offset: 2,
		});
		expect(first).toHaveLength(2);
		expect(second).toHaveLength(2);
		expect(new Set([...first, ...second].map((job) => job.id)).size).toBe(4);
		expect([...first, ...second].every((job) => job.queue === "ac15")).toBe(
			true,
		);
		await boss.stop();
	});

	it("AC 16: replay creates a fresh available job with reset counters atomically", async () => {
		await insertDead(pool, 1_700, "ac16");
		const boss = await createBoss(pool);
		expect(await boss.replayDead(["1700"])).toBe(1);
		const [jobs] = await pool.query<RowDataPacket[]>(
			"SELECT CAST(id AS CHAR) AS id, state, retry_count FROM jobs WHERE queue = 'ac16'",
		);
		expect(jobs).toHaveLength(1);
		expect(jobs[0].id).not.toBe("1700");
		expect(jobs[0].state).toBe("available");
		expect(jobs[0].retry_count).toBe(0);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs_dead WHERE id = 1700",
			),
		).toBe(0);
		await boss.stop();
	});

	it("AC 17: concurrent replay of one dead ID creates exactly one job", async () => {
		await insertDead(pool, 1_800, "ac17");
		const secondPool = await createPool();
		try {
			const results = await Promise.all([
				replayDead(pool, ["1800"]),
				replayDead(secondPool, ["1800"]),
			]);
			expect(results.sort()).toEqual([0, 1]);
			expect(
				await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac17'",
				),
			).toBe(1);
			expect(
				await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_dead WHERE id = 1800",
				),
			).toBe(0);
		} finally {
			await secondPool.end();
		}
	});

	it("AC 18: singleton collision rolls replay back and preserves the dead row", async () => {
		const boss = await createBoss(pool);
		await boss.enqueue("ac18", {}, { singletonKey: "collision" });
		await insertDead(pool, 1_900, "ac18", "collision");
		await expect(boss.replayDead(["1900"])).rejects.toBeInstanceOf(
			SingletonCollisionError,
		);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs_dead WHERE id = 1900",
			),
		).toBe(1);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac18'",
			),
		).toBe(1);
		await boss.stop();
	});

	it("AC 19: dead-letter ID equals the original job ID", async () => {
		const boss = await createBoss(pool, { pollIntervalMs: 20 });
		const id = await boss.enqueue("ac19", {}, { retryLimit: 0 });
		boss.work("ac19", async () => {
			throw new Error("dead");
		});
		const dead = await waitFor("AC 19 dead row", async () => {
			const rows = await boss.listDead({ queue: "ac19" });
			return rows[0] ?? null;
		});
		expect(dead.id).toBe(id);
		await boss.stop();
	});
});
