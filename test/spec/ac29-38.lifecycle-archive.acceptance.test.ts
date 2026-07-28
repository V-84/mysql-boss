import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { pruneArchive } from "../../src/archive.js";
import { claimJobs } from "../../src/claim.js";
import { ConfigError, MysqlBoss } from "../../src/index.js";
import {
	ARCHIVE_PRUNE,
	COMPLETE_ARCHIVE,
	COMPLETE_DELETE,
} from "../../src/sql.js";
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

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
	await new MysqlBoss({ pool }).migrate();
});

beforeEach(async () => {
	await cleanTables(pool);
});

afterAll(async () => {
	await pool.end();
});

describe("Implementation spec acceptance criteria 29-38", () => {
	it("AC 29: SIGKILL mid-job recovers through lease expiry and sweep", async () => {
		const producer = await createBoss(pool);
		const queue = `ac29-${randomUUID()}`;
		const id = await producer.enqueue(
			queue,
			{},
			{ retryLimit: 2, retryDelaySecs: 0 },
		);
		await producer.stop();

		const fixture = fileURLToPath(
			new URL("../fixtures/crash-worker.mjs", import.meta.url),
		);
		const child = fork(fixture, {
			env: {
				...process.env,
				CRASH_QUEUE: queue,
			},
			stdio: ["ignore", "pipe", "pipe", "ipc"],
		});
		try {
			await new Promise<void>((resolve, reject) => {
				const timeout = setTimeout(
					() => reject(new Error("Crash worker did not claim the job")),
					10_000,
				);
				child.on("error", reject);
				child.on("message", (message) => {
					if (
						typeof message === "object" &&
						message !== null &&
						"type" in message &&
						message.type === "claimed"
					) {
						clearTimeout(timeout);
						resolve();
					}
				});
			});
			child.kill("SIGKILL");
			await new Promise<void>((resolve) => child.once("exit", () => resolve()));

			const recovery = await createBoss(pool, {
				pollIntervalMs: 20,
				leaseSeconds: 3,
				heartbeatSeconds: 1,
				sweepIntervalMs: 200,
			});
			let retryCount = -1;
			let leaseError: string | null = null;
			recovery.work(queue, async (job) => {
				retryCount = job.retryCount;
				const [rows] = await pool.query<RowDataPacket[]>(
					"SELECT JSON_UNQUOTE(JSON_EXTRACT(last_error, '$.message')) AS message FROM jobs WHERE id = ?",
					[id],
				);
				leaseError = rows[0].message;
			});
			await waitFor("SIGKILL recovery archive", () =>
				recovery.getArchivedJob(id!),
			);
			expect(retryCount).toBe(1);
			expect(leaseError).toBe("lease expired");
			await recovery.stop();
		} finally {
			if (!child.killed) child.kill("SIGKILL");
		}
	}, 20_000);

	it("AC 30: heartbeat protects a handler that runs longer than its lease", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 20,
			leaseSeconds: 3,
			heartbeatSeconds: 1,
			sweepIntervalMs: 200,
		});
		const id = await boss.enqueue("ac30", {});
		let executions = 0;
		boss.work("ac30", async () => {
			executions++;
			await new Promise((resolve) => setTimeout(resolve, 4_000));
		});
		await waitFor("heartbeat-protected archive", () =>
			boss.getArchivedJob(id!),
		);
		expect(executions).toBe(1);
		await boss.stop();
	}, 10_000);

	it("AC 31: graceful stop drains and archives work within the timeout", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 20,
			drainTimeoutMs: 5_000,
		});
		const id = await boss.enqueue("ac31", {});
		let started = false;
		boss.work("ac31", async () => {
			started = true;
			await new Promise((resolve) => setTimeout(resolve, 300));
		});
		await waitFor("AC 31 handler start", () => started);
		await boss.stop();
		expect(await boss.getArchivedJob(id!)).not.toBeNull();
	});

	it("AC 32: timed-out stop aborts and immediately releases without penalty", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 20,
			drainTimeoutMs: 500,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
		});
		const id = await boss.enqueue("ac32", {});
		let signal: AbortSignal | null = null;
		boss.work("ac32", async (_job, context) => {
			signal = context.signal;
			await new Promise(() => {});
		});
		await waitFor("AC 32 handler start", () => signal !== null);
		const stopStarted = Date.now();
		await boss.stop();
		expect(Date.now() - stopStarted).toBeLessThan(2_500);
		expect(signal?.aborted).toBe(true);

		const [released] = await pool.query<RowDataPacket[]>(
			"SELECT state, retry_count, locked_by, lease_expires_at FROM jobs WHERE id = ?",
			[id],
		);
		expect(released[0].state).toBe("available");
		expect(released[0].retry_count).toBe(0);
		expect(released[0].locked_by).toBeNull();
		expect(released[0].lease_expires_at).toBeNull();

		const nextBoss = await createBoss(pool, { pollIntervalMs: 20 });
		let observedRetryCount = -1;
		nextBoss.work("ac32", async (job) => {
			observedRetryCount = job.retryCount;
		});
		await waitFor("immediate AC 32 reclaim", () =>
			nextBoss.getArchivedJob(id!),
		);
		expect(observedRetryCount).toBe(0);
		await nextBoss.stop();
	});

	it("AC 33: sweep dead-letters an expired job with exhausted retries", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue("ac33", {}, { retryLimit: 1 });
		await pool.query(
			`UPDATE jobs
			 SET state = 'active', retry_count = retry_limit,
			     started_at = UTC_TIMESTAMP(6), locked_by = UUID_TO_BIN(?),
			     lease_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND
			 WHERE id = ?`,
			[randomUUID(), id],
		);
		expect(await sweepStaleJobs(pool)).toBe(1);
		expect(
			await count(pool, "SELECT COUNT(*) AS count FROM jobs WHERE id = ?", [
				id,
			]),
		).toBe(0);
		const dead = await boss.listDead({ queue: "ac33" });
		expect(dead[0].id).toBe(id);
		expect(dead[0].lastError?.message).toBe("lease expired");
		await boss.stop();
	});

	it("AC 34: invalid lease-to-heartbeat ratio throws ConfigError synchronously", () => {
		expect(
			() =>
				new MysqlBoss({
					pool,
					leaseSeconds: 29,
					heartbeatSeconds: 10,
				}),
		).toThrow(ConfigError);
	});

	it("AC 35: completion is atomic and records measured duration", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue("ac35", {});
		const workerId = randomUUID();
		await claimJobs(pool, "ac35", workerId, 1, 30);
		await new Promise((resolve) => setTimeout(resolve, 100));

		const mover = await pool.getConnection();
		const observer = await pool.getConnection();
		try {
			await observer.query(
				"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			);
			await mover.beginTransaction();
			await mover.query(COMPLETE_ARCHIVE, [id, workerId]);
			const [during] = await observer.query<RowDataPacket[]>(
				`SELECT
				   (SELECT COUNT(*) FROM jobs WHERE id = ?) AS hot_count,
				   (SELECT COUNT(*) FROM jobs_archive WHERE id = ?) AS archive_count`,
				[id, id],
			);
			expect([during[0].hot_count, during[0].archive_count]).toEqual([1, 0]);
			await mover.query(COMPLETE_DELETE, [id, workerId]);
			await mover.commit();
		} finally {
			mover.release();
			observer.release();
		}

		const archived = await boss.getArchivedJob(id!);
		expect(archived).not.toBeNull();
		expect(archived!.durationMs).toBeGreaterThanOrEqual(90);
		expect(
			await count(pool, "SELECT COUNT(*) AS count FROM jobs WHERE id = ?", [
				id,
			]),
		).toBe(0);
		await boss.stop();
	});

	it("AC 36: getArchivedJob returns records and null for unknown IDs", async () => {
		const boss = await createBoss(pool, { pollIntervalMs: 20 });
		const id = await boss.enqueue("ac36", { archived: true });
		boss.work("ac36", async () => {});
		const archived = await waitFor("AC 36 archive", () =>
			boss.getArchivedJob(id!),
		);
		expect(archived.id).toBe(id);
		expect(archived.payload).toEqual({ archived: true });
		expect(await boss.getArchivedJob("18446744073709551615")).toBeNull();
		await boss.stop();
	});

	it("AC 37: archive keyset pagination uses its range index without OFFSET", async () => {
		for (let index = 0; index < 10; index++) {
			await pool.query(
				`INSERT INTO jobs_archive
				   (id, queue, priority, retry_count, created_at, started_at,
				    completed_at, duration_ms)
				 VALUES (?, 'ac37', 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6),
				         UTC_TIMESTAMP(6) - INTERVAL ? SECOND, 1)`,
				[3_700 + index, index],
			);
		}
		const boss = await createBoss(pool);
		const first = await boss.listArchive({ queue: "ac37", limit: 5 });
		const second = await boss.listArchive({
			queue: "ac37",
			before: first[4].completedAt,
			limit: 5,
		});
		expect(first).toHaveLength(5);
		expect(second).toHaveLength(5);
		const firstIds = new Set(first.map((job) => job.id));
		expect(second.some((job) => firstIds.has(job.id))).toBe(false);

		const [plan] = await pool.query<RowDataPacket[]>(`
			EXPLAIN
			SELECT id, queue, completed_at
			FROM jobs_archive
			WHERE queue = 'ac37' AND completed_at < UTC_TIMESTAMP(6)
			ORDER BY completed_at DESC
			LIMIT 5
		`);
		expect(plan[0].key).toBe("ix_archive_queue_time");
		expect(String(plan[0].Extra ?? "")).not.toContain("Using filesort");
		expect(String(ARCHIVE_PRUNE)).not.toContain("OFFSET");
		await boss.stop();
	});

	it("AC 38: retention prunes only old rows in batches no larger than 5,000", async () => {
		for (let offset = 0; offset < 5_001; offset += 500) {
			const size = Math.min(500, 5_001 - offset);
			const values = Array.from({ length: size }, (_, index) => [
				38_000 + offset + index,
				"ac38",
				0,
				0,
				"2025-01-01 00:00:00.000",
				"2025-01-01 00:00:00.000",
				"2025-01-01 00:00:00.000",
				1,
			]);
			await pool.query(
				`INSERT INTO jobs_archive
				   (id, queue, priority, retry_count, created_at, started_at,
				    completed_at, duration_ms)
				 VALUES ?`,
				[values],
			);
		}
		for (let index = 0; index < 5; index++) {
			await pool.query(
				`INSERT INTO jobs_archive
				   (id, queue, priority, retry_count, created_at, started_at,
				    completed_at, duration_ms)
				 VALUES (?, 'ac38', 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6),
				         UTC_TIMESTAMP(6), 1)`,
				[50_000 + index],
			);
		}

		expect(ARCHIVE_PRUNE).toContain("LIMIT 5000");
		await pruneArchive(pool, 14);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'ac38'",
			),
		).toBe(5);
		expect(
			await count(
				pool,
				`SELECT COUNT(*) AS count FROM jobs_archive
				 WHERE queue = 'ac38'
				   AND completed_at < UTC_TIMESTAMP(6) - INTERVAL 14 DAY`,
			),
		).toBe(0);
	});
});
