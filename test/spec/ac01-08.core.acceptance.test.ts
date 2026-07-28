import { randomUUID } from "node:crypto";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { completeJob } from "../../src/archive.js";
import { claimJobs } from "../../src/claim.js";
import { MysqlBoss } from "../../src/index.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

async function waitFor(
	description: string,
	predicate: () => Promise<boolean> | boolean,
	timeoutMs = 30_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
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
	await pool.query("DROP TABLE IF EXISTS acceptance_side_effects");
});

afterAll(async () => {
	await pool.query("DROP TABLE IF EXISTS acceptance_side_effects");
	await pool.end();
});

describe("Implementation spec acceptance criteria 1-8", () => {
	it("AC 1: migrate twice produces no error and no schema diff", async () => {
		const boss = new MysqlBoss({ pool, tablePrefix: "" });
		await boss.migrate();
		const tables = ["jobs", "jobs_archive", "jobs_dead", "schedules"];
		const before: Record<string, string> = {};
		for (const table of tables) {
			const [rows] = await pool.query<RowDataPacket[]>(
				`SHOW CREATE TABLE ${table}`,
			);
			before[table] = rows[0]["Create Table"];
		}

		await boss.migrate();
		for (const table of tables) {
			const [rows] = await pool.query<RowDataPacket[]>(
				`SHOW CREATE TABLE ${table}`,
			);
			expect(rows[0]["Create Table"]).toBe(before[table]);
		}
	});

	it("AC 2: a future job remains unavailable until the DB clock passes runAt", async () => {
		const boss = await createBoss(pool, { pollIntervalMs: 20 });
		const runAt = new Date(Date.now() + 600);
		let handledAt = 0;
		await boss.enqueue("ac2", {}, { runAt });
		boss.work("ac2", async () => {
			handledAt = Date.now();
		});

		await new Promise((resolve) => setTimeout(resolve, 200));
		expect(handledAt).toBe(0);
		await waitFor("future job", () => handledAt > 0, 5_000);
		expect(handledAt).toBeGreaterThanOrEqual(runAt.getTime() - 25);
		await boss.stop();
	});

	it("AC 3: four workers execute 10,000 jobs exactly once", async () => {
		await pool.query(`
			CREATE TABLE acceptance_side_effects (
				job_id BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (job_id)
			) ENGINE=InnoDB
		`);

		const total = 10_000;
		const dueAt = new Date().toISOString().slice(0, 23).replace("T", " ");
		for (let offset = 0; offset < total; offset += 500) {
			const values = Array.from(
				{ length: Math.min(500, total - offset) },
				(_, index) => ["ac3", JSON.stringify({ index: offset + index })],
			);
			await pool.query("INSERT INTO jobs (queue, payload, run_at) VALUES ?", [
				values.map(([queue, payload]) => [queue, payload, dueAt]),
			]);
		}

		const workers: Array<{ boss: MysqlBoss; pool: Pool }> = [];
		try {
			for (let index = 0; index < 4; index++) {
				const workerPool = await createPool();
				const boss = new MysqlBoss({
					pool: workerPool,
					tablePrefix: "",
					pollIntervalMs: 10,
					batchSize: 100,
					concurrency: 100,
					leaseSeconds: 30,
					heartbeatSeconds: 10,
					sweepIntervalMs: 5_000,
				});
				boss.work("ac3", async (job) => {
					await pool.query(
						"INSERT INTO acceptance_side_effects (job_id) VALUES (?)",
						[job.id],
					);
				});
				workers.push({ boss, pool: workerPool });
			}

			await waitFor(
				"10,000 archived jobs",
				async () =>
					(await count(
						pool,
						"SELECT COUNT(*) AS count FROM jobs_archive WHERE queue = 'ac3'",
					)) === total,
				120_000,
			);
			expect(
				await count(
					pool,
					"SELECT COUNT(*) AS count FROM acceptance_side_effects",
				),
			).toBe(total);
			expect(
				await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac3'",
				),
			).toBe(0);
			expect(
				await count(
					pool,
					"SELECT COUNT(*) AS count FROM jobs_dead WHERE queue = 'ac3'",
				),
			).toBe(0);
		} finally {
			for (const worker of workers) {
				await worker.boss.stop();
				await worker.pool.end();
			}
		}
	}, 180_000);

	it("AC 4: claim respects batchSize and establishes ownership and lease", async () => {
		const boss = await createBoss(pool);
		for (let index = 0; index < 6; index++) {
			await boss.enqueue("ac4", { index });
		}
		const workerId = randomUUID();
		const jobs = await claimJobs(pool, "ac4", workerId, 3, 30);
		expect(jobs).toHaveLength(3);

		const [rows] = await pool.query<RowDataPacket[]>(
			`SELECT state, BIN_TO_UUID(locked_by) AS owner,
			        TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), lease_expires_at) AS lease_seconds
			 FROM jobs WHERE id IN (?)`,
			[jobs.map((job) => job.id)],
		);
		expect(rows).toHaveLength(3);
		for (const row of rows) {
			expect(row.state).toBe("active");
			expect(row.owner).toBe(workerId);
			expect(Number(row.lease_seconds)).toBeGreaterThanOrEqual(28);
			expect(Number(row.lease_seconds)).toBeLessThanOrEqual(30);
		}
		await boss.stop();
	});

	it("AC 5: two concurrent claims neither block nor overlap", async () => {
		const boss = await createBoss(pool);
		for (let index = 0; index < 20; index++) {
			await boss.enqueue("ac5", { index });
		}
		const secondPool = await createPool();
		try {
			const startedAt = Date.now();
			const [first, second] = await Promise.all([
				claimJobs(pool, "ac5", randomUUID(), 10, 30),
				claimJobs(secondPool, "ac5", randomUUID(), 10, 30),
			]);
			expect(Date.now() - startedAt).toBeLessThan(2_000);
			expect(first.length).toBeLessThanOrEqual(10);
			expect(second.length).toBeLessThanOrEqual(10);
			expect(first.length + second.length).toBeGreaterThan(0);
			const firstIds = new Set(first.map((job) => job.id));
			expect(second.some((job) => firstIds.has(job.id))).toBe(false);
		} finally {
			await secondPool.end();
			await boss.stop();
		}
	});

	it("AC 6: singleton deduplicates within a queue but not across queues", async () => {
		const boss = await createBoss(pool);
		const first = await boss.enqueue("ac6-a", {}, { singletonKey: "same" });
		const duplicate = await boss.enqueue("ac6-a", {}, { singletonKey: "same" });
		const otherQueue = await boss.enqueue(
			"ac6-b",
			{},
			{ singletonKey: "same" },
		);
		expect(first).not.toBeNull();
		expect(duplicate).toBeNull();
		expect(otherQueue).not.toBeNull();
		await boss.stop();
	});

	it("AC 7: a fenced completion affects no row and archives nothing", async () => {
		const boss = await createBoss(pool);
		const jobId = await boss.enqueue("ac7", {});
		const firstWorker = randomUUID();
		const secondWorker = randomUUID();
		await claimJobs(pool, "ac7", firstWorker, 1, 30);
		await pool.query(
			"UPDATE jobs SET locked_by = UUID_TO_BIN(?) WHERE id = ?",
			[secondWorker, jobId],
		);

		expect(await completeJob(pool, jobId!, firstWorker)).toBe(false);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs_archive WHERE id = ?",
				[jobId],
			),
		).toBe(0);
		await boss.stop();
	});

	it("AC 8: every reused and newly created library connection is READ COMMITTED and UTC", async () => {
		const isolatedPool = await createPool();
		const connections = await Promise.all([
			isolatedPool.getConnection(),
			isolatedPool.getConnection(),
			isolatedPool.getConnection(),
		]);
		for (const connection of connections) {
			await connection.query(
				"SET SESSION transaction_isolation = 'REPEATABLE-READ', SESSION time_zone = 'SYSTEM'",
			);
			connection.release();
		}

		const boss = new MysqlBoss({ pool: isolatedPool, tablePrefix: "" });
		await boss.migrate();
		await Promise.all([
			boss.enqueue("ac8", { index: 1 }),
			boss.enqueue("ac8", { index: 2 }),
			boss.enqueue("ac8", { index: 3 }),
		]);

		const inspected = await Promise.all([
			isolatedPool.getConnection(),
			isolatedPool.getConnection(),
			isolatedPool.getConnection(),
		]);
		try {
			for (const connection of inspected) {
				const [rows] = await connection.query<RowDataPacket[]>(
					"SELECT @@SESSION.transaction_isolation AS isolation_level, @@SESSION.time_zone AS time_zone",
				);
				expect(rows[0].isolation_level).toBe("READ-COMMITTED");
				expect(rows[0].time_zone).toBe("+00:00");
			}
		} finally {
			for (const connection of inspected) connection.release();
			await boss.stop();
			await isolatedPool.end();
		}
	});
});
