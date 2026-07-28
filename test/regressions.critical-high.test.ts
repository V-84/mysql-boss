import mysql from "mysql2/promise";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MysqlBoss } from "../src/index.js";
import { cleanTables, createBoss, createPool } from "./helpers.js";

const DB_CONFIG = {
	host: process.env.MYSQL_HOST ?? "127.0.0.1",
	port: Number(process.env.MYSQL_PORT ?? 3307),
	user: process.env.MYSQL_USER ?? "root",
	password: process.env.MYSQL_PASSWORD ?? "test",
	database: process.env.MYSQL_DATABASE ?? "testdb",
};

const pools = new Set<Pool>();

async function trackedPool(
	overrides: Parameters<typeof mysql.createPool>[0] = {},
): Promise<Pool> {
	const pool = mysql.createPool({
		...DB_CONFIG,
		waitForConnections: true,
		connectionLimit: 10,
		...overrides,
	});
	pools.add(pool);
	return pool;
}

async function waitFor<T>(
	description: string,
	predicate: () => Promise<T | null | false> | T | null | false,
	timeoutMs = 10_000,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const result = await predicate();
		if (result) return result;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(`Timed out waiting for ${description}`);
}

afterEach(async () => {
	vi.useRealTimers();
	for (const pool of pools) {
		await pool.end();
	}
	pools.clear();
});

describe("critical/high regression coverage", () => {
	it("initializes a physical connection that existed before MysqlBoss", async () => {
		const pool = await trackedPool({ connectionLimit: 1 });
		await pool.query(
			"SET SESSION transaction_isolation = 'REPEATABLE-READ', SESSION time_zone = 'SYSTEM'",
		);

		const boss = new MysqlBoss({ pool, tablePrefix: "" });
		await boss.migrate();
		await boss.enqueue("preused-connection", {});

		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT @@SESSION.transaction_isolation AS isolation_level, @@SESSION.time_zone AS time_zone",
		);
		expect(rows[0].isolation_level).toBe("READ-COMMITTED");
		expect(rows[0].time_zone).toBe("+00:00");
		expect(pool.listenerCount("connection")).toBe(0);

		await boss.stop();
	});

	it("claims and archives IDs above Number.MAX_SAFE_INTEGER exactly", async () => {
		const pool = await trackedPool();
		const boss = await createBoss(pool, {
			pollIntervalMs: 25,
			batchSize: 1,
			concurrency: 1,
		});
		await cleanTables(pool);
		await pool.query("ALTER TABLE jobs AUTO_INCREMENT = 9007199254740993");

		const expectedId = "9007199254740993";
		const enqueuedId = await boss.enqueue("large-id", { exact: true });
		expect(enqueuedId).toBe(expectedId);

		let handledId: string | null = null;
		let calls = 0;
		boss.work("large-id", async (job) => {
			calls++;
			handledId = job.id;
		});

		const archived = await waitFor("large ID archive", () =>
			boss.getArchivedJob(expectedId),
		);
		expect(handledId).toBe(expectedId);
		expect(archived.id).toBe(expectedId);
		expect(calls).toBe(1);

		await boss.stop();
		await pool.query("TRUNCATE TABLE jobs");
		await pool.query("TRUNCATE TABLE jobs_archive");
	});

	it("does not convert completion infrastructure errors into handler retries", async () => {
		const pool = await trackedPool();
		const reported: Array<{ context: string; error: unknown }> = [];
		const boss = await createBoss(pool, {
			pollIntervalMs: 25,
			sweepIntervalMs: 60_000,
			onError(error, context) {
				reported.push({ context, error });
			},
		});
		await cleanTables(pool);

		const jobId = await boss.enqueue("completion-error", {}, { retryLimit: 2 });
		boss.work("completion-error", async () => {
			await pool.query("DROP TABLE jobs_archive");
		});

		await waitFor("completion error report", () =>
			reported.some((entry) => entry.context === "complete"),
		);
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT state, retry_count, last_error FROM jobs WHERE id = ?",
			[jobId],
		);
		expect(rows[0].state).toBe("active");
		expect(rows[0].retry_count).toBe(0);
		expect(rows[0].last_error).toBeNull();

		await boss.stop();
		await boss.migrate();
		await cleanTables(pool);
	});

	it("aborts only the handler whose lease was lost", async () => {
		const pool = await trackedPool({
			supportBigNumbers: true,
			bigNumberStrings: true,
		});
		const boss = await createBoss(pool, {
			pollIntervalMs: 25,
			batchSize: 2,
			concurrency: 2,
			leaseSeconds: 6,
			heartbeatSeconds: 1,
			sweepIntervalMs: 60_000,
		});
		await cleanTables(pool);

		await boss.enqueue("selective-heartbeat", { n: 1 });
		await boss.enqueue("selective-heartbeat", { n: 2 });

		const signals = new Map<string, AbortSignal>();
		const aborted = new Set<string>();
		boss.work("selective-heartbeat", async (job, { signal }) => {
			signals.set(job.id, signal);
			await new Promise<void>((resolve) => {
				const timer = setTimeout(resolve, 3_000);
				signal.addEventListener(
					"abort",
					() => {
						clearTimeout(timer);
						aborted.add(job.id);
						resolve();
					},
					{ once: true },
				);
			});
		});

		await waitFor("both heartbeat handlers", () => signals.size === 2);
		const [active] = await pool.query<RowDataPacket[]>(
			"SELECT CAST(id AS CHAR) AS id FROM jobs WHERE queue = 'selective-heartbeat' ORDER BY id",
		);
		const lostId = active[0].id as string;
		const healthyId = active[1].id as string;
		await pool.query(
			"UPDATE jobs SET locked_by = UUID_TO_BIN('22222222-2222-2222-2222-222222222222') WHERE id = ?",
			[lostId],
		);

		await waitFor("lost lease abort", () => aborted.has(lostId));
		expect(aborted.has(healthyId)).toBe(false);
		expect(signals.get(healthyId)?.aborted).toBe(false);

		await boss.stop({ drainTimeoutMs: 4_000 });
		await cleanTables(pool);
	});

	it("returns UTC archive and DLQ dates regardless of mysql2 client timezone", async () => {
		const pool = await trackedPool({ timezone: "+01:00" });
		const boss = await createBoss(pool, { pollIntervalMs: 25 });
		await cleanTables(pool);

		const jobId = await boss.enqueue("utc-date", {});
		boss.work("utc-date", async () => {});
		const archived = await waitFor("UTC date archive", () =>
			boss.getArchivedJob(jobId!),
		);
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT UNIX_TIMESTAMP(completed_at) AS completed_at_unix FROM jobs_archive WHERE id = ?",
			[jobId],
		);
		const expectedMilliseconds = Number(rows[0].completed_at_unix) * 1000;
		expect(
			Math.abs(archived.completedAt.getTime() - expectedMilliseconds),
		).toBeLessThan(1);

		const deadId = await boss.enqueue("utc-dead-date", {}, { retryLimit: 0 });
		boss.work("utc-dead-date", async () => {
			throw new Error("expected");
		});
		const dead = await waitFor("UTC dead date", async () => {
			const jobs = await boss.listDead({ queue: "utc-dead-date" });
			return jobs[0] ?? null;
		});
		expect(dead.id).toBe(deadId);
		const [deadRows] = await pool.query<RowDataPacket[]>(
			"SELECT UNIX_TIMESTAMP(failed_at) AS failed_at_unix FROM jobs_dead WHERE id = ?",
			[deadId],
		);
		const expectedFailedMilliseconds =
			Number(deadRows[0].failed_at_unix) * 1000;
		expect(
			Math.abs(dead.failedAt.getTime() - expectedFailedMilliseconds),
		).toBeLessThan(1);

		await boss.stop();
	});

	it("uses the database clock for schedules and preserves falsy payloads", async () => {
		const pool = await trackedPool();
		const boss = await createBoss(pool, {
			pollIntervalMs: 25,
			tickIntervalMs: 100,
		});
		await cleanTables(pool);

		vi.useFakeTimers({ toFake: ["Date"] });
		vi.setSystemTime(new Date("2000-01-01T00:00:00Z"));
		await boss.schedule("db-clock", "falsy-cron", "* * * * *", {
			payload: false,
		});
		vi.useRealTimers();

		const [scheduleRows] = await pool.query<RowDataPacket[]>(
			"SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), next_run_at) AS seconds_until FROM schedules WHERE name = 'db-clock'",
		);
		expect(scheduleRows[0].seconds_until).toBeGreaterThanOrEqual(0);
		expect(scheduleRows[0].seconds_until).toBeLessThanOrEqual(60);

		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE WHERE name = 'db-clock'",
		);
		let payload: unknown = "not-run";
		boss.work("falsy-cron", async (job) => {
			payload = job.payload;
		});
		await waitFor("falsy scheduled payload", () => payload !== "not-run");
		expect(payload).toBe(false);

		await boss.stop();
	});
});
