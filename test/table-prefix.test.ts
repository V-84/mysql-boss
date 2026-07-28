import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ConfigError, MysqlBoss } from "../src/index.js";
import { createPool } from "./helpers.js";

async function waitFor<T>(
	description: string,
	predicate: () => Promise<T | null | false>,
	timeoutMs = 10_000,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const value = await predicate();
		if (value) return value;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(`Timed out waiting for ${description}`);
}

const DEFAULT_TABLES = [
	"mysql_boss_jobs",
	"mysql_boss_jobs_archive",
	"mysql_boss_jobs_dead",
	"mysql_boss_schedules",
];

const CUSTOM_TABLES = [
	"tenant_queue_jobs",
	"tenant_queue_jobs_archive",
	"tenant_queue_jobs_dead",
	"tenant_queue_schedules",
];

async function dropTables(pool: Pool, tables: string[]): Promise<void> {
	for (const table of tables) {
		await pool.query(`DROP TABLE IF EXISTS \`${table}\``);
	}
}

async function existingTables(pool: Pool, names: string[]): Promise<string[]> {
	const [rows] = await pool.query<RowDataPacket[]>(
		`SELECT TABLE_NAME
		 FROM INFORMATION_SCHEMA.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (?)
		 ORDER BY TABLE_NAME`,
		[names],
	);
	return rows.map((row) => row.TABLE_NAME);
}

describe("table prefix", () => {
	let pool: Pool;

	beforeAll(async () => {
		pool = await createPool();
		await dropTables(pool, [...DEFAULT_TABLES, ...CUSTOM_TABLES]);
	});

	afterAll(async () => {
		await dropTables(pool, [...DEFAULT_TABLES, ...CUSTOM_TABLES]);
		await pool.end();
	});

	it("uses mysql_boss for all table names by default", async () => {
		const boss = new MysqlBoss({ pool });
		await boss.migrate();

		expect(await existingTables(pool, DEFAULT_TABLES)).toEqual(
			[...DEFAULT_TABLES].sort(),
		);
	});

	it("routes queue, archive, DLQ, replay, and schedules through a custom prefix", async () => {
		const boss = new MysqlBoss({
			pool,
			tablePrefix: "tenant_queue",
			pollIntervalMs: 20,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
		});
		await boss.migrate();
		await boss.schedule("nightly", "scheduled", "0 0 * * *");

		const successId = await boss.enqueue("success", { ok: true });
		const deadId = await boss.enqueue(
			"failure",
			{ ok: false },
			{ retryLimit: 0 },
		);
		boss.work("success", async () => {});
		boss.work("failure", async () => {
			throw new Error("expected failure");
		});

		await waitFor("prefixed archive row", () =>
			boss.getArchivedJob(successId!),
		);
		await waitFor("prefixed dead row", async () => {
			const rows = await boss.listDead({ queue: "failure" });
			return rows[0] ?? null;
		});
		await boss.stop();

		expect(await boss.replayDead([deadId!])).toBe(1);
		const [rows] = await pool.query<RowDataPacket[]>(
			`SELECT
			   (SELECT COUNT(*) FROM tenant_queue_jobs WHERE queue = 'failure') AS hot_count,
			   (SELECT COUNT(*) FROM tenant_queue_jobs_archive WHERE id = ?) AS archive_count,
			   (SELECT COUNT(*) FROM tenant_queue_jobs_dead WHERE id = ?) AS dead_count,
			   (SELECT COUNT(*) FROM tenant_queue_schedules WHERE name = 'nightly') AS schedule_count`,
			[successId, deadId],
		);
		expect(rows[0]).toMatchObject({
			hot_count: 1,
			archive_count: 1,
			dead_count: 0,
			schedule_count: 1,
		});
		expect(await existingTables(pool, CUSTOM_TABLES)).toEqual(
			[...CUSTOM_TABLES].sort(),
		);
	});

	it("rejects unsafe or overlong prefixes before migration", () => {
		expect(() => new MysqlBoss({ pool, tablePrefix: "bad-prefix" })).toThrow(
			ConfigError,
		);
		expect(() => new MysqlBoss({ pool, tablePrefix: "x".repeat(51) })).toThrow(
			ConfigError,
		);
	});
});
