import type { Pool, RowDataPacket } from "mysql2/promise";
import {
	afterAll,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
	vi,
} from "vitest";
import { nextOccurrence } from "../../src/cron/next.js";
import { parseCron } from "../../src/cron/parse.js";
import { ValidationError } from "../../src/errors.js";
import { MysqlBoss } from "../../src/index.js";
import { runTick } from "../../src/tick.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

async function waitFor(
	description: string,
	predicate: () => Promise<boolean> | boolean,
	timeoutMs = 10_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(`Timed out waiting for ${description}`);
}

async function count(pool: Pool, sql: string): Promise<number> {
	const [rows] = await pool.query<RowDataPacket[]>(sql);
	return Number(rows[0].count);
}

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
	await new MysqlBoss({ pool, tablePrefix: "" }).migrate();
});

beforeEach(async () => {
	vi.useRealTimers();
	await cleanTables(pool);
});

afterAll(async () => {
	vi.useRealTimers();
	await pool.end();
});

describe("Implementation spec acceptance criteria 20-28", () => {
	it("AC 20: cron oracle fixtures include Vixie semantics and London DST transitions", () => {
		const fixtures = [
			{
				expression: "*/15 9-17 * * 1-5",
				timezone: "UTC",
				after: "2026-07-27T09:01:00Z",
				expected: "2026-07-27T09:15:00.000Z",
			},
			{
				expression: "0 9 1 * 1",
				timezone: "UTC",
				after: "2026-08-02T00:00:00Z",
				expected: "2026-08-03T09:00:00.000Z",
			},
			{
				expression: "30 1 * * *",
				timezone: "Europe/London",
				after: "2026-03-29T00:00:00Z",
				expected: "2026-03-30T00:30:00.000Z",
			},
		];
		for (const fixture of fixtures) {
			expect(
				nextOccurrence(
					parseCron(fixture.expression),
					new Date(fixture.after),
					fixture.timezone,
				).toISOString(),
			).toBe(fixture.expected);
		}

		const fallBackFields = parseCron("30 1 * * *");
		const first = nextOccurrence(
			fallBackFields,
			new Date("2026-10-25T00:00:00Z"),
			"Europe/London",
		);
		expect(first.toISOString()).toBe("2026-10-25T00:30:00.000Z");
		const second = nextOccurrence(fallBackFields, first, "Europe/London");
		expect(second.toISOString()).toBe("2026-10-26T01:30:00.000Z");
	});

	it("AC 21: schedule upserts an existing name in place", async () => {
		const boss = await createBoss(pool);
		await boss.schedule("ac21", "old-queue", "0 * * * *", {
			payload: { version: 1 },
		});
		await boss.schedule("ac21", "new-queue", "30 * * * *", {
			payload: { version: 2 },
		});
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT name, queue, cron, payload FROM schedules WHERE name = 'ac21'",
		);
		expect(rows).toHaveLength(1);
		expect(rows[0].queue).toBe("new-queue");
		expect(rows[0].cron).toBe("30 * * * *");
		expect(rows[0].payload).toEqual({ version: 2 });
		await boss.stop();
	});

	it("AC 22: concurrent tickers fire a due occurrence exactly once", async () => {
		const boss = await createBoss(pool);
		await boss.schedule("ac22", "ac22-queue", "* * * * *");
		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE WHERE name = 'ac22'",
		);

		await Promise.all(Array.from({ length: 12 }, () => runTick(pool)));
		await Promise.all(Array.from({ length: 12 }, () => runTick(pool)));
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac22-queue'",
			),
		).toBe(1);
		await boss.stop();
	});

	it("AC 23: downtime spanning multiple occurrences creates one catch-up job", async () => {
		const boss = await createBoss(pool);
		await boss.schedule("ac23", "ac23-queue", "* * * * *");
		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 10 MINUTE WHERE name = 'ac23'",
		);
		expect(await runTick(pool)).toBe(1);
		expect(
			await count(
				pool,
				"SELECT COUNT(*) AS count FROM jobs WHERE queue = 'ac23-queue'",
			),
		).toBe(1);
		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), next_run_at) AS seconds_until FROM schedules WHERE name = 'ac23'",
		);
		expect(Number(rows[0].seconds_until)).toBeGreaterThanOrEqual(0);
		expect(Number(rows[0].seconds_until)).toBeLessThanOrEqual(60);
		await boss.stop();
	});

	it("AC 24: next_run_at never decreases under a backward-skewed worker clock", async () => {
		const boss = await createBoss(pool);
		await boss.schedule("ac24", "ac24-queue", "* * * * *");
		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 5 MINUTE WHERE name = 'ac24'",
		);
		const [beforeRows] = await pool.query<RowDataPacket[]>(
			"SELECT UNIX_TIMESTAMP(next_run_at) AS next_run_unix FROM schedules WHERE name = 'ac24'",
		);
		const before = Number(beforeRows[0].next_run_unix);

		vi.useFakeTimers({ toFake: ["Date"] });
		vi.setSystemTime(new Date("2000-01-01T00:00:00Z"));
		await runTick(pool);
		vi.useRealTimers();

		const [afterRows] = await pool.query<RowDataPacket[]>(
			`SELECT UNIX_TIMESTAMP(next_run_at) AS next_run_unix,
			        UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) AS db_now_unix
			 FROM schedules WHERE name = 'ac24'`,
		);
		expect(Number(afterRows[0].next_run_unix)).toBeGreaterThan(before);
		expect(Number(afterRows[0].next_run_unix)).toBeGreaterThan(
			Number(afterRows[0].db_now_unix),
		);
		await boss.stop();
	});

	it("AC 25: caller singleton keys beginning with cron: are rejected", async () => {
		const boss = await createBoss(pool);
		await expect(
			boss.enqueue("ac25", {}, { singletonKey: "cron:reserved" }),
		).rejects.toBeInstanceOf(ValidationError);
		await boss.stop();
	});

	it("AC 26: one worker drains by priority DESC, run_at, then id", async () => {
		const boss = await createBoss(pool, {
			pollIntervalMs: 20,
			batchSize: 1,
			concurrency: 1,
		});
		const commonRunAt = new Date(Date.now() - 1_000);
		await boss.enqueue(
			"ac26",
			{ label: "first-low" },
			{
				priority: 0,
				runAt: commonRunAt,
			},
		);
		await boss.enqueue(
			"ac26",
			{ label: "second-low" },
			{
				priority: 0,
				runAt: commonRunAt,
			},
		);
		await boss.enqueue(
			"ac26",
			{ label: "high" },
			{
				priority: 10,
				runAt: commonRunAt,
			},
		);
		const order: string[] = [];
		boss.work("ac26", async (job) => {
			order.push((job.payload as { label: string }).label);
		});
		await waitFor("priority drain", () => order.length === 3);
		expect(order).toEqual(["high", "first-low", "second-low"]);
		await boss.stop();
	});

	it("AC 27: dequeue EXPLAIN uses ix_jobs_dequeue without filesort", async () => {
		const [rows] = await pool.query<RowDataPacket[]>(`
			EXPLAIN
			SELECT id, queue, payload, retry_count, retry_limit,
			       retry_delay_secs, retry_backoff
			FROM jobs
			WHERE queue = 'ac27'
			  AND state = 'available'
			  AND run_at <= UTC_TIMESTAMP(6)
			ORDER BY priority DESC, run_at, id
			LIMIT 10
			FOR UPDATE SKIP LOCKED
		`);
		expect(rows[0].key).toBe("ix_jobs_dequeue");
		expect(String(rows[0].Extra ?? "")).not.toContain("Using filesort");
	});

	it("AC 28: priorities outside the SMALLINT range are rejected", async () => {
		const boss = await createBoss(pool);
		await expect(
			boss.enqueue("ac28", {}, { priority: 32_768 }),
		).rejects.toBeInstanceOf(ValidationError);
		await expect(
			boss.enqueue("ac28", {}, { priority: -32_769 }),
		).rejects.toBeInstanceOf(ValidationError);
		expect(await boss.enqueue("ac28", {}, { priority: 32_767 })).not.toBeNull();
		expect(
			await boss.enqueue("ac28", {}, { priority: -32_768 }),
		).not.toBeNull();
		await boss.stop();
	});
});
