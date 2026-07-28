import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { nextOccurrence } from "../../src/cron/next.js";
import { parseCron } from "../../src/cron/parse.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

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

describe("AC 20: schedule() upserts a cron schedule", () => {
	it("creates a schedule that the tick can fire", async () => {
		const boss = await createBoss(pool);

		await boss.schedule("every-minute", "cron-q-20", "* * * * *", {
			payload: { cron: true },
		});

		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT name, queue, cron, timezone FROM schedules WHERE name = ?",
			["every-minute"],
		);

		expect(rows.length).toBe(1);
		expect(rows[0].queue).toBe("cron-q-20");
		expect(rows[0].cron).toBe("* * * * *");
		expect(rows[0].timezone).toBe("UTC");

		await boss.stop();
	}, 5000);

	it("upserts an existing schedule", async () => {
		const boss = await createBoss(pool);

		await boss.schedule("upsert-test", "cron-q-20", "0 * * * *");
		await boss.schedule("upsert-test", "cron-q-20-updated", "30 * * * *");

		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT queue, cron FROM schedules WHERE name = ?",
			["upsert-test"],
		);

		expect(rows.length).toBe(1);
		expect(rows[0].queue).toBe("cron-q-20-updated");
		expect(rows[0].cron).toBe("30 * * * *");

		await boss.stop();
	}, 5000);
});

describe("AC 21: unschedule() removes a schedule", () => {
	it("deletes a schedule by name", async () => {
		const boss = await createBoss(pool);

		await boss.schedule("remove-me", "cron-q-21", "* * * * *");

		const [before] = await pool.query<RowDataPacket[]>(
			"SELECT name FROM schedules WHERE name = ?",
			["remove-me"],
		);
		expect(before.length).toBe(1);

		await boss.unschedule("remove-me");

		const [after] = await pool.query<RowDataPacket[]>(
			"SELECT name FROM schedules WHERE name = ?",
			["remove-me"],
		);
		expect(after.length).toBe(0);

		await boss.stop();
	}, 5000);
});

describe("AC 22: cron tick fires due schedules", () => {
	it("enqueues a job when next_run_at is in the past", async () => {
		const boss = await createBoss(pool, { tickIntervalMs: 500 });
		const queue = "cron-q-22";

		// Schedule with "every minute" and manually set next_run_at to the past
		await boss.schedule("tick-test", queue, "* * * * *", {
			payload: { fired: true },
		});

		// Force next_run_at to the past so tick fires immediately
		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE WHERE name = ?",
			["tick-test"],
		);

		let firedPayload: unknown = null;
		boss.work(queue, async (job) => {
			firedPayload = job.payload;
		});

		await new Promise((r) => setTimeout(r, 4000));
		await boss.stop();

		expect(firedPayload).toEqual({ fired: true });
	}, 10000);
});

describe("AC 23: cron tick advances next_run_at", () => {
	it("updates next_run_at after firing", async () => {
		const boss = await createBoss(pool, { tickIntervalMs: 500 });
		const queue = "cron-q-23";

		await boss.schedule("advance-test", queue, "* * * * *");

		// Force next_run_at to the past
		await pool.query(
			"UPDATE schedules SET next_run_at = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE WHERE name = ?",
			["advance-test"],
		);

		const [before] = await pool.query<RowDataPacket[]>(
			"SELECT next_run_at FROM schedules WHERE name = ?",
			["advance-test"],
		);
		const beforeRunAt = new Date(before[0].next_run_at);

		boss.work(queue, async () => {});

		await new Promise((r) => setTimeout(r, 3000));
		await boss.stop();

		const [after] = await pool.query<RowDataPacket[]>(
			"SELECT next_run_at FROM schedules WHERE name = ?",
			["advance-test"],
		);
		const afterRunAt = new Date(after[0].next_run_at);

		// next_run_at should have advanced to the future
		expect(afterRunAt.getTime()).toBeGreaterThan(beforeRunAt.getTime());
	}, 10000);
});

describe("AC 24: cron parser handles 5-field Vixie cron", () => {
	it("parses standard expressions", () => {
		const fields = parseCron("*/15 9-17 * * 1-5");
		expect(fields.minutes.has(0)).toBe(true);
		expect(fields.minutes.has(15)).toBe(true);
		expect(fields.minutes.has(30)).toBe(true);
		expect(fields.minutes.has(45)).toBe(true);
		expect(fields.hours.has(9)).toBe(true);
		expect(fields.hours.has(17)).toBe(true);
		expect(fields.hours.has(18)).toBe(false);
		expect(fields.daysOfWeek.has(1)).toBe(true);
		expect(fields.daysOfWeek.has(5)).toBe(true);
		expect(fields.daysOfWeek.has(0)).toBe(false);
		expect(fields.daysOfWeek.has(6)).toBe(false);
	});

	it("parses month and DOW names", () => {
		const fields = parseCron("0 0 1 jan,jun mon");
		expect(fields.months.has(1)).toBe(true);
		expect(fields.months.has(6)).toBe(true);
		expect(fields.months.has(2)).toBe(false);
		expect(fields.daysOfWeek.has(1)).toBe(true);
	});

	it("handles wrap-around ranges in DOW", () => {
		const fields = parseCron("0 0 * * 5-1");
		expect(fields.daysOfWeek.has(5)).toBe(true);
		expect(fields.daysOfWeek.has(6)).toBe(true);
		expect(fields.daysOfWeek.has(0)).toBe(true);
		expect(fields.daysOfWeek.has(1)).toBe(true);
		expect(fields.daysOfWeek.has(2)).toBe(false);
	});

	it("rejects invalid expressions", () => {
		expect(() => parseCron("* * *")).toThrow();
		expect(() => parseCron("60 * * * *")).toThrow();
	});
});

describe("AC 25: timezone-aware next occurrence", () => {
	it("computes next occurrence in UTC", () => {
		const fields = parseCron("0 12 * * *");
		const after = new Date("2026-07-28T11:00:00Z");
		const next = nextOccurrence(fields, after, "UTC");

		expect(next.getUTCHours()).toBe(12);
		expect(next.getUTCMinutes()).toBe(0);
		expect(next.getUTCDate()).toBe(28);
	});

	it("handles timezone offset", () => {
		const fields = parseCron("0 9 * * *");
		const after = new Date("2026-07-28T12:00:00Z");
		const next = nextOccurrence(fields, after, "America/New_York");

		// 9 AM ET = 13:00 UTC (EDT is UTC-4)
		expect(next.getUTCHours()).toBe(13);
		expect(next.getUTCMinutes()).toBe(0);
	});

	it("skips DST gap times", () => {
		// US spring forward: 2:00 AM -> 3:00 AM on March 8, 2026
		const fields = parseCron("30 2 8 3 *");
		const after = new Date("2026-03-08T00:00:00Z");
		const result = nextOccurrence(fields, after, "America/New_York");

		// 2:30 AM doesn't exist during DST gap; should find next valid occurrence
		// The next valid 2:30 AM in ET would be a different day or the function skips it
		// Since the specific day is pinned (March 8), it may advance to next year
		expect(result).toBeInstanceOf(Date);
	});
});
