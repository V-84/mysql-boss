import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { cleanTables, createBoss, createPool } from "./helpers.js";

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
	const boss = await createBoss(pool);
	await boss.stop();
});

afterEach(async () => {
	await cleanTables(pool);
});

afterAll(async () => {
	await pool.end();
});

async function insertDead(
	id: number,
	queue: string,
	payload: unknown,
	failedAt = "2026-07-29 10:00:00.000",
): Promise<void> {
	await pool.query(
		`INSERT INTO jobs_dead
		   (id, queue, priority, payload, retry_count, retry_limit,
		    retry_delay_secs, retry_backoff, created_at, failed_at, last_error)
		 VALUES (?, ?, 7, ?, 2, 3, 30, 0,
		         '2026-07-28 09:00:00.000', ?,
		         JSON_OBJECT('message', 'mapped failure',
		                     'at', '2026-07-29T10:00:00.000Z'))`,
		[id, queue, JSON.stringify(payload), failedAt],
	);
}

describe("public API regression coverage", () => {
	it("unschedule removes an existing schedule", async () => {
		const boss = await createBoss(pool);
		await boss.schedule("remove-me", "scheduled", "* * * * *");

		await boss.unschedule("remove-me");

		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT name FROM schedules WHERE name = 'remove-me'",
		);
		expect(rows).toHaveLength(0);
		await boss.stop();
	});

	it("listDead maps payload, retry metadata, errors, and UTC dates", async () => {
		await insertDead(61_001, "mapped-dead", { invoice: 42 });
		const boss = await createBoss(pool);

		const dead = await boss.listDead({ queue: "mapped-dead" });

		expect(dead).toHaveLength(1);
		expect(dead[0]).toMatchObject({
			id: "61001",
			queue: "mapped-dead",
			payload: { invoice: 42 },
			priority: 7,
			retryCount: 2,
			lastError: {
				message: "mapped failure",
				at: "2026-07-29T10:00:00.000Z",
			},
		});
		expect(dead[0].createdAt.toISOString()).toBe("2026-07-28T09:00:00.000Z");
		expect(dead[0].failedAt.toISOString()).toBe("2026-07-29T10:00:00.000Z");
		await boss.stop();
	});

	it("replayDead moves multiple dead jobs in one atomic batch", async () => {
		for (let index = 0; index < 3; index++) {
			await insertDead(62_001 + index, "batch-replay", { index });
		}
		const boss = await createBoss(pool);

		expect(await boss.replayDead(["62001", "62002", "62003"])).toBe(3);

		const [rows] = await pool.query<RowDataPacket[]>(
			`SELECT
			   (SELECT COUNT(*) FROM jobs
			    WHERE queue = 'batch-replay' AND state = 'available') AS hot_count,
			   (SELECT COUNT(*) FROM jobs_dead
			    WHERE queue = 'batch-replay') AS dead_count`,
		);
		expect(rows[0]).toMatchObject({ hot_count: 3, dead_count: 0 });
		await boss.stop();
	});

	it("enqueue persists the documented default priority of zero", async () => {
		const boss = await createBoss(pool);
		const id = await boss.enqueue("default-priority", {});

		const [rows] = await pool.query<RowDataPacket[]>(
			"SELECT priority FROM jobs WHERE id = ?",
			[id],
		);
		expect(rows[0].priority).toBe(0);
		await boss.stop();
	});
});
