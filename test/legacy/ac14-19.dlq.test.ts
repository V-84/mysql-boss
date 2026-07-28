import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { SingletonCollisionError } from "../../src/errors.js";
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

describe("Legacy coverage: listDead returns dead-lettered jobs", () => {
	it("returns dead jobs for a queue with correct fields", async () => {
		const boss = await createBoss(pool);
		const queue = "dlq-q-14";

		await boss.enqueue(queue, { dead: "payload" }, { retryLimit: 0 });

		boss.work(queue, async () => {
			throw new Error("die now");
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		const dead = await boss.listDead({ queue });

		expect(dead.length).toBe(1);
		expect(dead[0].queue).toBe(queue);
		expect(dead[0].payload).toEqual({ dead: "payload" });
		expect(dead[0].lastError).toBeDefined();
		expect(dead[0].lastError?.message).toBe("die now");
		expect(dead[0].failedAt).toBeInstanceOf(Date);
		expect(dead[0].createdAt).toBeInstanceOf(Date);
	}, 10000);
});

describe("Legacy coverage: listDead pagination", () => {
	it("supports limit and offset for paging through dead jobs", async () => {
		const boss = await createBoss(pool);
		const queue = "dlq-q-15";

		// Enqueue 5 jobs that will all fail immediately
		for (let i = 0; i < 5; i++) {
			await boss.enqueue(queue, { index: i }, { retryLimit: 0 });
		}

		boss.work(queue, async () => {
			throw new Error("fail all");
		});

		await new Promise((r) => setTimeout(r, 3000));
		await boss.stop();

		const page1 = await boss.listDead({ queue, limit: 2, offset: 0 });
		expect(page1.length).toBe(2);

		const page2 = await boss.listDead({ queue, limit: 2, offset: 2 });
		expect(page2.length).toBe(2);

		const page3 = await boss.listDead({ queue, limit: 2, offset: 4 });
		expect(page3.length).toBe(1);

		// All IDs should be distinct
		const allIds = [...page1, ...page2, ...page3].map((j) => j.id);
		expect(new Set(allIds).size).toBe(5);
	}, 10000);
});

describe("Legacy coverage: listDead date filtering", () => {
	it("filters dead jobs by before/after dates", async () => {
		const boss = await createBoss(pool);
		const queue = "dlq-q-16";

		await boss.enqueue(queue, { filtered: true }, { retryLimit: 0 });

		boss.work(queue, async () => {
			throw new Error("filter me");
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		// Should find the job with a wide date range
		const found = await boss.listDead({
			queue,
			after: new Date("2020-01-01"),
			before: new Date("2030-01-01"),
		});
		expect(found.length).toBe(1);

		// Should NOT find with a past-only range
		const notFound = await boss.listDead({
			queue,
			after: new Date("2020-01-01"),
			before: new Date("2020-12-31"),
		});
		expect(notFound.length).toBe(0);
	}, 10000);
});

describe("Legacy coverage: replayDead moves jobs back to available", () => {
	it("replays dead jobs back into the jobs table", async () => {
		const boss = await createBoss(pool);
		const queue = "dlq-q-17";

		await boss.enqueue(queue, { replay: true }, { retryLimit: 0 });

		boss.work(queue, async () => {
			throw new Error("will replay");
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		const dead = await boss.listDead({ queue });
		expect(dead.length).toBe(1);
		const deadId = dead[0].id;

		const replayed = await boss.replayDead([deadId]);
		expect(replayed).toBe(1);

		// Should no longer be in dead
		const deadAfter = await boss.listDead({ queue });
		expect(deadAfter.length).toBe(0);

		// Should be back in jobs as available
		const [jobRows] = await pool.query<RowDataPacket[]>(
			"SELECT state FROM jobs WHERE queue = ?",
			[queue],
		);
		expect(jobRows.length).toBe(1);
		expect(jobRows[0].state).toBe("available");
	}, 10000);
});

describe("Legacy coverage: replayDead with multiple IDs", () => {
	it("replays multiple dead jobs at once", async () => {
		const boss = await createBoss(pool);
		const queue = "dlq-q-18";

		for (let i = 0; i < 3; i++) {
			await boss.enqueue(queue, { batch: i }, { retryLimit: 0 });
		}

		boss.work(queue, async () => {
			throw new Error("batch fail");
		});

		await new Promise((r) => setTimeout(r, 3000));
		await boss.stop();

		const dead = await boss.listDead({ queue });
		expect(dead.length).toBe(3);

		const ids = dead.map((d) => d.id);
		const replayed = await boss.replayDead(ids);
		expect(replayed).toBe(3);

		const deadAfter = await boss.listDead({ queue });
		expect(deadAfter.length).toBe(0);
	}, 10000);
});

describe("Legacy coverage: replayDead throws SingletonCollisionError", () => {
	it("throws when replaying a dead job would collide with a live singleton", async () => {
		const boss = await createBoss(pool);
		const queue = "dlq-q-19";

		// Enqueue a singleton job that will fail
		await boss.enqueue(
			queue,
			{ singleton: true },
			{ retryLimit: 0, singletonKey: "unique-key" },
		);

		boss.work(queue, async () => {
			throw new Error("singleton fail");
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		const dead = await boss.listDead({ queue });
		expect(dead.length).toBe(1);

		// Enqueue another job with the same singleton key (while the dead one exists)
		const liveId = await boss.enqueue(
			queue,
			{ singleton: "live" },
			{ singletonKey: "unique-key" },
		);
		expect(liveId).not.toBeNull();

		// Replaying the dead job should throw because of singleton collision
		await expect(boss.replayDead([dead[0].id])).rejects.toThrow(
			SingletonCollisionError,
		);
	}, 10000);
});
