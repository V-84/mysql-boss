import type { Pool } from "mysql2/promise";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { ValidationError } from "../../src/errors.js";
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

describe("Legacy coverage: higher priority jobs are claimed first", () => {
	it("claims jobs in priority DESC, run_at, id order", async () => {
		const boss = await createBoss(pool, { batchSize: 1 });
		const queue = "priority-q-26";

		// Enqueue jobs with different priorities (lower number = lower priority)
		await boss.enqueue(queue, { p: "low" }, { priority: 0 });
		await boss.enqueue(queue, { p: "high" }, { priority: 10 });
		await boss.enqueue(queue, { p: "medium" }, { priority: 5 });
		await boss.enqueue(queue, { p: "highest" }, { priority: 100 });
		await boss.enqueue(queue, { p: "negative" }, { priority: -10 });

		const order: string[] = [];
		boss.work(queue, async (job) => {
			order.push((job.payload as { p: string }).p);
		});

		await new Promise((r) => setTimeout(r, 3000));
		await boss.stop();

		expect(order).toEqual(["highest", "high", "medium", "low", "negative"]);
	}, 10000);
});

describe("Legacy coverage: priority validation", () => {
	it("rejects priority outside SMALLINT range", async () => {
		const boss = await createBoss(pool);

		await expect(
			boss.enqueue("prio-q", { bad: true }, { priority: 32768 }),
		).rejects.toThrow(ValidationError);

		await expect(
			boss.enqueue("prio-q", { bad: true }, { priority: -32769 }),
		).rejects.toThrow(ValidationError);
	}, 5000);

	it("accepts boundary values", async () => {
		const boss = await createBoss(pool);

		const id1 = await boss.enqueue(
			"prio-q",
			{ edge: "max" },
			{ priority: 32767 },
		);
		expect(id1).toBeTruthy();

		const id2 = await boss.enqueue(
			"prio-q",
			{ edge: "min" },
			{ priority: -32768 },
		);
		expect(id2).toBeTruthy();

		await boss.stop();
	}, 5000);
});

describe("Legacy coverage: default priority is 0", () => {
	it("jobs without explicit priority get priority 0", async () => {
		const boss = await createBoss(pool, { batchSize: 1 });
		const queue = "priority-q-28";

		// Enqueue a high-priority job and a default-priority job
		await boss.enqueue(queue, { p: "default" });
		await boss.enqueue(queue, { p: "explicit" }, { priority: 1 });

		const order: string[] = [];
		boss.work(queue, async (job) => {
			order.push((job.payload as { p: string }).p);
		});

		await new Promise((r) => setTimeout(r, 2000));
		await boss.stop();

		// Explicit priority 1 should come before default priority 0
		expect(order).toEqual(["explicit", "default"]);
	}, 10000);
});
