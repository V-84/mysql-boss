import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { MysqlBoss } from "../../src/index.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

let pool: Pool;

beforeAll(async () => {
	pool = await createPool();
	const boss = new MysqlBoss({ pool });
	await boss.migrate();
}, 30_000);

afterAll(async () => {
	await pool.end();
}, 10_000);

beforeEach(async () => {
	await cleanTables(pool);
});

describe("debug: basic work flow", () => {
	it("multi worker contention", async () => {
		const JOB_COUNT = 50;
		const boss = await createBoss(pool, { pollIntervalMs: 50 });

		for (let i = 0; i < JOB_COUNT; i++) {
			await boss.enqueue("multi-q", { idx: i });
		}

		const processed: string[] = [];

		const pool2 = await createPool();
		const boss2 = await createBoss(pool2, { pollIntervalMs: 50 });

		boss.work("multi-q", async (job) => {
			processed.push(`w1:${job.id}`);
		});
		boss2.work("multi-q", async (job) => {
			processed.push(`w2:${job.id}`);
		});

		const deadline = Date.now() + 15_000;
		while (Date.now() < deadline) {
			if (processed.length >= JOB_COUNT) break;
			await new Promise((r) => setTimeout(r, 200));
		}

		console.log(`Processed ${processed.length} / ${JOB_COUNT}`);
		const [remaining] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs WHERE queue = 'multi-q'",
		);
		console.log("Remaining in jobs:", remaining[0].cnt);

		await boss.stop();
		await boss2.stop();
		await pool2.end();

		expect(processed.length).toBe(JOB_COUNT);
	}, 30_000);

	it("enqueue then work processes the job", async () => {
		const boss = await createBoss(pool, { pollIntervalMs: 100 });

		await boss.enqueue("debug-q", { hello: "world" });

		// Verify the job is in the table
		const [before] = await pool.query<RowDataPacket[]>(
			"SELECT id, state, queue FROM jobs WHERE queue = 'debug-q'",
		);
		console.log("Jobs before work:", before);

		let processed = false;
		let jobPayload: unknown = null;
		const _jobError: unknown = null;

		boss.work("debug-q", async (job) => {
			console.log(
				"Handler called with job:",
				job.id,
				JSON.stringify(job.payload),
			);
			processed = true;
			jobPayload = job.payload;
		});

		// Wait for processing
		await new Promise((r) => setTimeout(r, 2000));

		console.log("Processed:", processed, "Payload:", jobPayload);

		await boss.stop();

		// Check what's in the tables
		const [jobsRemaining] = await pool.query<RowDataPacket[]>(
			"SELECT id, state FROM jobs WHERE queue = 'debug-q'",
		);
		const [archived] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs_archive WHERE queue = 'debug-q'",
		);
		console.log("Jobs remaining:", jobsRemaining);
		console.log("Archived:", archived);

		expect(processed).toBe(true);
	});
});
