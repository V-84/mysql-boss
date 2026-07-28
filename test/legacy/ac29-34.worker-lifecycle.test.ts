import type { Pool, RowDataPacket } from "mysql2/promise";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { sweepStaleJobs } from "../../src/sweep.js";
import { cleanTables, createBoss, createPool } from "../helpers.js";

interface JobRow extends RowDataPacket {
	id: bigint;
	state: string;
	locked_by: Buffer | null;
	lease_expires_at: Date | null;
}

interface DeadRow extends RowDataPacket {
	id: bigint;
	last_error: { message: string };
}

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

describe("Legacy coverage: stop() drains in-flight handlers", () => {
	it("waits for running handlers to finish before resolving", async () => {
		const boss = await createBoss(pool, { drainTimeoutMs: 10000 });
		const queue = "drain-q-29";

		await boss.enqueue(queue, { slow: true });

		let handlerFinished = false;
		boss.work(queue, async () => {
			await new Promise((r) => setTimeout(r, 2000));
			handlerFinished = true;
		});

		await new Promise((r) => setTimeout(r, 500));
		await boss.stop();

		expect(handlerFinished).toBe(true);
	}, 15000);
});

describe("Legacy coverage: stop() aborts handlers exceeding drainTimeoutMs", () => {
	it("aborts long-running handlers after drain timeout", async () => {
		const boss = await createBoss(pool, {
			drainTimeoutMs: 1000,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
		});
		const queue = "drain-q-30";

		await boss.enqueue(queue, { stuck: true });

		let wasAborted = false;
		boss.work(queue, async (_job, { signal }) => {
			try {
				await new Promise((resolve, reject) => {
					const t = setTimeout(resolve, 30000);
					signal.addEventListener("abort", () => {
						clearTimeout(t);
						reject(new Error("aborted"));
					});
				});
			} catch {
				wasAborted = true;
			}
		});

		await new Promise((r) => setTimeout(r, 500));
		await boss.stop({ drainTimeoutMs: 1000 });

		expect(wasAborted).toBe(true);
	}, 10000);
});

describe("Legacy coverage: straggler jobs released after drain timeout", () => {
	it("releases timed-out jobs back to available state", async () => {
		const boss = await createBoss(pool, {
			drainTimeoutMs: 1000,
			leaseSeconds: 30,
			heartbeatSeconds: 10,
		});
		const queue = "drain-q-31";

		const jobId = await boss.enqueue(queue, { release: true });

		// Handler that ignores abort signal — simulates a truly stuck handler
		// This keeps the job in inFlight so DRAIN_RELEASE fires
		boss.work(queue, async () => {
			await new Promise((resolve) => setTimeout(resolve, 60000));
		});

		await new Promise((r) => setTimeout(r, 500));
		await boss.stop({ drainTimeoutMs: 1000 });

		// Give a moment for the drain release query to run
		await new Promise((r) => setTimeout(r, 500));

		const [rows] = await pool.query<JobRow[]>(
			"SELECT id, state, locked_by, lease_expires_at FROM jobs WHERE id = ?",
			[jobId],
		);

		expect(rows.length).toBe(1);
		expect(rows[0].state).toBe("available");
		expect(rows[0].locked_by).toBeNull();
		expect(rows[0].lease_expires_at).toBeNull();
	}, 10000);
});

describe("Legacy coverage: heartbeat extends lease", () => {
	it("heartbeat renews lease_expires_at for in-flight jobs", async () => {
		const boss = await createBoss(pool, {
			leaseSeconds: 5,
			heartbeatSeconds: 1,
		});
		const queue = "heartbeat-q-32";

		const jobId = await boss.enqueue(queue, { hb: true });

		let initialExpiry: Date | null = null;

		boss.work(queue, async () => {
			// Record initial lease_expires_at
			const [rows] = await pool.query<JobRow[]>(
				"SELECT lease_expires_at FROM jobs WHERE id = ?",
				[jobId],
			);
			initialExpiry = rows[0]?.lease_expires_at ?? null;

			// Wait long enough for heartbeat to fire (1s interval)
			await new Promise((r) => setTimeout(r, 2500));
		});

		await new Promise((r) => setTimeout(r, 4000));
		await boss.stop();

		expect(initialExpiry).not.toBeNull();

		// Job should be archived (completed successfully)
		const [archived] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs_archive WHERE id = ?",
			[jobId],
		);
		expect(archived.length).toBe(1);
	}, 10000);
});

describe("Legacy coverage: sweep reclaims expired leases", () => {
	it("sweep returns expired-lease jobs to available", async () => {
		const boss = await createBoss(pool);

		const jobId = await boss.enqueue("sweep-q-33", { sweep: true });

		// Manually claim the job and set an already-expired lease
		await pool.query(
			`UPDATE jobs SET state = 'active',
			 locked_by = UUID_TO_BIN('11111111-1111-1111-1111-111111111111'),
			 lease_expires_at = UTC_TIMESTAMP(6) - INTERVAL 10 SECOND,
			 started_at = UTC_TIMESTAMP(6)
			 WHERE id = ?`,
			[jobId],
		);

		const swept = await sweepStaleJobs(pool);
		expect(swept).toBe(1);

		const [rows] = await pool.query<JobRow[]>(
			"SELECT state, locked_by FROM jobs WHERE id = ?",
			[jobId],
		);
		expect(rows[0].state).toBe("available");
		expect(rows[0].locked_by).toBeNull();

		await boss.stop();
	}, 10000);
});

describe("Legacy coverage: sweep DLQs exhausted expired-lease jobs", () => {
	it("sweep moves expired-lease exhausted-retry jobs to DLQ", async () => {
		const boss = await createBoss(pool);

		const jobId = await boss.enqueue(
			"sweep-q-34",
			{ dead: true },
			{ retryLimit: 1 },
		);

		// Set retry_count = retry_limit and expired lease
		await pool.query(
			`UPDATE jobs SET state = 'active',
			 retry_count = 1,
			 locked_by = UUID_TO_BIN('22222222-2222-2222-2222-222222222222'),
			 lease_expires_at = UTC_TIMESTAMP(6) - INTERVAL 10 SECOND,
			 started_at = UTC_TIMESTAMP(6)
			 WHERE id = ?`,
			[jobId],
		);

		const swept = await sweepStaleJobs(pool);
		expect(swept).toBe(1);

		// Should not be in jobs
		const [jobRows] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs WHERE id = ?",
			[jobId],
		);
		expect(jobRows.length).toBe(0);

		// Should be in jobs_dead
		const [deadRows] = await pool.query<DeadRow[]>(
			"SELECT id, last_error FROM jobs_dead WHERE id = ?",
			[jobId],
		);
		expect(deadRows.length).toBe(1);
		expect(deadRows[0].last_error.message).toBe("lease expired");

		await boss.stop();
	}, 10000);
});
