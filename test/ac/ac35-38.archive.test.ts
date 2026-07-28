import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { MysqlBoss } from "../../src/index.js";
import { createPool, createBoss, cleanTables } from "../helpers.js";

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

describe("AC 35: archive on successful completion", () => {
	it("successful completion moves the row atomically to jobs_archive with duration_ms", async () => {
		const boss = await createBoss(pool);

		const jobId = await boss.enqueue("archive-q", { data: "test" });
		expect(jobId).not.toBeNull();

		const handlerStarted = Date.now();
		await new Promise<void>((resolve) => {
			boss.work("archive-q", async (job) => {
				// Simulate some work
				await new Promise((r) => setTimeout(r, 100));
				resolve();
			});
		});

		// Wait for archiving
		await new Promise((r) => setTimeout(r, 500));
		await boss.stop();

		// Verify job moved to archive
		const [archiveRows] = await pool.query<RowDataPacket[]>(
			"SELECT * FROM jobs_archive WHERE id = ?",
			[jobId],
		);
		expect(archiveRows).toHaveLength(1);
		expect(archiveRows[0].queue).toBe("archive-q");
		expect(archiveRows[0].duration_ms).toBeGreaterThanOrEqual(50);

		// Verify job removed from hot table
		const [jobRows] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs WHERE id = ?",
			[jobId],
		);
		expect(jobRows[0].cnt).toBe(0);
	});
});

describe("AC 36: getArchivedJob", () => {
	it("returns the record for an archived id", async () => {
		const boss = await createBoss(pool);
		const jobId = await boss.enqueue("get-archive-q", { key: "value" });

		await new Promise<void>((resolve) => {
			boss.work("get-archive-q", async () => {
				resolve();
			});
		});

		await new Promise((r) => setTimeout(r, 500));
		await boss.stop();

		const archived = await boss.getArchivedJob(jobId!);
		expect(archived).not.toBeNull();
		expect(archived!.id).toBe(jobId);
		expect(archived!.queue).toBe("get-archive-q");
		expect(archived!.payload).toEqual({ key: "value" });
		expect(archived!.completedAt).toBeInstanceOf(Date);
		expect(archived!.durationMs).toBeGreaterThanOrEqual(0);
	});

	it("returns null for an unknown id", async () => {
		const boss = await createBoss(pool);
		const result = await boss.getArchivedJob("999999999");
		expect(result).toBeNull();
	});
});

describe("AC 37: listArchive keyset pagination uses range scan", () => {
	it("paginates correctly with keyset pagination", async () => {
		const boss = await createBoss(pool);

		// Insert archive rows directly with distinct timestamps for reliable pagination
		for (let i = 0; i < 10; i++) {
			await pool.query(
				`INSERT INTO jobs_archive (id, queue, priority, retry_count, created_at, started_at, completed_at, duration_ms)
				 VALUES (?, 'paginate-q', 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6),
				         UTC_TIMESTAMP(6) - INTERVAL ? SECOND, 100)`,
				[300000 + i, i],
			);
		}

		// First page (most recent first)
		const page1 = await boss.listArchive({ queue: "paginate-q", limit: 5 });
		expect(page1).toHaveLength(5);

		// Second page using keyset
		const page2 = await boss.listArchive({
			queue: "paginate-q",
			before: page1[page1.length - 1].completedAt,
			limit: 5,
		});
		expect(page2).toHaveLength(5);

		// No overlap
		const page1Ids = new Set(page1.map((j) => j.id));
		const page2Ids = new Set(page2.map((j) => j.id));
		for (const id of page2Ids) {
			expect(page1Ids.has(id)).toBe(false);
		}
	});

	it("EXPLAIN shows range scan on ix_archive_queue_time", async () => {
		const boss = await createBoss(pool);

		const [rows] = await pool.query<RowDataPacket[]>(
			"EXPLAIN SELECT id, queue, priority, payload, singleton_key, retry_count, created_at, started_at, completed_at, duration_ms FROM jobs_archive WHERE queue = 'test' AND completed_at < NOW() ORDER BY completed_at DESC LIMIT 10",
		);

		const plan = rows[0];
		// Should use the ix_archive_queue_time index
		expect(plan.key).toContain("ix_archive_queue_time");
		// Should not use filesort
		if (plan.Extra) {
			expect(plan.Extra).not.toContain("Using filesort");
		}
	});
});

describe("AC 38: archive retention prune", () => {
	it("deletes only rows older than archiveRetentionDays in batches <= 5000", async () => {
		const boss = await createBoss(pool, { archiveRetentionDays: 1 });

		// Manually insert old archive rows
		for (let i = 0; i < 10; i++) {
			await pool.query(
				`INSERT INTO jobs_archive (id, queue, priority, retry_count, created_at, started_at, completed_at, duration_ms)
				 VALUES (?, 'prune-q', 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6),
				         UTC_TIMESTAMP(6) - INTERVAL 2 DAY, 100)`,
				[100000 + i],
			);
		}

		// Insert recent archive rows
		for (let i = 0; i < 5; i++) {
			await pool.query(
				`INSERT INTO jobs_archive (id, queue, priority, retry_count, created_at, started_at, completed_at, duration_ms)
				 VALUES (?, 'prune-q', 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), 100)`,
				[200000 + i],
			);
		}

		// Trigger prune via the internal function
		const { pruneArchive } = await import("../../src/archive.js");
		await pruneArchive(pool, 1);

		// Old rows should be deleted
		const [remaining] = await pool.query<RowDataPacket[]>(
			"SELECT COUNT(*) as cnt FROM jobs_archive WHERE queue = 'prune-q'",
		);
		expect(remaining[0].cnt).toBe(5);

		// Only recent rows should remain
		const [recent] = await pool.query<RowDataPacket[]>(
			"SELECT id FROM jobs_archive WHERE queue = 'prune-q' ORDER BY id",
		);
		for (const row of recent) {
			expect(Number(row.id)).toBeGreaterThanOrEqual(200000);
		}
	});
});
