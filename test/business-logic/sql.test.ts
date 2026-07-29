import type { Pool, PoolConnection, RowDataPacket } from "mysql2/promise";
import { describe, expect, it, vi } from "vitest";
import { claimJobs } from "../../src/claim.js";
import { acquireConnection } from "../../src/connection.js";
import { deadLetterJob, replayDead } from "../../src/dlq.js";
import {
	DEFAULT_TABLE_PREFIX,
	INIT_SESSION,
	UNPREFIXED_SQL,
	createSql,
} from "../../src/sql.js";
import { runTick } from "../../src/tick.js";

function fakePool(connection: Partial<PoolConnection>): Pool {
	return {
		getConnection: vi.fn().mockResolvedValue(connection),
	} as unknown as Pool;
}

describe("SQL generation and transactional business logic", () => {
	it("prefixes every table reference without corrupting overlapping names", () => {
		const sql = createSql("tenant");

		expect(DEFAULT_TABLE_PREFIX).toBe("mysql_boss");
		expect(sql.CREATE_JOBS).toContain("`tenant_jobs`");
		expect(sql.CREATE_JOBS_ARCHIVE).toContain("`tenant_jobs_archive`");
		expect(sql.CREATE_JOBS_ARCHIVE).not.toContain("`tenant_jobs`_archive");
		expect(sql.CREATE_JOBS_DEAD).toContain("`tenant_jobs_dead`");
		expect(sql.CREATE_SCHEDULES).toContain("`tenant_schedules`");
		expect(sql.REPLAY_INSERT).toContain("FROM `tenant_jobs_dead`");
		expect(sql.REPLAY_INSERT).toContain("INSERT INTO `tenant_jobs`");
		expect(sql.TICK_SELECT).toContain("FROM `tenant_schedules`");
		expect(sql.TICK_ENQUEUE).toContain("INSERT IGNORE INTO `tenant_jobs`");
	});

	it("supports deliberately unprefixed tables and rejects unsafe identifiers", () => {
		const sql = createSql("");

		expect(sql.CREATE_JOBS).toContain("CREATE TABLE IF NOT EXISTS `jobs`");
		expect(sql.REPLAY_INSERT).toContain("FROM `jobs_dead`");
		expect(() => createSql("tenant-name")).toThrow(/ASCII letters/);
		expect(() => createSql("x".repeat(51))).toThrow(/50 characters/);
	});

	it("releases a connection when session initialization fails", async () => {
		const initError = new Error("session init failed");
		const connection = {
			query: vi.fn().mockRejectedValue(initError),
			release: vi.fn(),
		};

		await expect(acquireConnection(fakePool(connection))).rejects.toBe(
			initError,
		);
		expect(connection.query).toHaveBeenCalledWith(INIT_SESSION);
		expect(connection.release).toHaveBeenCalledOnce();
	});

	it("rolls back when a claimed batch cannot be fully fenced", async () => {
		const connection = {
			beginTransaction: vi.fn(),
			commit: vi.fn(),
			rollback: vi.fn().mockResolvedValue(undefined),
			release: vi.fn(),
			query: vi.fn(async (statement: string) => {
				if (statement === INIT_SESSION) return [[], []];
				if (statement === UNPREFIXED_SQL.CLAIM_SELECT) {
					return [
						[
							{
								id: "41",
								queue: "payments",
								payload: { amount: 10 },
								retry_count: 0,
								retry_limit: 2,
								retry_delay_secs: 30,
								retry_backoff: 0,
							},
						],
						[],
					];
				}
				if (statement === UNPREFIXED_SQL.CLAIM_UPDATE) {
					return [{ affectedRows: 0 }, []];
				}
				throw new Error(`Unexpected SQL: ${statement}`);
			}),
		};

		await expect(
			claimJobs(
				fakePool(connection),
				"payments",
				"c603111d-e3a8-42eb-8e15-c67ea596d189",
				1,
				30,
			),
		).rejects.toThrow(
			"Claim invariant violated: selected 1 jobs but updated 0",
		);
		expect(connection.rollback).toHaveBeenCalledOnce();
		expect(connection.commit).not.toHaveBeenCalled();
		expect(connection.release).toHaveBeenCalledOnce();
	});

	it("commits a no-op dead-letter move when lease ownership was lost", async () => {
		const connection = {
			beginTransaction: vi.fn(),
			commit: vi.fn(),
			rollback: vi.fn().mockResolvedValue(undefined),
			release: vi.fn(),
			query: vi.fn(async (statement: string) => {
				if (statement === INIT_SESSION) return [[], []];
				if (statement === UNPREFIXED_SQL.DLQ_INSERT) {
					return [{ affectedRows: 0 }, []];
				}
				throw new Error(`Unexpected SQL: ${statement}`);
			}),
		};

		await expect(
			deadLetterJob(
				fakePool(connection),
				"42",
				"4e678982-196f-4f33-82dd-3dff2a18ed62",
				{ message: "failed", at: "2026-07-29T00:00:00.000Z" },
			),
		).resolves.toBe(false);
		expect(connection.commit).toHaveBeenCalledOnce();
		expect(connection.rollback).not.toHaveBeenCalled();
		expect(connection.release).toHaveBeenCalledOnce();
	});

	it("rolls back a failed dead-letter move so the hot job remains recoverable", async () => {
		const writeError = new Error("dead table unavailable");
		const connection = {
			beginTransaction: vi.fn(),
			commit: vi.fn(),
			rollback: vi.fn().mockResolvedValue(undefined),
			release: vi.fn(),
			query: vi.fn(async (statement: string) => {
				if (statement === INIT_SESSION) return [[], []];
				if (statement === UNPREFIXED_SQL.DLQ_INSERT) throw writeError;
				throw new Error(`Unexpected SQL: ${statement}`);
			}),
		};

		await expect(
			deadLetterJob(
				fakePool(connection),
				"43",
				"4e678982-196f-4f33-82dd-3dff2a18ed62",
				{ message: "failed", at: "2026-07-29T00:00:00.000Z" },
			),
		).rejects.toBe(writeError);
		expect(connection.rollback).toHaveBeenCalledOnce();
		expect(connection.commit).not.toHaveBeenCalled();
		expect(connection.release).toHaveBeenCalledOnce();
	});

	it("rolls replay back for malformed IDs without misreporting a singleton collision", async () => {
		const connection = {
			beginTransaction: vi.fn(),
			commit: vi.fn(),
			rollback: vi.fn().mockResolvedValue(undefined),
			release: vi.fn(),
			query: vi.fn(async (statement: string) => {
				if (statement === INIT_SESSION) return [[], []];
				throw new Error(`Unexpected SQL: ${statement}`);
			}),
		};

		await expect(
			replayDead(fakePool(connection), ["not-a-bigint"]),
		).rejects.toThrow(SyntaxError);
		expect(connection.rollback).toHaveBeenCalledOnce();
		expect(connection.release).toHaveBeenCalledOnce();
	});

	it("rolls a cron tick back when one selected schedule is invalid", async () => {
		const connection = {
			beginTransaction: vi.fn(),
			commit: vi.fn(),
			rollback: vi.fn().mockResolvedValue(undefined),
			release: vi.fn(),
			query: vi.fn(async (statement: string) => {
				if (statement === INIT_SESSION) return [[], []];
				if (statement === UNPREFIXED_SQL.DB_NOW) {
					return [[{ db_now_unix: 1_785_283_200 }], []];
				}
				if (statement === UNPREFIXED_SQL.TICK_SELECT) {
					return [
						[
							{
								name: "invalid",
								queue: "scheduled",
								cron: "not a cron",
								timezone: "UTC",
								payload: null,
								job_options: null,
								next_run_at_unix: 1_785_283_140,
							},
						] satisfies RowDataPacket[],
						[],
					];
				}
				if (statement === UNPREFIXED_SQL.TICK_ENQUEUE) {
					return [{ affectedRows: 1 }, []];
				}
				throw new Error(`Unexpected SQL: ${statement}`);
			}),
		};

		await expect(runTick(fakePool(connection))).rejects.toThrow(
			/Cron expression must have exactly 5 fields/,
		);
		expect(connection.rollback).toHaveBeenCalledOnce();
		expect(connection.commit).not.toHaveBeenCalled();
		expect(connection.release).toHaveBeenCalledOnce();
	});
});
