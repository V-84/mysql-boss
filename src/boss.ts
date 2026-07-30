import { randomUUID } from "node:crypto";
import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { getArchivedJob, listArchive, pruneArchive } from "./archive.js";
import { withConnection } from "./connection.js";
import { listDead, replayDead } from "./dlq.js";
import { ConfigError, ValidationError } from "./errors.js";
import type {
	ArchivedJob,
	DeadJob,
	EnqueueOptions,
	JobHandler,
	MysqlBossOptions,
	ScheduleOptions,
	WorkOptions,
} from "./index.js";
import { migrate } from "./migrate.js";
import { type SqlStatements, createSql } from "./sql.js";
import { deleteSchedule, runTick, upsertSchedule } from "./tick.js";
import { toUtcString } from "./util.js";
import { WorkerManager } from "./worker.js";

export class MysqlBoss {
	private pool: Pool;
	private workerId: string;
	private worker: WorkerManager | null = null;
	private tickTimer: ReturnType<typeof setInterval> | null = null;
	private pruneCounter = 0;
	private stopped = false;
	private sql: SqlStatements;

	private pollIntervalMs: number;
	private batchSize: number;
	private concurrency: number;
	private leaseSeconds: number;
	private heartbeatSeconds: number;
	private sweepIntervalMs: number;
	private tickIntervalMs: number;
	private archiveRetentionDays: number;
	private drainTimeoutMs: number;
	private maintenance: boolean;
	private onError: (err: unknown, context: string) => void;

	constructor(opts: MysqlBossOptions) {
		this.pool = opts.pool;
		this.workerId = randomUUID();
		try {
			this.sql = createSql(opts.tablePrefix);
		} catch (error) {
			throw new ConfigError(
				error instanceof Error ? error.message : "Invalid tablePrefix",
			);
		}

		this.pollIntervalMs = opts.pollIntervalMs ?? 2000;
		this.batchSize = opts.batchSize ?? 10;
		this.concurrency = opts.concurrency ?? this.batchSize;
		this.leaseSeconds = opts.leaseSeconds ?? 300;
		this.heartbeatSeconds = opts.heartbeatSeconds ?? 100;
		this.sweepIntervalMs = opts.sweepIntervalMs ?? 60_000;
		this.tickIntervalMs = opts.tickIntervalMs ?? 30_000;
		this.archiveRetentionDays = opts.archiveRetentionDays ?? 14;
		this.drainTimeoutMs = opts.drainTimeoutMs ?? 30_000;
		this.maintenance = opts.maintenance ?? true;
		this.onError = opts.onError ?? (() => {});

		if (this.batchSize < 1 || this.batchSize > 100) {
			throw new ConfigError("batchSize must be between 1 and 100");
		}
		if (this.leaseSeconds < 3 * this.heartbeatSeconds) {
			throw new ConfigError(
				`leaseSeconds (${this.leaseSeconds}) must be >= 3 × heartbeatSeconds (${this.heartbeatSeconds})`,
			);
		}
		if (
			this.archiveRetentionDays < 1 ||
			!Number.isInteger(this.archiveRetentionDays)
		) {
			throw new ConfigError("archiveRetentionDays must be an integer >= 1");
		}
	}

	async migrate(): Promise<void> {
		await migrate(this.pool, this.sql);
	}

	async enqueue<T>(
		queue: string,
		payload: T,
		opts?: EnqueueOptions,
	): Promise<string | null> {
		const priority = opts?.priority ?? 0;
		const singletonKey = opts?.singletonKey ?? null;
		const retryLimit = opts?.retryLimit ?? 2;
		const retryDelaySecs = opts?.retryDelaySecs ?? 30;
		const retryBackoff = opts?.retryBackoff ? 1 : 0;
		const runAt = opts?.runAt ?? null;

		if (priority < -32768 || priority > 32767) {
			throw new ValidationError(
				`priority must be between -32768 and 32767, got ${priority}`,
			);
		}

		if (singletonKey !== null) {
			if (singletonKey.length > 191) {
				throw new ValidationError("singletonKey must be <= 191 characters");
			}
			if (singletonKey.startsWith("cron:")) {
				throw new ValidationError(
					'singletonKey must not start with "cron:" (reserved for scheduled jobs)',
				);
			}
		}

		const payloadJson = payload != null ? JSON.stringify(payload) : null;
		const runAtParam = runAt ? toUtcString(runAt) : null;

		if (singletonKey !== null) {
			return withConnection(this.pool, async (connection) => {
				const [result] = await connection.query<ResultSetHeader>(
					this.sql.ENQUEUE_SINGLETON,
					[
						queue,
						priority,
						payloadJson,
						singletonKey,
						retryLimit,
						retryDelaySecs,
						retryBackoff,
						runAtParam,
					],
				);
				if (result.affectedRows === 0) return null;
				const [rows] = await connection.query<
					(RowDataPacket & { id: string })[]
				>(this.sql.LAST_INSERT_ID);
				return rows[0].id;
			});
		}

		return withConnection(this.pool, async (connection) => {
			const [result] = await connection.query<ResultSetHeader>(
				this.sql.ENQUEUE,
				[
					queue,
					priority,
					payloadJson,
					singletonKey,
					retryLimit,
					retryDelaySecs,
					retryBackoff,
					runAtParam,
				],
			);
			const [rows] = await connection.query<(RowDataPacket & { id: string })[]>(
				this.sql.LAST_INSERT_ID,
			);
			return rows[0].id;
		});
	}

	work<T>(queue: string, handler: JobHandler<T>, opts?: WorkOptions): void {
		if (this.stopped) {
			throw new Error("Cannot register work after stop()");
		}

		const queueConcurrency = opts?.concurrency ?? this.concurrency;
		if (
			!Number.isInteger(queueConcurrency) ||
			queueConcurrency < 1 ||
			queueConcurrency > 1000
		) {
			throw new ValidationError(
				`concurrency must be an integer between 1 and 1000, got ${opts?.concurrency}`,
			);
		}

		if (!this.worker) {
			this.worker = new WorkerManager({
				pool: this.pool,
				workerId: this.workerId,
				pollIntervalMs: this.pollIntervalMs,
				batchSize: this.batchSize,
				concurrency: this.concurrency,
				leaseSeconds: this.leaseSeconds,
				heartbeatSeconds: this.heartbeatSeconds,
				sweepIntervalMs: this.sweepIntervalMs,
				drainTimeoutMs: this.drainTimeoutMs,
				maintenance: this.maintenance,
				onError: this.onError,
				sql: this.sql,
			});
			if (this.maintenance) {
				this.startTick();
			}
		}

		this.worker.registerQueue(queue, handler as JobHandler, queueConcurrency);
	}

	private startTick(): void {
		if (this.tickTimer) return;
		const jitter = 0.8 + Math.random() * 0.4;
		this.tickTimer = setInterval(async () => {
			try {
				await runTick(this.pool, this.sql);
				this.pruneCounter++;
				if (this.pruneCounter >= 10) {
					this.pruneCounter = 0;
					await pruneArchive(this.pool, this.archiveRetentionDays, this.sql);
				}
			} catch (err) {
				this.onError(err, "tick");
			}
		}, this.tickIntervalMs * jitter);
		this.tickTimer.unref();
	}

	async schedule(
		name: string,
		queue: string,
		cron: string,
		opts?: ScheduleOptions,
	): Promise<void> {
		const timezone = opts?.timezone ?? "UTC";
		const payload = opts?.payload ?? null;
		const jobOptions = {
			priority: opts?.priority ?? 0,
			retryLimit: opts?.retryLimit ?? 2,
			retryDelaySecs: opts?.retryDelaySecs ?? 30,
			retryBackoff: opts?.retryBackoff ?? false,
		};

		await upsertSchedule(
			this.pool,
			name,
			queue,
			cron,
			timezone,
			payload,
			jobOptions,
			this.sql,
		);
	}

	async unschedule(name: string): Promise<void> {
		await deleteSchedule(this.pool, name, this.sql);
	}

	async listDead(q: {
		queue: string;
		before?: Date;
		after?: Date;
		limit?: number;
		offset?: number;
	}): Promise<DeadJob[]> {
		const before = q.before ?? new Date("9999-12-31");
		const after = q.after ?? new Date("1970-01-01");
		const limit = q.limit ?? 50;
		const offset = q.offset ?? 0;

		return listDead(this.pool, q.queue, before, after, limit, offset, this.sql);
	}

	async replayDead(ids: string[]): Promise<number> {
		return replayDead(this.pool, ids, this.sql);
	}

	async getArchivedJob(id: string): Promise<ArchivedJob | null> {
		return getArchivedJob(this.pool, id, this.sql);
	}

	async listArchive(q: {
		queue: string;
		before?: Date;
		limit?: number;
	}): Promise<ArchivedJob[]> {
		const before = q.before ?? new Date("9999-12-31");
		const limit = q.limit ?? 50;

		return listArchive(this.pool, q.queue, before, limit, this.sql);
	}

	async stop(opts?: { drainTimeoutMs?: number }): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;

		if (this.tickTimer) {
			clearInterval(this.tickTimer);
			this.tickTimer = null;
		}

		if (this.worker) {
			await this.worker.stop(opts?.drainTimeoutMs);
		}
	}
}
