import { randomUUID } from "node:crypto";
import type { Pool, ResultSetHeader } from "mysql2/promise";
import {
	completeJob,
	getArchivedJob,
	listArchive,
	pruneArchive,
} from "./archive.js";
import { claimJobs } from "./claim.js";
import { deadLetterJob, listDead, replayDead } from "./dlq.js";
import {
	ConfigError,
	SingletonCollisionError,
	ValidationError,
} from "./errors.js";
import { failJob } from "./fail.js";
import type {
	ActiveJob,
	ArchivedJob,
	DeadJob,
	EnqueueOptions,
	JobHandler,
	MysqlBossOptions,
	ScheduleOptions,
} from "./index.js";
import { migrate } from "./migrate.js";
import {
	ENQUEUE,
	ENQUEUE_SINGLETON,
	SET_READ_COMMITTED,
	SET_UTC_TIMEZONE,
} from "./sql.js";
import { deleteSchedule, runTick, upsertSchedule } from "./tick.js";
import { WorkerManager } from "./worker.js";

export class MysqlBoss {
	private pool: Pool;
	private workerId: string;
	private worker: WorkerManager | null = null;
	private tickTimer: ReturnType<typeof setInterval> | null = null;
	private pruneCounter = 0;
	private stopped = false;

	private pollIntervalMs: number;
	private batchSize: number;
	private concurrency: number;
	private leaseSeconds: number;
	private heartbeatSeconds: number;
	private sweepIntervalMs: number;
	private tickIntervalMs: number;
	private archiveRetentionDays: number;
	private drainTimeoutMs: number;

	constructor(opts: MysqlBossOptions) {
		this.pool = opts.pool;
		this.workerId = randomUUID();

		this.pollIntervalMs = opts.pollIntervalMs ?? 2000;
		this.batchSize = opts.batchSize ?? 10;
		this.concurrency = opts.concurrency ?? this.batchSize;
		this.leaseSeconds = opts.leaseSeconds ?? 300;
		this.heartbeatSeconds = opts.heartbeatSeconds ?? 100;
		this.sweepIntervalMs = opts.sweepIntervalMs ?? 60_000;
		this.tickIntervalMs = opts.tickIntervalMs ?? 30_000;
		this.archiveRetentionDays = opts.archiveRetentionDays ?? 14;
		this.drainTimeoutMs = opts.drainTimeoutMs ?? 30_000;

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

		this.setupConnectionInit();
	}

	private setupConnectionInit(): void {
		// biome-ignore lint/suspicious/noExplicitAny: mysql2 promise pool doesn't expose the underlying callback pool type
		(this.pool as any).pool.on("connection", (conn: any) => {
			conn.query(SET_READ_COMMITTED, () => {});
			conn.query(SET_UTC_TIMEZONE, () => {});
		});
	}

	async migrate(): Promise<void> {
		await migrate(this.pool);
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
		const runAtParam = runAt ?? null;

		if (singletonKey !== null) {
			const [result] = await this.pool.query<ResultSetHeader>(
				ENQUEUE_SINGLETON,
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
			return result.insertId.toString();
		}

		const [result] = await this.pool.query<ResultSetHeader>(ENQUEUE, [
			queue,
			priority,
			payloadJson,
			singletonKey,
			retryLimit,
			retryDelaySecs,
			retryBackoff,
			runAtParam,
		]);
		return result.insertId.toString();
	}

	work<T>(queue: string, handler: JobHandler<T>): void {
		if (this.stopped) {
			throw new Error("Cannot register work after stop()");
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
			});
			this.startTick();
		}

		this.worker.registerQueue(queue, handler as JobHandler);
	}

	private startTick(): void {
		if (this.tickTimer) return;
		const jitter = 0.8 + Math.random() * 0.4;
		this.tickTimer = setInterval(async () => {
			try {
				await runTick(this.pool);
				this.pruneCounter++;
				if (this.pruneCounter >= 10) {
					this.pruneCounter = 0;
					await pruneArchive(this.pool, this.archiveRetentionDays);
				}
			} catch {
				// tick failure is not fatal
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
		);
	}

	async unschedule(name: string): Promise<void> {
		await deleteSchedule(this.pool, name);
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

		return listDead(this.pool, q.queue, before, after, limit, offset);
	}

	async replayDead(ids: string[]): Promise<number> {
		return replayDead(this.pool, ids);
	}

	async getArchivedJob(id: string): Promise<ArchivedJob | null> {
		return getArchivedJob(this.pool, id);
	}

	async listArchive(q: {
		queue: string;
		before?: Date;
		limit?: number;
	}): Promise<ArchivedJob[]> {
		const before = q.before ?? new Date("9999-12-31");
		const limit = q.limit ?? 50;

		return listArchive(this.pool, q.queue, before, limit);
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
