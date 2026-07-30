import type { Pool } from "mysql2/promise";
import { completeJob } from "./archive.js";
import { claimJobs } from "./claim.js";
import { withConnection } from "./connection.js";
import { deadLetterJob } from "./dlq.js";
import { failJob } from "./fail.js";
import { heartbeatOwnedJobs } from "./heartbeat.js";
import type { ActiveJob, JobHandler } from "./index.js";
import type { SqlStatements } from "./sql.js";
import { sweepStaleJobs } from "./sweep.js";

interface WorkerConfig {
	pool: Pool;
	workerId: string;
	pollIntervalMs: number;
	batchSize: number;
	concurrency: number;
	leaseSeconds: number;
	heartbeatSeconds: number;
	sweepIntervalMs: number;
	drainTimeoutMs: number;
	maintenance: boolean;
	onError: (err: unknown, context: string) => void;
	sql: SqlStatements;
}

interface InFlightJob {
	jobId: string;
	jobIdBigint: bigint;
	abortController: AbortController;
	promise: Promise<void>;
}

export class WorkerManager {
	private config: WorkerConfig;
	private registrations = new Map<string, JobHandler>();
	private queueLimits = new Map<string, number>();
	private inFlight = new Map<string, InFlightJob>();
	private pollTimers = new Map<string, ReturnType<typeof setTimeout>>();
	private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
	private sweepTimer: ReturnType<typeof setInterval> | null = null;
	private stopped = false;

	constructor(config: WorkerConfig) {
		this.config = config;
	}

	registerQueue(queue: string, handler: JobHandler, concurrency?: number): void {
		if (this.registrations.has(queue)) {
			throw new Error(`Handler already registered for queue "${queue}"`);
		}
		this.registrations.set(queue, handler);
		this.queueLimits.set(queue, concurrency ?? this.config.concurrency);
		this.startPolling(queue);
		this.ensureTimers();
	}

	private ensureTimers(): void {
		if (!this.heartbeatTimer) {
			this.heartbeatTimer = setInterval(
				() => this.heartbeat(),
				this.config.heartbeatSeconds * 1000,
			);
			this.heartbeatTimer.unref();
		}
		if (!this.sweepTimer && this.config.maintenance) {
			const jitteredInterval =
				this.config.sweepIntervalMs * (0.8 + Math.random() * 0.4);
			this.sweepTimer = setInterval(() => this.sweep(), jitteredInterval);
			this.sweepTimer.unref();
		}
	}

	private startPolling(queue: string): void {
		if (this.stopped) return;
		this.poll(queue);
	}

	private async poll(queue: string): Promise<void> {
		if (this.stopped) return;

		const handler = this.registrations.get(queue);
		if (!handler) return;

		let queueInFlight = 0;
		for (const [key] of this.inFlight) {
			if (key.startsWith(`${queue}/`)) queueInFlight++;
		}

		const limit = this.queueLimits.get(queue) ?? this.config.concurrency;
		const available = limit - queueInFlight;
		if (available <= 0) {
			this.schedulePoll(queue);
			return;
		}

		try {
			const batch = Math.min(available, this.config.batchSize);
			const jobs = await claimJobs(
				this.config.pool,
				queue,
				this.config.workerId,
				batch,
				this.config.leaseSeconds,
				this.config.sql,
			);

			for (const job of jobs) {
				this.dispatch(queue, job, handler);
			}

			if (jobs.length >= batch && !this.stopped) {
				setImmediate(() => this.poll(queue));
				return;
			}
		} catch (err) {
			this.config.onError(err, "poll");
		}

		this.schedulePoll(queue);
	}

	private schedulePoll(queue: string): void {
		if (this.stopped) return;
		const jitter = 0.8 + Math.random() * 0.4;
		const delay = this.config.pollIntervalMs * jitter;
		const timer = setTimeout(() => this.poll(queue), delay);
		timer.unref();
		this.pollTimers.set(queue, timer);
	}

	private dispatch(queue: string, job: ActiveJob, handler: JobHandler): void {
		const ac = new AbortController();
		const key = `${queue}/${job.id}`;

		const promise = this.executeJob(job, handler, ac)
			.catch((error) => this.reportError(error, "job"))
			.finally(() => {
				this.inFlight.delete(key);
			});

		this.inFlight.set(key, {
			jobId: job.id,
			jobIdBigint: BigInt(job.id),
			abortController: ac,
			promise,
		});
	}

	private async executeJob(
		job: ActiveJob,
		handler: JobHandler,
		abortController: AbortController,
	): Promise<void> {
		try {
			await handler(job, { signal: abortController.signal });
		} catch (error) {
			if (abortController.signal.aborted) return;
			await this.handleHandlerFailure(job.id, error);
			return;
		}

		if (abortController.signal.aborted) return;

		try {
			await completeJob(
				this.config.pool,
				job.id,
				this.config.workerId,
				this.config.sql,
			);
		} catch (error) {
			this.reportError(error, "complete");
		}
	}

	private async handleHandlerFailure(
		jobId: string,
		error: unknown,
	): Promise<void> {
		const errorObj = {
			message: error instanceof Error ? error.message : String(error),
			stack: error instanceof Error ? error.stack : undefined,
			at: new Date().toISOString(),
		};

		const retried = await failJob(
			this.config.pool,
			jobId,
			this.config.workerId,
			errorObj,
			this.config.sql,
		);

		if (!retried) {
			await deadLetterJob(
				this.config.pool,
				jobId,
				this.config.workerId,
				errorObj,
				this.config.sql,
			);
		}
	}

	private reportError(error: unknown, context: string): void {
		try {
			this.config.onError(error, context);
		} catch {
			// User-provided error reporters must not break worker bookkeeping.
		}
	}

	private async heartbeat(): Promise<void> {
		if (this.inFlight.size === 0) return;

		const entries = [...this.inFlight.values()];
		const ids = entries.map((j) => j.jobIdBigint);
		try {
			const ownedIds = await heartbeatOwnedJobs(
				this.config.pool,
				ids,
				this.config.workerId,
				this.config.leaseSeconds,
				this.config.sql,
			);

			for (const entry of entries) {
				if (!ownedIds.has(entry.jobId)) {
					entry.abortController.abort();
				}
			}
		} catch (err) {
			this.reportError(err, "heartbeat");
		}
	}

	private async sweep(): Promise<void> {
		try {
			await sweepStaleJobs(this.config.pool, this.config.sql);
		} catch (err) {
			this.config.onError(err, "sweep");
		}
	}

	async stop(drainTimeoutMs?: number): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;

		for (const timer of this.pollTimers.values()) {
			clearTimeout(timer);
		}
		this.pollTimers.clear();

		const timeout = drainTimeoutMs ?? this.config.drainTimeoutMs;
		const inFlightPromises = [...this.inFlight.values()].map((j) => j.promise);

		if (inFlightPromises.length > 0) {
			const drainPromise = Promise.allSettled(inFlightPromises);
			const timeoutPromise = new Promise<"timeout">((resolve) => {
				const t = setTimeout(() => resolve("timeout"), timeout);
				t.unref();
			});

			const result = await Promise.race([
				drainPromise.then(() => "drained" as const),
				timeoutPromise,
			]);

			if (result === "timeout") {
				// Snapshot before aborting — finally blocks will clear inFlight
				const stragglerIds = [...this.inFlight.values()].map(
					(j) => j.jobIdBigint,
				);

				for (const entry of this.inFlight.values()) {
					entry.abortController.abort();
				}

				// Brief grace period for handlers to react to abort
				const grace = new Promise<void>((r) => {
					const t = setTimeout(r, 1000);
					t.unref();
				});
				await Promise.race([
					Promise.allSettled([...this.inFlight.values()].map((j) => j.promise)),
					grace,
				]);

				if (stragglerIds.length > 0) {
					try {
						await withConnection(this.config.pool, async (connection) => {
							await connection.query(this.config.sql.DRAIN_RELEASE, [
								stragglerIds,
								this.config.workerId,
							]);
						});
					} catch {
						// best effort
					}
				}
			}
		}

		if (this.heartbeatTimer) {
			clearInterval(this.heartbeatTimer);
			this.heartbeatTimer = null;
		}
		if (this.sweepTimer) {
			clearInterval(this.sweepTimer);
			this.sweepTimer = null;
		}
	}

	get inFlightCount(): number {
		return this.inFlight.size;
	}

	get isStopped(): boolean {
		return this.stopped;
	}
}
