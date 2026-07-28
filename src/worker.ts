import type { Pool } from "mysql2/promise";
import { completeJob } from "./archive.js";
import { claimJobs } from "./claim.js";
import { deadLetterJob } from "./dlq.js";
import { failJob } from "./fail.js";
import { sendHeartbeat } from "./heartbeat.js";
import type { ActiveJob, JobHandler } from "./index.js";
import { DRAIN_RELEASE } from "./sql.js";
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
	private inFlight = new Map<string, InFlightJob>();
	private pollTimers = new Map<string, ReturnType<typeof setTimeout>>();
	private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
	private sweepTimer: ReturnType<typeof setInterval> | null = null;
	private stopped = false;

	constructor(config: WorkerConfig) {
		this.config = config;
	}

	registerQueue(queue: string, handler: JobHandler): void {
		if (this.registrations.has(queue)) {
			throw new Error(`Handler already registered for queue "${queue}"`);
		}
		this.registrations.set(queue, handler);
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
		if (!this.sweepTimer) {
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

		const available = this.config.concurrency - queueInFlight;
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
			);

			for (const job of jobs) {
				this.dispatch(queue, job, handler);
			}

			if (jobs.length >= batch && !this.stopped) {
				setImmediate(() => this.poll(queue));
				return;
			}
		} catch {
			// claim error — back off
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

		const promise = (async () => {
			try {
				await handler(job, { signal: ac.signal });
				if (!ac.signal.aborted) {
					await completeJob(this.config.pool, job.id, this.config.workerId);
				}
			} catch (err) {
				if (ac.signal.aborted) return;
				const errorObj = {
					message: err instanceof Error ? err.message : String(err),
					stack: err instanceof Error ? err.stack : undefined,
					at: new Date().toISOString(),
				};

				const retried = await failJob(
					this.config.pool,
					job.id,
					this.config.workerId,
					errorObj,
				);

				if (!retried) {
					await deadLetterJob(
						this.config.pool,
						job.id,
						this.config.workerId,
						errorObj,
					);
				}
			} finally {
				this.inFlight.delete(key);
			}
		})();

		this.inFlight.set(key, {
			jobId: job.id,
			jobIdBigint: BigInt(job.id),
			abortController: ac,
			promise,
		});
	}

	private async heartbeat(): Promise<void> {
		if (this.inFlight.size === 0) return;

		const ids = [...this.inFlight.values()].map((j) => j.jobIdBigint);
		try {
			await sendHeartbeat(
				this.config.pool,
				ids,
				this.config.workerId,
				this.config.leaseSeconds,
			);
		} catch {
			// heartbeat failure is not fatal
		}
	}

	private async sweep(): Promise<void> {
		try {
			await sweepStaleJobs(this.config.pool);
		} catch {
			// sweep failure is not fatal
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
				for (const entry of this.inFlight.values()) {
					entry.abortController.abort();
				}

				await Promise.allSettled(
					[...this.inFlight.values()].map((j) => j.promise),
				);

				const stragglers = [...this.inFlight.values()];
				if (stragglers.length > 0) {
					const ids = stragglers.map((j) => j.jobIdBigint);
					try {
						await this.config.pool.query(DRAIN_RELEASE, [
							ids,
							this.config.workerId,
						]);
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
