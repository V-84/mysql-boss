import type { Pool } from "mysql2/promise";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ActiveJob, JobHandler } from "../../src/index.js";
import { UNPREFIXED_SQL } from "../../src/sql.js";

const dependencies = vi.hoisted(() => ({
	claimJobs: vi.fn(),
	completeJob: vi.fn(),
	deadLetterJob: vi.fn(),
	failJob: vi.fn(),
	heartbeatOwnedJobs: vi.fn(),
	sweepStaleJobs: vi.fn(),
}));

vi.mock("../../src/archive.js", () => ({
	completeJob: dependencies.completeJob,
}));
vi.mock("../../src/claim.js", () => ({
	claimJobs: dependencies.claimJobs,
}));
vi.mock("../../src/dlq.js", () => ({
	deadLetterJob: dependencies.deadLetterJob,
}));
vi.mock("../../src/fail.js", () => ({
	failJob: dependencies.failJob,
}));
vi.mock("../../src/heartbeat.js", () => ({
	heartbeatOwnedJobs: dependencies.heartbeatOwnedJobs,
}));
vi.mock("../../src/sweep.js", () => ({
	sweepStaleJobs: dependencies.sweepStaleJobs,
}));

import { WorkerManager } from "../../src/worker.js";

const workerId = "8094c98c-16a4-4bba-9ba9-e29872db0874";
const job: ActiveJob = {
	id: "91",
	queue: "payments",
	payload: { amount: 10 },
	retryCount: 0,
	retryLimit: 2,
};

type WorkerInternals = {
	poll(queue: string): Promise<void>;
	heartbeat(): Promise<void>;
	sweep(): Promise<void>;
	reportError(error: unknown, context: string): void;
	registrations: Map<string, JobHandler>;
	inFlight: Map<
		string,
		{
			jobId: string;
			jobIdBigint: bigint;
			abortController: AbortController;
			promise: Promise<void>;
		}
	>;
};

function createWorker(
	onError = vi.fn(),
	overrides: Partial<ConstructorParameters<typeof WorkerManager>[0]> = {},
): WorkerManager {
	return new WorkerManager({
		pool: {} as Pool,
		workerId,
		pollIntervalMs: 60_000,
		batchSize: 2,
		concurrency: 2,
		leaseSeconds: 30,
		heartbeatSeconds: 10,
		sweepIntervalMs: 60_000,
		drainTimeoutMs: 20,
		onError,
		sql: UNPREFIXED_SQL,
		...overrides,
	});
}

function internals(worker: WorkerManager): WorkerInternals {
	return worker as unknown as WorkerInternals;
}

beforeEach(() => {
	dependencies.claimJobs.mockResolvedValue([]);
	dependencies.completeJob.mockResolvedValue(true);
	dependencies.deadLetterJob.mockResolvedValue(true);
	dependencies.failJob.mockResolvedValue(true);
	dependencies.heartbeatOwnedJobs.mockResolvedValue(new Set(["91"]));
	dependencies.sweepStaleJobs.mockResolvedValue(0);
});

afterEach(() => {
	vi.clearAllMocks();
});

describe("worker state-machine business logic", () => {
	it("rejects duplicate queue handlers and exposes stopped state", async () => {
		const worker = createWorker();
		const handler = vi.fn();

		expect(worker.inFlightCount).toBe(0);
		expect(worker.isStopped).toBe(false);
		worker.registerQueue("payments", handler);
		expect(() => worker.registerQueue("payments", handler)).toThrow(
			'Handler already registered for queue "payments"',
		);

		await worker.stop();
		expect(worker.isStopped).toBe(true);
		await expect(worker.stop()).resolves.toBeUndefined();
	});

	it("reports claim failures and keeps polling available", async () => {
		const pollError = new Error("claim failed");
		const onError = vi.fn();
		dependencies.claimJobs.mockRejectedValueOnce(pollError);
		const worker = createWorker(onError);

		worker.registerQueue("payments", vi.fn());
		await vi.waitFor(() => {
			expect(onError).toHaveBeenCalledWith(pollError, "poll");
		});

		await worker.stop();
	});

	it("contains handler-failure infrastructure errors and clears in-flight state", async () => {
		const persistenceError = new Error("retry update failed");
		const onError = vi.fn();
		dependencies.claimJobs.mockResolvedValueOnce([job]);
		dependencies.failJob.mockRejectedValueOnce(persistenceError);
		const worker = createWorker(onError);

		worker.registerQueue("payments", async () => {
			throw "non-Error rejection";
		});

		await vi.waitFor(() => {
			expect(onError).toHaveBeenCalledWith(persistenceError, "job");
			expect(worker.inFlightCount).toBe(0);
		});
		await worker.stop();
	});

	it("aborts only work whose heartbeat no longer proves lease ownership", async () => {
		dependencies.claimJobs.mockResolvedValueOnce([job]);
		dependencies.heartbeatOwnedJobs.mockResolvedValueOnce(new Set());
		let observedAbort = false;
		const worker = createWorker();

		worker.registerQueue("payments", async (_job, { signal }) => {
			await new Promise<void>((resolve) => {
				signal.addEventListener(
					"abort",
					() => {
						observedAbort = true;
						resolve();
					},
					{ once: true },
				);
			});
		});
		await vi.waitFor(() => expect(worker.inFlightCount).toBe(1));

		await internals(worker).heartbeat();

		await vi.waitFor(() => {
			expect(observedAbort).toBe(true);
			expect(worker.inFlightCount).toBe(0);
		});
		expect(dependencies.completeJob).not.toHaveBeenCalled();
		await worker.stop();
	});

	it("reports heartbeat and sweep infrastructure failures", async () => {
		const heartbeatError = new Error("heartbeat failed");
		const sweepError = new Error("sweep failed");
		const onError = vi.fn();
		const handlerDone = Promise.withResolvers<void>();
		dependencies.claimJobs.mockResolvedValueOnce([job]);
		dependencies.heartbeatOwnedJobs.mockRejectedValueOnce(heartbeatError);
		dependencies.sweepStaleJobs.mockRejectedValueOnce(sweepError);
		const worker = createWorker(onError);

		worker.registerQueue("payments", () => handlerDone.promise);
		await vi.waitFor(() => expect(worker.inFlightCount).toBe(1));
		await internals(worker).heartbeat();
		await internals(worker).sweep();

		expect(onError).toHaveBeenCalledWith(heartbeatError, "heartbeat");
		expect(onError).toHaveBeenCalledWith(sweepError, "sweep");
		handlerDone.resolve();
		await vi.waitFor(() => expect(worker.inFlightCount).toBe(0));
		await worker.stop();
	});

	it("does not let a throwing error reporter escape worker bookkeeping", () => {
		const worker = createWorker(() => {
			throw new Error("reporter failed");
		});

		expect(() =>
			internals(worker).reportError(new Error("worker failed"), "job"),
		).not.toThrow();
	});

	it("short-circuits polling when stopped, unregistered, or saturated", async () => {
		const stopped = createWorker();
		await stopped.stop();
		await internals(stopped).poll("payments");

		const unregistered = createWorker();
		await internals(unregistered).poll("payments");

		const saturated = createWorker(vi.fn(), { concurrency: 1 });
		internals(saturated).registrations.set("payments", vi.fn());
		internals(saturated).inFlight.set("payments/91", {
			jobId: "91",
			jobIdBigint: 91n,
			abortController: new AbortController(),
			promise: Promise.resolve(),
		});
		await internals(saturated).poll("payments");

		expect(dependencies.claimJobs).not.toHaveBeenCalled();
		internals(saturated).inFlight.clear();
		await saturated.stop();
	});
});
