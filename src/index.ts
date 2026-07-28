import type { Pool } from "mysql2/promise";

export interface MysqlBossOptions {
	pool: Pool;
	pollIntervalMs?: number;
	batchSize?: number;
	concurrency?: number;
	leaseSeconds?: number;
	heartbeatSeconds?: number;
	sweepIntervalMs?: number;
	tickIntervalMs?: number;
	archiveRetentionDays?: number;
	drainTimeoutMs?: number;
}

export interface EnqueueOptions {
	priority?: number;
	singletonKey?: string;
	retryLimit?: number;
	retryDelaySecs?: number;
	retryBackoff?: boolean;
	runAt?: Date;
}

export interface ActiveJob<T = unknown> {
	id: string;
	queue: string;
	payload: T;
	retryCount: number;
	retryLimit: number;
}

export type JobHandler<T = unknown> = (
	job: ActiveJob<T>,
	ctx: { signal: AbortSignal },
) => Promise<void>;

export interface DeadJob<T = unknown> {
	id: string;
	queue: string;
	payload: T;
	priority: number;
	retryCount: number;
	createdAt: Date;
	failedAt: Date;
	lastError: { message: string; stack?: string; at: string } | null;
}

export interface ArchivedJob<T = unknown> {
	id: string;
	queue: string;
	payload: T;
	priority: number;
	retryCount: number;
	createdAt: Date;
	startedAt: Date;
	completedAt: Date;
	durationMs: number;
}

export interface ScheduleOptions
	extends Omit<EnqueueOptions, "singletonKey" | "runAt"> {
	timezone?: string;
	payload?: unknown;
}

export { MysqlBoss } from "./boss.js";
export { ConfigError, ValidationError, SingletonCollisionError } from "./errors.js";
