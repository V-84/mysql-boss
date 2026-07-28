import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { acquireConnection, withConnection } from "./connection.js";
import { SingletonCollisionError } from "./errors.js";
import type { DeadJob } from "./index.js";
import { withDeadlockRetry } from "./retry-util.js";
import {
	DLQ_DELETE,
	DLQ_INSERT,
	LIST_DEAD,
	REPLAY_DELETE,
	REPLAY_INSERT,
} from "./sql.js";
import { toUtcString } from "./util.js";

export async function deadLetterJob(
	pool: Pool,
	jobId: string,
	workerId: string,
	error: { message: string; stack?: string; at: string },
): Promise<boolean> {
	return withDeadlockRetry(async () => {
		const conn = await acquireConnection(pool);
		try {
			await conn.beginTransaction();

			const [result] = await conn.query<ResultSetHeader>(DLQ_INSERT, [
				JSON.stringify(error),
				jobId,
				workerId,
			]);

			if (result.affectedRows === 0) {
				await conn.commit();
				return false;
			}

			await conn.query(DLQ_DELETE, [jobId, workerId]);
			await conn.commit();
			return true;
		} catch (err) {
			await conn.rollback().catch(() => {});
			throw err;
		} finally {
			conn.release();
		}
	});
}

interface DeadRow extends RowDataPacket {
	id: string;
	queue: string;
	priority: number;
	payload: unknown;
	retry_count: number;
	created_at_unix: number | string;
	failed_at_unix: number | string;
	last_error: { message: string; stack?: string; at: string } | null;
}

export async function listDead(
	pool: Pool,
	queue: string,
	before: Date,
	after: Date,
	limit: number,
	offset: number,
): Promise<DeadJob[]> {
	return withConnection(pool, async (connection) => {
		const [rows] = await connection.query<DeadRow[]>(LIST_DEAD, [
			queue,
			toUtcString(after),
			toUtcString(before),
			limit,
			offset,
		]);
		return rows.map((r) => ({
			id: r.id,
			queue: r.queue,
			payload: r.payload,
			priority: r.priority,
			retryCount: r.retry_count,
			createdAt: new Date(Number(r.created_at_unix) * 1000),
			failedAt: new Date(Number(r.failed_at_unix) * 1000),
			lastError: r.last_error,
		}));
	});
}

const ER_DUP_ENTRY = 1062;

export async function replayDead(pool: Pool, ids: string[]): Promise<number> {
	return withDeadlockRetry(async () => {
		const conn = await acquireConnection(pool);
		try {
			await conn.beginTransaction();

			try {
				const bigIds = ids.map(BigInt);
				const [result] = await conn.query<ResultSetHeader>(REPLAY_INSERT, [
					bigIds,
				]);

				if (result.affectedRows > 0) {
					await conn.query(REPLAY_DELETE, [bigIds]);
				}

				await conn.commit();
				return result.affectedRows;
			} catch (err) {
				await conn.rollback();
				if (
					typeof err === "object" &&
					err !== null &&
					"errno" in err &&
					(err as { errno: number }).errno === ER_DUP_ENTRY
				) {
					throw new SingletonCollisionError(
						"Replay collides with a live singleton job",
					);
				}
				throw err;
			}
		} finally {
			conn.release();
		}
	});
}
