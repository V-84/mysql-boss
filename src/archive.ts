import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import type { ArchivedJob } from "./index.js";
import {
	ARCHIVE_PRUNE,
	COMPLETE_ARCHIVE,
	COMPLETE_DELETE,
	GET_ARCHIVED_JOB,
	LIST_ARCHIVE,
} from "./sql.js";
import { withDeadlockRetry } from "./retry-util.js";

export async function completeJob(
	pool: Pool,
	jobId: string,
	workerId: string,
): Promise<boolean> {
	return withDeadlockRetry(async () => {
		const conn = await pool.getConnection();
		try {
			await conn.beginTransaction();

			const [result] = await conn.query<ResultSetHeader>(COMPLETE_ARCHIVE, [
				jobId,
				workerId,
			]);

			if (result.affectedRows === 0) {
				await conn.commit();
				return false;
			}

			await conn.query(COMPLETE_DELETE, [jobId, workerId]);

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

interface ArchiveRow extends RowDataPacket {
	id: bigint;
	queue: string;
	priority: number;
	payload: unknown;
	singleton_key: string | null;
	retry_count: number;
	created_at: Date;
	started_at: Date;
	completed_at: Date;
	duration_ms: number;
}

function mapArchiveRow(row: ArchiveRow): ArchivedJob {
	return {
		id: row.id.toString(),
		queue: row.queue,
		payload: row.payload,
		priority: row.priority,
		retryCount: row.retry_count,
		createdAt: row.created_at,
		startedAt: row.started_at,
		completedAt: row.completed_at,
		durationMs: row.duration_ms,
	};
}

export async function getArchivedJob(
	pool: Pool,
	id: string,
): Promise<ArchivedJob | null> {
	const [rows] = await pool.query<ArchiveRow[]>(GET_ARCHIVED_JOB, [id]);
	if (rows.length === 0) return null;
	return mapArchiveRow(rows[0]);
}

export async function listArchive(
	pool: Pool,
	queue: string,
	before: Date,
	limit: number,
): Promise<ArchivedJob[]> {
	const [rows] = await pool.query<ArchiveRow[]>(LIST_ARCHIVE, [
		queue,
		before,
		limit,
	]);
	return rows.map(mapArchiveRow);
}

export async function pruneArchive(
	pool: Pool,
	retentionDays: number,
): Promise<void> {
	let deleted: number;
	do {
		const [result] = await pool.query<ResultSetHeader>(ARCHIVE_PRUNE, [
			retentionDays,
		]);
		deleted = result.affectedRows;
	} while (deleted >= 5000);
}
