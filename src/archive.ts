import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { acquireConnection, withConnection } from "./connection.js";
import type { ArchivedJob } from "./index.js";
import { withDeadlockRetry } from "./retry-util.js";
import { type SqlStatements, UNPREFIXED_SQL } from "./sql.js";
import { toUtcString } from "./util.js";

export async function completeJob(
	pool: Pool,
	jobId: string,
	workerId: string,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<boolean> {
	return withDeadlockRetry(async () => {
		const conn = await acquireConnection(pool);
		try {
			await conn.beginTransaction();

			const [result] = await conn.query<ResultSetHeader>(sql.COMPLETE_ARCHIVE, [
				jobId,
				workerId,
			]);

			if (result.affectedRows === 0) {
				await conn.commit();
				return false;
			}

			await conn.query(sql.COMPLETE_DELETE, [jobId, workerId]);

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
	id: string;
	queue: string;
	priority: number;
	payload: unknown;
	singleton_key: string | null;
	retry_count: number;
	created_at_unix: number | string;
	started_at_unix: number | string;
	completed_at_unix: number | string;
	duration_ms: number;
}

function unixDate(value: number | string): Date {
	return new Date(Number(value) * 1000);
}

function mapArchiveRow(row: ArchiveRow): ArchivedJob {
	return {
		id: row.id,
		queue: row.queue,
		payload: row.payload,
		priority: row.priority,
		retryCount: row.retry_count,
		createdAt: unixDate(row.created_at_unix),
		startedAt: unixDate(row.started_at_unix),
		completedAt: unixDate(row.completed_at_unix),
		durationMs: row.duration_ms,
	};
}

export async function getArchivedJob(
	pool: Pool,
	id: string,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<ArchivedJob | null> {
	return withConnection(pool, async (connection) => {
		const [rows] = await connection.query<ArchiveRow[]>(sql.GET_ARCHIVED_JOB, [
			id,
		]);
		if (rows.length === 0) return null;
		return mapArchiveRow(rows[0]);
	});
}

export async function listArchive(
	pool: Pool,
	queue: string,
	before: Date,
	limit: number,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<ArchivedJob[]> {
	return withConnection(pool, async (connection) => {
		const [rows] = await connection.query<ArchiveRow[]>(sql.LIST_ARCHIVE, [
			queue,
			toUtcString(before),
			limit,
		]);
		return rows.map(mapArchiveRow);
	});
}

export async function pruneArchive(
	pool: Pool,
	retentionDays: number,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<void> {
	await withConnection(pool, async (connection) => {
		let deleted: number;
		do {
			const [result] = await connection.query<ResultSetHeader>(
				sql.ARCHIVE_PRUNE,
				[retentionDays],
			);
			deleted = result.affectedRows;
		} while (deleted >= 5000);
	});
}
