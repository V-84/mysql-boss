import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { acquireConnection } from "./connection.js";
import { withDeadlockRetry } from "./retry-util.js";
import { type SqlStatements, UNPREFIXED_SQL } from "./sql.js";

interface SweepRow extends RowDataPacket {
	id: string;
	retry_count: number;
	retry_limit: number;
}

export async function sweepStaleJobs(
	pool: Pool,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<number> {
	return withDeadlockRetry(async () => {
		const conn = await acquireConnection(pool);
		try {
			await conn.beginTransaction();

			const [rows] = await conn.query<SweepRow[]>(sql.SWEEP_SELECT);

			if (rows.length === 0) {
				await conn.commit();
				return 0;
			}

			const retryable: string[] = [];
			const exhausted: string[] = [];

			for (const row of rows) {
				if (row.retry_count < row.retry_limit) {
					retryable.push(row.id);
				} else {
					exhausted.push(row.id);
				}
			}

			if (retryable.length > 0) {
				await conn.query(sql.SWEEP_RETRY, [retryable]);
			}

			if (exhausted.length > 0) {
				await conn.query(sql.SWEEP_DLQ_INSERT, [exhausted]);
				await conn.query(sql.SWEEP_DLQ_DELETE, [exhausted]);
			}

			await conn.commit();
			return rows.length;
		} catch (err) {
			await conn.rollback().catch(() => {});
			throw err;
		} finally {
			conn.release();
		}
	});
}
