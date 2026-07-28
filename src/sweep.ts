import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { SWEEP_DLQ_DELETE, SWEEP_DLQ_INSERT, SWEEP_RETRY, SWEEP_SELECT } from "./sql.js";
import { withDeadlockRetry } from "./retry-util.js";

interface SweepRow extends RowDataPacket {
	id: bigint;
	retry_count: number;
	retry_limit: number;
}

export async function sweepStaleJobs(pool: Pool): Promise<number> {
	return withDeadlockRetry(async () => {
		const conn = await pool.getConnection();
		try {
			await conn.beginTransaction();

			const [rows] = await conn.query<SweepRow[]>(SWEEP_SELECT);

			if (rows.length === 0) {
				await conn.commit();
				return 0;
			}

			const retryable: bigint[] = [];
			const exhausted: bigint[] = [];

			for (const row of rows) {
				if (row.retry_count < row.retry_limit) {
					retryable.push(row.id);
				} else {
					exhausted.push(row.id);
				}
			}

			if (retryable.length > 0) {
				await conn.query(SWEEP_RETRY, [[retryable]]);
			}

			if (exhausted.length > 0) {
				await conn.query(SWEEP_DLQ_INSERT, [[exhausted]]);
				await conn.query(SWEEP_DLQ_DELETE, [[exhausted]]);
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
