import type { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { acquireConnection } from "./connection.js";
import type { ActiveJob } from "./index.js";
import { withDeadlockRetry } from "./retry-util.js";
import { CLAIM_SELECT, CLAIM_UPDATE } from "./sql.js";

interface ClaimRow extends RowDataPacket {
	id: string;
	queue: string;
	payload: unknown;
	retry_count: number;
	retry_limit: number;
	retry_delay_secs: number;
	retry_backoff: number;
}

export async function claimJobs(
	pool: Pool,
	queue: string,
	workerId: string,
	batchSize: number,
	leaseSeconds: number,
): Promise<ActiveJob[]> {
	return withDeadlockRetry(async () => {
		const conn = await acquireConnection(pool);
		try {
			await conn.beginTransaction();

			const [rows] = await conn.query<ClaimRow[]>(CLAIM_SELECT, [
				queue,
				batchSize,
			]);

			if (rows.length === 0) {
				await conn.commit();
				return [];
			}

			const ids = rows.map((r) => r.id);
			const [result] = await conn.query<ResultSetHeader>(CLAIM_UPDATE, [
				workerId,
				leaseSeconds,
				ids,
			]);
			if (result.affectedRows !== rows.length) {
				throw new Error(
					`Claim invariant violated: selected ${rows.length} jobs but updated ${result.affectedRows}`,
				);
			}

			await conn.commit();

			return rows.map((r) => ({
				id: r.id,
				queue: r.queue,
				payload: r.payload,
				retryCount: r.retry_count,
				retryLimit: r.retry_limit,
			}));
		} catch (err) {
			await conn.rollback().catch(() => {});
			throw err;
		} finally {
			conn.release();
		}
	});
}
