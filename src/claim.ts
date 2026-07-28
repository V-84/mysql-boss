import type { Pool, RowDataPacket } from "mysql2/promise";
import type { ActiveJob } from "./index.js";
import { CLAIM_SELECT, CLAIM_UPDATE } from "./sql.js";
import { withDeadlockRetry } from "./retry-util.js";

interface ClaimRow extends RowDataPacket {
	id: bigint;
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
		const conn = await pool.getConnection();
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
			await conn.query(CLAIM_UPDATE, [workerId, leaseSeconds, [ids]]);

			await conn.commit();

			return rows.map((r) => ({
				id: r.id.toString(),
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
