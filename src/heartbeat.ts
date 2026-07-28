import type { Pool, ResultSetHeader } from "mysql2/promise";
import { HEARTBEAT } from "./sql.js";

export async function sendHeartbeat(
	pool: Pool,
	jobIds: bigint[],
	workerId: string,
	leaseSeconds: number,
): Promise<number> {
	if (jobIds.length === 0) return 0;
	const [result] = await pool.query<ResultSetHeader>(HEARTBEAT, [
		leaseSeconds,
		jobIds,
		workerId,
	]);
	return result.affectedRows;
}
