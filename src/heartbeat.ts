import type { Pool, RowDataPacket } from "mysql2/promise";
import { withConnection } from "./connection.js";
import { HEARTBEAT, HEARTBEAT_OWNED } from "./sql.js";

interface OwnedRow extends RowDataPacket {
	id: string;
}

export async function heartbeatOwnedJobs(
	pool: Pool,
	jobIds: bigint[],
	workerId: string,
	leaseSeconds: number,
): Promise<Set<string>> {
	if (jobIds.length === 0) return new Set();
	return withConnection(pool, async (connection) => {
		await connection.query(HEARTBEAT, [leaseSeconds, jobIds, workerId]);
		const [rows] = await connection.query<OwnedRow[]>(HEARTBEAT_OWNED, [
			jobIds,
			workerId,
		]);
		return new Set(rows.map((row) => row.id));
	});
}
