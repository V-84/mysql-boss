import type { Pool, RowDataPacket } from "mysql2/promise";
import { withConnection } from "./connection.js";
import { type SqlStatements, UNPREFIXED_SQL } from "./sql.js";

interface OwnedRow extends RowDataPacket {
	id: string;
}

export async function heartbeatOwnedJobs(
	pool: Pool,
	jobIds: bigint[],
	workerId: string,
	leaseSeconds: number,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<Set<string>> {
	if (jobIds.length === 0) return new Set();
	return withConnection(pool, async (connection) => {
		await connection.query(sql.HEARTBEAT, [leaseSeconds, jobIds, workerId]);
		const [rows] = await connection.query<OwnedRow[]>(sql.HEARTBEAT_OWNED, [
			jobIds,
			workerId,
		]);
		return new Set(rows.map((row) => row.id));
	});
}
