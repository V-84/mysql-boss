import type { Pool, ResultSetHeader } from "mysql2/promise";
import { withConnection } from "./connection.js";
import { withDeadlockRetry } from "./retry-util.js";
import { FAIL_RETRY } from "./sql.js";

export async function failJob(
	pool: Pool,
	jobId: string,
	workerId: string,
	error: { message: string; stack?: string; at: string },
): Promise<boolean> {
	return withDeadlockRetry(async () => {
		return withConnection(pool, async (connection) => {
			const [result] = await connection.query<ResultSetHeader>(FAIL_RETRY, [
				JSON.stringify(error),
				jobId,
				workerId,
			]);
			return result.affectedRows > 0;
		});
	});
}
