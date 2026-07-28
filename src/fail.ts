import type { Pool, ResultSetHeader } from "mysql2/promise";
import { FAIL_RETRY } from "./sql.js";
import { withDeadlockRetry } from "./retry-util.js";

export async function failJob(
	pool: Pool,
	jobId: string,
	workerId: string,
	error: { message: string; stack?: string; at: string },
): Promise<boolean> {
	return withDeadlockRetry(async () => {
		const [result] = await pool.query<ResultSetHeader>(FAIL_RETRY, [
			JSON.stringify(error),
			jobId,
			workerId,
		]);
		return result.affectedRows > 0;
	});
}
