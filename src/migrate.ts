import type { Pool } from "mysql2/promise";
import { withConnection } from "./connection.js";
import {
	CREATE_JOBS,
	CREATE_JOBS_ARCHIVE,
	CREATE_JOBS_DEAD,
	CREATE_SCHEDULES,
} from "./sql.js";

export async function migrate(pool: Pool): Promise<void> {
	await withConnection(pool, async (connection) => {
		await connection.query(CREATE_JOBS);
		await connection.query(CREATE_JOBS_ARCHIVE);
		await connection.query(CREATE_JOBS_DEAD);
		await connection.query(CREATE_SCHEDULES);
	});
}
