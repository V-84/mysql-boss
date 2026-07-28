import type { Pool } from "mysql2/promise";
import {
	CREATE_JOBS,
	CREATE_JOBS_ARCHIVE,
	CREATE_JOBS_DEAD,
	CREATE_SCHEDULES,
} from "./sql.js";

export async function migrate(pool: Pool): Promise<void> {
	await pool.query(CREATE_JOBS);
	await pool.query(CREATE_JOBS_ARCHIVE);
	await pool.query(CREATE_JOBS_DEAD);
	await pool.query(CREATE_SCHEDULES);
}
