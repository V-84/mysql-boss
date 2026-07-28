import type { Pool } from "mysql2/promise";
import { withConnection } from "./connection.js";
import { type SqlStatements, UNPREFIXED_SQL } from "./sql.js";

export async function migrate(
	pool: Pool,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<void> {
	await withConnection(pool, async (connection) => {
		await connection.query(sql.CREATE_JOBS);
		await connection.query(sql.CREATE_JOBS_ARCHIVE);
		await connection.query(sql.CREATE_JOBS_DEAD);
		await connection.query(sql.CREATE_SCHEDULES);
	});
}
