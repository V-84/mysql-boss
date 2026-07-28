import type { Pool, PoolConnection } from "mysql2/promise";
import { INIT_SESSION } from "./sql.js";

export async function acquireConnection(pool: Pool): Promise<PoolConnection> {
	const connection = await pool.getConnection();
	try {
		await connection.query(INIT_SESSION);
		return connection;
	} catch (error) {
		connection.release();
		throw error;
	}
}

export async function withConnection<T>(
	pool: Pool,
	fn: (connection: PoolConnection) => Promise<T>,
): Promise<T> {
	const connection = await acquireConnection(pool);
	try {
		return await fn(connection);
	} finally {
		connection.release();
	}
}
