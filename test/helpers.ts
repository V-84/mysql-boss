import mysql from "mysql2/promise";
import type { Pool } from "mysql2/promise";
import { MysqlBoss } from "../src/index.js";

const DB_CONFIG = {
	host: process.env.MYSQL_HOST ?? "127.0.0.1",
	port: Number(process.env.MYSQL_PORT ?? 3307),
	user: process.env.MYSQL_USER ?? "root",
	password: process.env.MYSQL_PASSWORD ?? "test",
	database: process.env.MYSQL_DATABASE ?? "testdb",
};

export async function createPool(): Promise<Pool> {
	return mysql.createPool({
		...DB_CONFIG,
		waitForConnections: true,
		connectionLimit: 20,
		multipleStatements: true,
	});
}

export async function createBoss(
	pool: Pool,
	overrides?: Partial<ConstructorParameters<typeof MysqlBoss>[0]>,
): Promise<MysqlBoss> {
	const boss = new MysqlBoss({
		pool,
		pollIntervalMs: 200,
		batchSize: 10,
		leaseSeconds: 30,
		heartbeatSeconds: 10,
		sweepIntervalMs: 2000,
		tickIntervalMs: 2000,
		archiveRetentionDays: 14,
		drainTimeoutMs: 5000,
		...overrides,
	});
	await boss.migrate();
	return boss;
}

export async function cleanTables(pool: Pool): Promise<void> {
	await pool.query("DELETE FROM jobs");
	await pool.query("DELETE FROM jobs_archive");
	await pool.query("DELETE FROM jobs_dead");
	await pool.query("DELETE FROM schedules");
}
