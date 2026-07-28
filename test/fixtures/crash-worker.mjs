import mysql from "mysql2/promise";
import { MysqlBoss } from "../../dist/esm/index.js";

const pool = mysql.createPool({
	host: process.env.MYSQL_HOST,
	port: Number(process.env.MYSQL_PORT),
	user: process.env.MYSQL_USER,
	password: process.env.MYSQL_PASSWORD,
	database: process.env.MYSQL_DATABASE,
	waitForConnections: true,
	connectionLimit: 4,
});

const boss = new MysqlBoss({
	pool,
	pollIntervalMs: 20,
	batchSize: 1,
	concurrency: 1,
	leaseSeconds: 3,
	heartbeatSeconds: 1,
	sweepIntervalMs: 60_000,
	tickIntervalMs: 60_000,
});

await boss.migrate();
boss.work(process.env.CRASH_QUEUE, async (job) => {
	process.send?.({ type: "claimed", jobId: job.id });
	await new Promise(() => {});
});

process.send?.({ type: "ready" });
