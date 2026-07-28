import type { Pool, RowDataPacket } from "mysql2/promise";
import { acquireConnection, withConnection } from "./connection.js";
import { nextOccurrence } from "./cron/next.js";
import { parseCron } from "./cron/parse.js";
import type { EnqueueOptions } from "./index.js";
import { withDeadlockRetry } from "./retry-util.js";
import { type SqlStatements, UNPREFIXED_SQL } from "./sql.js";
import { toUtcString } from "./util.js";

interface ScheduleRow extends RowDataPacket {
	name: string;
	queue: string;
	cron: string;
	timezone: string;
	payload: unknown;
	job_options: {
		priority?: number;
		retryLimit?: number;
		retryDelaySecs?: number;
		retryBackoff?: boolean;
	} | null;
	next_run_at_unix: number | string;
}

interface DbNowRow extends RowDataPacket {
	db_now_unix: number | string;
}

export async function runTick(
	pool: Pool,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<number> {
	return withDeadlockRetry(async () => {
		const conn = await acquireConnection(pool);
		try {
			await conn.beginTransaction();

			const [nowRows] = await conn.query<DbNowRow[]>(sql.DB_NOW);
			const dbNow = new Date(Number(nowRows[0].db_now_unix) * 1000);

			const [schedules] = await conn.query<ScheduleRow[]>(sql.TICK_SELECT);

			if (schedules.length === 0) {
				await conn.commit();
				return 0;
			}

			let fired = 0;
			for (const sched of schedules) {
				const opts = sched.job_options ?? {};
				const priority = opts.priority ?? 0;
				const retryLimit = opts.retryLimit ?? 2;
				const retryDelaySecs = opts.retryDelaySecs ?? 30;
				const retryBackoff = opts.retryBackoff ? 1 : 0;

				await conn.query(sql.TICK_ENQUEUE, [
					sched.queue,
					priority,
					sched.payload != null ? JSON.stringify(sched.payload) : null,
					sched.name,
					toUtcString(new Date(Number(sched.next_run_at_unix) * 1000)),
					retryLimit,
					retryDelaySecs,
					retryBackoff,
				]);

				const fields = parseCron(sched.cron);
				const nextRunAt = new Date(Number(sched.next_run_at_unix) * 1000);
				const base = dbNow > nextRunAt ? dbNow : nextRunAt;
				const nextRun = nextOccurrence(fields, base, sched.timezone);

				await conn.query(sql.TICK_ADVANCE, [toUtcString(nextRun), sched.name]);
				fired++;
			}

			await conn.commit();
			return fired;
		} catch (err) {
			await conn.rollback().catch(() => {});
			throw err;
		} finally {
			conn.release();
		}
	});
}

export async function upsertSchedule(
	pool: Pool,
	name: string,
	queue: string,
	cron: string,
	timezone: string,
	payload: unknown,
	jobOptions: {
		priority?: number;
		retryLimit?: number;
		retryDelaySecs?: number;
		retryBackoff?: boolean;
	} | null,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<void> {
	const fields = parseCron(cron);
	await withConnection(pool, async (connection) => {
		const [nowRows] = await connection.query<DbNowRow[]>(sql.DB_NOW);
		const dbNow = new Date(Number(nowRows[0].db_now_unix) * 1000);
		const nextRun = nextOccurrence(fields, dbNow, timezone);

		await connection.query(sql.SCHEDULE_UPSERT, [
			name,
			queue,
			cron,
			timezone,
			payload != null ? JSON.stringify(payload) : null,
			jobOptions ? JSON.stringify(jobOptions) : null,
			toUtcString(nextRun),
		]);
	});
}

export async function deleteSchedule(
	pool: Pool,
	name: string,
	sql: SqlStatements = UNPREFIXED_SQL,
): Promise<void> {
	await withConnection(pool, async (connection) => {
		await connection.query(sql.SCHEDULE_DELETE, [name]);
	});
}
