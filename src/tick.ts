import type { Pool, RowDataPacket } from "mysql2/promise";
import type { EnqueueOptions } from "./index.js";
import { parseCron } from "./cron/parse.js";
import { nextOccurrence } from "./cron/next.js";
import {
	DB_NOW,
	TICK_SELECT,
	TICK_ENQUEUE,
	TICK_ADVANCE,
	SCHEDULE_UPSERT,
	SCHEDULE_DELETE,
} from "./sql.js";
import { withDeadlockRetry } from "./retry-util.js";

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
	next_run_at: Date;
}

interface DbNowRow extends RowDataPacket {
	db_now: Date;
}

export async function runTick(pool: Pool): Promise<number> {
	return withDeadlockRetry(async () => {
		const conn = await pool.getConnection();
		try {
			await conn.beginTransaction();

			const [nowRows] = await conn.query<DbNowRow[]>(DB_NOW);
			const dbNow = nowRows[0].db_now;

			const [schedules] = await conn.query<ScheduleRow[]>(TICK_SELECT);

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

				await conn.query(TICK_ENQUEUE, [
					sched.queue,
					priority,
					sched.payload ? JSON.stringify(sched.payload) : null,
					sched.name,
					sched.next_run_at,
					retryLimit,
					retryDelaySecs,
					retryBackoff,
				]);

				const fields = parseCron(sched.cron);
				const base =
					dbNow > sched.next_run_at ? dbNow : sched.next_run_at;
				const nextRun = nextOccurrence(fields, base, sched.timezone);

				await conn.query(TICK_ADVANCE, [nextRun, sched.name]);
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
): Promise<void> {
	const fields = parseCron(cron);
	const nextRun = nextOccurrence(fields, new Date(), timezone);

	await pool.query(SCHEDULE_UPSERT, [
		name,
		queue,
		cron,
		timezone,
		payload ? JSON.stringify(payload) : null,
		jobOptions ? JSON.stringify(jobOptions) : null,
		nextRun,
	]);
}

export async function deleteSchedule(
	pool: Pool,
	name: string,
): Promise<void> {
	await pool.query(SCHEDULE_DELETE, [name]);
}
