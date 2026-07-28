import type { CronFields } from "./parse.js";

function getLocalParts(
	dt: Date,
	tz: string,
): {
	year: number;
	month: number;
	day: number;
	hour: number;
	minute: number;
	dow: number;
} {
	const fmt = new Intl.DateTimeFormat("en-US", {
		timeZone: tz,
		year: "numeric",
		month: "numeric",
		day: "numeric",
		hour: "numeric",
		minute: "numeric",
		weekday: "short",
		hour12: false,
	});
	const parts = fmt.formatToParts(dt);
	const get = (type: Intl.DateTimeFormatPartTypes) =>
		Number(parts.find((p) => p.type === type)?.value ?? 0);

	const dowMap: Record<string, number> = {
		Sun: 0,
		Mon: 1,
		Tue: 2,
		Wed: 3,
		Thu: 4,
		Fri: 5,
		Sat: 6,
	};
	const dowStr = parts.find((p) => p.type === "weekday")?.value ?? "Sun";

	return {
		year: get("year"),
		month: get("month"),
		day: get("day"),
		hour: get("hour") === 24 ? 0 : get("hour"),
		minute: get("minute"),
		dow: dowMap[dowStr] ?? 0,
	};
}

function localToUtc(
	year: number,
	month: number,
	day: number,
	hour: number,
	minute: number,
	tz: string,
): Date | null {
	const guess = new Date(Date.UTC(year, month - 1, day, hour, minute));

	for (let offset = -14 * 60; offset <= 14 * 60; offset += 15) {
		const candidate = new Date(guess.getTime() + offset * 60_000);
		const parts = getLocalParts(candidate, tz);
		if (
			parts.year === year &&
			parts.month === month &&
			parts.day === day &&
			parts.hour === hour &&
			parts.minute === minute
		) {
			return candidate;
		}
	}
	return null;
}

function daysInMonth(year: number, month: number): number {
	return new Date(year, month, 0).getDate();
}

export function nextOccurrence(
	fields: CronFields,
	after: Date,
	tz: string,
	maxIterations = 366 * 24 * 60,
): Date {
	const local = getLocalParts(after, tz);
	let { year, month, day, hour, minute } = local;

	minute += 1;
	if (minute > 59) {
		minute = 0;
		hour += 1;
	}
	if (hour > 23) {
		hour = 0;
		day += 1;
	}
	const dim = daysInMonth(year, month);
	if (day > dim) {
		day = 1;
		month += 1;
	}
	if (month > 12) {
		month = 1;
		year += 1;
	}

	for (let i = 0; i < maxIterations; i++) {
		if (!fields.months.has(month)) {
			month += 1;
			day = 1;
			hour = 0;
			minute = 0;
			if (month > 12) {
				month = 1;
				year += 1;
			}
			continue;
		}

		const dim = daysInMonth(year, month);
		if (day > dim) {
			month += 1;
			day = 1;
			hour = 0;
			minute = 0;
			if (month > 12) {
				month = 1;
				year += 1;
			}
			continue;
		}

		const candidateUtc = localToUtc(year, month, day, hour, minute, tz);
		if (candidateUtc === null) {
			// DST gap — this local time doesn't exist; skip forward
			minute += 1;
			if (minute > 59) {
				minute = 0;
				hour += 1;
			}
			if (hour > 23) {
				hour = 0;
				day += 1;
			}
			continue;
		}

		const actualParts = getLocalParts(candidateUtc, tz);
		const dow = actualParts.dow;

		const domMatch = fields.daysOfMonth.has(day);
		const dowMatch = fields.daysOfWeek.has(dow);
		// Vixie cron OR semantics: if both DOM and DOW are restricted, match either
		const dayMatch =
			fields.hasDom && fields.hasDow
				? domMatch || dowMatch
				: fields.hasDom
					? domMatch
					: fields.hasDow
						? dowMatch
						: true;

		if (dayMatch && fields.hours.has(hour) && fields.minutes.has(minute)) {
			return candidateUtc;
		}

		if (!dayMatch) {
			day += 1;
			hour = 0;
			minute = 0;
			if (day > daysInMonth(year, month)) {
				day = 1;
				month += 1;
				if (month > 12) {
					month = 1;
					year += 1;
				}
			}
			continue;
		}

		if (!fields.hours.has(hour)) {
			hour += 1;
			minute = 0;
			if (hour > 23) {
				hour = 0;
				day += 1;
				if (day > daysInMonth(year, month)) {
					day = 1;
					month += 1;
					if (month > 12) {
						month = 1;
						year += 1;
					}
				}
			}
			continue;
		}

		minute += 1;
		if (minute > 59) {
			minute = 0;
			hour += 1;
			if (hour > 23) {
				hour = 0;
				day += 1;
				if (day > daysInMonth(year, month)) {
					day = 1;
					month += 1;
					if (month > 12) {
						month = 1;
						year += 1;
					}
				}
			}
		}
	}

	throw new Error("Could not find next cron occurrence within search window");
}
