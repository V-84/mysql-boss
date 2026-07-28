export interface CronFields {
	minutes: Set<number>;
	hours: Set<number>;
	daysOfMonth: Set<number>;
	months: Set<number>;
	daysOfWeek: Set<number>;
	hasDom: boolean;
	hasDow: boolean;
}

const MONTH_NAMES: Record<string, number> = {
	jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
	jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

const DOW_NAMES: Record<string, number> = {
	sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6,
};

function parseField(
	field: string,
	min: number,
	max: number,
	names?: Record<string, number>,
): Set<number> {
	const result = new Set<number>();

	for (const part of field.split(",")) {
		let stepParts = part.split("/");
		const step = stepParts.length > 1 ? Number.parseInt(stepParts[1], 10) : 1;
		if (Number.isNaN(step) || step < 1) {
			throw new Error(`Invalid step in cron field: ${part}`);
		}

		let rangePart = stepParts[0];

		if (rangePart === "*") {
			for (let i = min; i <= max; i += step) {
				result.add(i);
			}
			continue;
		}

		const resolve = (val: string): number => {
			if (names) {
				const lower = val.toLowerCase();
				if (lower in names) return names[lower];
			}
			const n = Number.parseInt(val, 10);
			if (Number.isNaN(n) || n < min || n > max) {
				throw new Error(`Value out of range [${min}-${max}]: ${val}`);
			}
			return n;
		};

		if (rangePart.includes("-")) {
			const [startStr, endStr] = rangePart.split("-");
			const start = resolve(startStr);
			const end = resolve(endStr);
			if (start > end) {
				// Wrap-around for dow
				for (let i = start; i <= max; i += step) result.add(i);
				for (let i = min; i <= end; i += step) result.add(i);
			} else {
				for (let i = start; i <= end; i += step) result.add(i);
			}
		} else {
			result.add(resolve(rangePart));
		}
	}

	return result;
}

export function parseCron(expression: string): CronFields {
	const parts = expression.trim().split(/\s+/);
	if (parts.length !== 5) {
		throw new Error(
			`Cron expression must have exactly 5 fields, got ${parts.length}: "${expression}"`,
		);
	}

	const [minStr, hourStr, domStr, monStr, dowStr] = parts;

	return {
		minutes: parseField(minStr, 0, 59),
		hours: parseField(hourStr, 0, 23),
		daysOfMonth: parseField(domStr, 1, 31, undefined),
		months: parseField(monStr, 1, 12, MONTH_NAMES),
		daysOfWeek: parseField(dowStr, 0, 6, DOW_NAMES),
		hasDom: domStr !== "*",
		hasDow: dowStr !== "*",
	};
}
