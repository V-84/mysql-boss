import { describe, expect, it } from "vitest";
import { nextOccurrence } from "../../src/cron/next.js";
import { parseCron } from "../../src/cron/parse.js";

function next(cron: string, after: string, timezone = "UTC"): string {
	return nextOccurrence(
		parseCron(cron),
		new Date(after),
		timezone,
	).toISOString();
}

describe("cron calendar boundary business logic", () => {
	it.each([
		{
			name: "minute, day, month, and year rollover",
			cron: "0 0 1 1 *",
			after: "2026-12-31T23:59:00.000Z",
			expected: "2027-01-01T00:00:00.000Z",
		},
		{
			name: "invalid day after initial minute rollover",
			cron: "0 0 1 5 *",
			after: "2026-04-30T23:59:00.000Z",
			expected: "2026-05-01T00:00:00.000Z",
		},
		{
			name: "restricted months",
			cron: "0 0 1 3 *",
			after: "2026-01-01T00:00:00.000Z",
			expected: "2026-03-01T00:00:00.000Z",
		},
		{
			name: "day mismatch across a month boundary",
			cron: "0 0 1 * *",
			after: "2026-01-01T00:00:00.000Z",
			expected: "2026-02-01T00:00:00.000Z",
		},
		{
			name: "hour mismatch across a year boundary",
			cron: "0 0 * * *",
			after: "2026-12-31T22:59:00.000Z",
			expected: "2027-01-01T00:00:00.000Z",
		},
		{
			name: "minute mismatch across a year boundary",
			cron: "0 23 * * *",
			after: "2026-12-31T23:58:00.000Z",
			expected: "2027-01-01T23:00:00.000Z",
		},
	])("finds the next occurrence through $name", ({ cron, after, expected }) => {
		expect(next(cron, after)).toBe(expected);
	});

	it("uses Vixie OR semantics when day-of-month and day-of-week are restricted", () => {
		expect(next("0 9 15 * mon", "2026-07-12T10:00:00.000Z")).toBe(
			"2026-07-13T09:00:00.000Z",
		);
	});

	it("handles named fields, steps, wraparound ranges, and Sunday alias 7", () => {
		const fields = parseCron("*/20 9-11/2 * jan-mar/2 fri-mon");
		expect([...fields.minutes]).toEqual([0, 20, 40]);
		expect([...fields.hours]).toEqual([9, 11]);
		expect([...fields.months]).toEqual([1, 3]);
		expect([...fields.daysOfWeek]).toEqual([5, 6, 0, 1]);
		expect(parseCron("0 0 * * 7").daysOfWeek).toEqual(new Set([0]));
	});

	it("rejects invalid steps, invalid timezones, and exhausted search windows", () => {
		expect(() => parseCron("*/0 * * * *")).toThrow(/Invalid step/);
		expect(() => parseCron("*/wat * * * *")).toThrow(/Invalid step/);
		expect(() =>
			nextOccurrence(
				parseCron("* * * * *"),
				new Date("2026-01-01T00:00:00.000Z"),
				"Not/A_Timezone",
			),
		).toThrow(RangeError);
		expect(() =>
			nextOccurrence(
				parseCron("0 0 1 1 *"),
				new Date("2026-01-01T00:00:00.000Z"),
				"UTC",
				0,
			),
		).toThrow(/Could not find next cron occurrence/);
	});
});
