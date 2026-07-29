import type { Pool } from "mysql2/promise";
import { describe, expect, it } from "vitest";
import { ConfigError, MysqlBoss, ValidationError } from "../../src/index.js";

const unusedPool = {} as Pool;

describe("public configuration and lifecycle validation", () => {
	it.each([0, 101])("rejects batchSize %d", (batchSize) => {
		expect(() => new MysqlBoss({ pool: unusedPool, batchSize })).toThrow(
			ConfigError,
		);
	});

	it("rejects unsafe lease and archive retention settings", () => {
		expect(
			() =>
				new MysqlBoss({
					pool: unusedPool,
					leaseSeconds: 29,
					heartbeatSeconds: 10,
				}),
		).toThrow(/must be >= 3/);
		expect(
			() => new MysqlBoss({ pool: unusedPool, archiveRetentionDays: 0 }),
		).toThrow(ConfigError);
		expect(
			() => new MysqlBoss({ pool: unusedPool, archiveRetentionDays: 1.5 }),
		).toThrow(ConfigError);
	});

	it("rejects enqueue values before acquiring a database connection", async () => {
		const boss = new MysqlBoss({ pool: unusedPool });

		await expect(
			boss.enqueue("queue", {}, { singletonKey: "x".repeat(192) }),
		).rejects.toBeInstanceOf(ValidationError);
		await expect(
			boss.enqueue("queue", {}, { priority: 32768 }),
		).rejects.toBeInstanceOf(ValidationError);
	});

	it("makes stop idempotent and prevents later handler registration", async () => {
		const boss = new MysqlBoss({ pool: unusedPool });

		await boss.stop();
		await expect(boss.stop()).resolves.toBeUndefined();
		expect(() => boss.work("queue", async () => {})).toThrow(
			"Cannot register work after stop()",
		);
	});
});
