import { describe, expect, it, vi } from "vitest";
import { withDeadlockRetry } from "../../src/retry-util.js";

describe("database contention retry policy", () => {
	it.each([1213, 1205])(
		"retries MySQL errno %d and returns the later result",
		async (errno) => {
			vi.spyOn(Math, "random").mockReturnValue(0);
			const operation = vi
				.fn<() => Promise<string>>()
				.mockRejectedValueOnce({ errno })
				.mockResolvedValue("committed");

			await expect(withDeadlockRetry(operation)).resolves.toBe("committed");
			expect(operation).toHaveBeenCalledTimes(2);
			vi.restoreAllMocks();
		},
	);

	it("does not retry unrelated failures", async () => {
		const error = new Error("invalid statement");
		const operation = vi.fn().mockRejectedValue(error);

		await expect(withDeadlockRetry(operation)).rejects.toBe(error);
		expect(operation).toHaveBeenCalledOnce();
	});

	it("stops after three retryable failures and preserves the final error", async () => {
		vi.spyOn(Math, "random").mockReturnValue(0);
		const first = { errno: 1213, attempt: 1 };
		const second = { errno: 1213, attempt: 2 };
		const final = { errno: 1213, attempt: 3 };
		const operation = vi
			.fn()
			.mockRejectedValueOnce(first)
			.mockRejectedValueOnce(second)
			.mockRejectedValueOnce(final);

		await expect(withDeadlockRetry(operation)).rejects.toBe(final);
		expect(operation).toHaveBeenCalledTimes(3);
		vi.restoreAllMocks();
	});
});
