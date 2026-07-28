const ER_LOCK_DEADLOCK = 1213;
const ER_LOCK_WAIT_TIMEOUT = 1205;
const MAX_RETRIES = 3;

function isRetryable(err: unknown): boolean {
	if (typeof err === "object" && err !== null && "errno" in err) {
		const errno = (err as { errno: number }).errno;
		return errno === ER_LOCK_DEADLOCK || errno === ER_LOCK_WAIT_TIMEOUT;
	}
	return false;
}

export async function withDeadlockRetry<T>(fn: () => Promise<T>): Promise<T> {
	let lastErr: unknown;
	for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
		try {
			return await fn();
		} catch (err) {
			lastErr = err;
			if (!isRetryable(err) || attempt === MAX_RETRIES - 1) {
				throw err;
			}
			const jitter = Math.random() * 50 * (attempt + 1);
			await new Promise((resolve) => setTimeout(resolve, jitter));
		}
	}
	throw lastErr;
}
