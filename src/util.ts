export function toUtcString(date: Date): string {
	return date.toISOString().slice(0, 23).replace("T", " ");
}
