export class ConfigError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ConfigError";
	}
}

export class ValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ValidationError";
	}
}

export class SingletonCollisionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "SingletonCollisionError";
	}
}
