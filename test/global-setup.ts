import { GenericContainer, Wait } from "testcontainers";

export default async function setupMysql(): Promise<
	(() => Promise<void>) | void
> {
	if (process.env.MYSQL_HOST) return;

	const container = await new GenericContainer("mysql:8.0")
		.withEnvironment({
			MYSQL_ROOT_PASSWORD: "test",
			MYSQL_DATABASE: "testdb",
		})
		.withExposedPorts(3306)
		.withHealthCheck({
			test: ["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 -uroot -ptest"],
			interval: 1_000,
			timeout: 5_000,
			retries: 60,
			startPeriod: 5_000,
		})
		.withWaitStrategy(Wait.forHealthCheck())
		.withStartupTimeout(120_000)
		.start();

	process.env.MYSQL_HOST = container.getHost();
	process.env.MYSQL_PORT = container.getMappedPort(3306).toString();
	process.env.MYSQL_USER = "root";
	process.env.MYSQL_PASSWORD = "test";
	process.env.MYSQL_DATABASE = "testdb";

	return async () => {
		await container.stop();
	};
}
