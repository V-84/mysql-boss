import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    testTimeout: 120_000,
    hookTimeout: 180_000,
    pool: "forks",
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    include: ["test/**/*.test.ts"],
  },
});
