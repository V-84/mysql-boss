import { writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const cjsDir = join(__dirname, "..", "dist", "cjs");

writeFileSync(join(cjsDir, "package.json"), JSON.stringify({ type: "commonjs" }));
