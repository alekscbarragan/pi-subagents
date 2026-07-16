import * as fs from "node:fs";
import * as path from "node:path";
import { allocateOwnerWorktree } from "../../src/runs/shared/worktree.ts";

const [repo, records, candidates, barrier, childId] = process.argv.slice(2);
const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
fs.writeFileSync(path.join(barrier, `ready-${childId}`), "ready");
while (!fs.existsSync(path.join(barrier, "go"))) sleep(2);
try {
	const result = allocateOwnerWorktree({ operationId: "shared-operation", ownerId: "workflow", cwd: repo }, { root: records, worktreeBaseDir: candidates });
	fs.writeFileSync(path.join(barrier, `result-${childId}.json`), JSON.stringify({ state: result.state, generation: result.generation, path: result.worktree?.path }));
} catch (error) {
	fs.writeFileSync(path.join(barrier, `result-${childId}.json`), JSON.stringify({ error: error?.message }));
}
