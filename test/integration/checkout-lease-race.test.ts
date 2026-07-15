import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";

function git(cwd: string, args: string[]) { const r = spawnSync("git", ["-C", cwd, ...args], { encoding: "utf-8" }); if (r.status !== 0) throw new Error(r.stderr.trim() || r.stdout.trim()); }
function fixture() { const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-lease-race-")); const repo = path.join(root, "repo"); fs.mkdirSync(repo); git(repo, ["init"]); git(repo, ["config", "user.email", "test@example.com"]); git(repo, ["config", "user.name", "Test"]); fs.writeFileSync(path.join(repo, "a"), "a"); git(repo, ["add", "a"]); git(repo, ["commit", "--no-verify", "-m", "initial"]); return { root, repo, store: path.join(root, "store"), barrier: path.join(root, "barrier") }; }
async function waitFor(check: () => boolean, timeout = 5000) { const end = Date.now() + timeout; while (!check()) { if (Date.now() > end) throw new Error("race helper timed out"); await new Promise((r) => setTimeout(r, 10)); } }
async function race(unsafe: boolean, modes: readonly ["workflow" | "ordinary", "workflow" | "ordinary"] = ["workflow", "workflow"]) {
	const f = fixture(); fs.mkdirSync(f.barrier);
	const children = ["a", "b"].map((id, index) => spawn(process.execPath, ["--experimental-strip-types", "test/support/checkout-lease-race-child.mjs", f.repo, f.store, id, f.barrier, String(unsafe), modes[index]!], { cwd: process.cwd(), stdio: "inherit" }));
	try {
		await waitFor(() => fs.readdirSync(f.barrier).filter((n) => n.startsWith("ready-")).length === 2);
		fs.writeFileSync(path.join(f.barrier, "go"), "go");
		await Promise.all(children.map((child) => new Promise<void>((resolve, reject) => child.once("exit", (code) => code === 0 ? resolve() : reject(new Error(`child exited ${code}`))))));
		return ["a", "b"].map((id) => JSON.parse(fs.readFileSync(path.join(f.barrier, `result-${id}.json`), "utf-8")) as { success: boolean; code?: string });
	} finally { for (const child of children) child.kill(); fs.rmSync(f.root, { recursive: true, force: true }); }
}

describe("owner worktree multiprocess replay", () => {
	it("concurrent same-operation callers converge on one allocation", async () => {
		const f = fixture(); fs.mkdirSync(f.barrier); const records = path.join(f.root, "allocations"); const candidates = path.join(f.root, "candidates");
		const children = ["a", "b"].map((id) => spawn(process.execPath, ["--experimental-strip-types", "test/support/worktree-allocation-race-child.mjs", f.repo, records, candidates, f.barrier, id], { cwd: process.cwd(), stdio: "inherit" }));
		try {
			await waitFor(() => fs.readdirSync(f.barrier).filter((name) => name.startsWith("ready-")).length === 2); fs.writeFileSync(path.join(f.barrier, "go"), "go");
			await Promise.all(children.map((child) => new Promise<void>((resolve, reject) => child.once("exit", (code) => code === 0 ? resolve() : reject(new Error(`child exited ${code}`))))));
			const results = ["a", "b"].map((id) => JSON.parse(fs.readFileSync(path.join(f.barrier, `result-${id}.json`), "utf-8")) as { state?: string; generation?: string; path?: string; error?: string });
			assert.deepEqual(results.map((result) => result.state), ["allocated", "allocated"]); assert.equal(results[0]!.generation, results[1]!.generation); assert.equal(results[0]!.path, results[1]!.path);
			assert.equal(fs.readdirSync(candidates).length, 1);
		} finally { for (const child of children) child.kill(); fs.rmSync(f.root, { recursive: true, force: true }); }
	});
});

describe("checkout lease multiprocess admission", () => {
	it("admits exactly one writer across two OS processes", async () => { const results = await race(false); assert.equal(results.filter((r) => r.success).length, 1); assert.equal(results.find((r) => !r.success)?.code, "conflict"); });
	it("negative control exposes overlap when atomic create-if-absent is disabled", async () => { const results = await race(true); assert.equal(results.filter((r) => r.success).length, 2); });
	it("admits at most one workflow/ordinary writer across two OS processes", async () => { const results = await race(false, ["workflow", "ordinary"]); assert.ok(results.filter((r) => r.success).length <= 1, JSON.stringify(results)); assert.ok(results.some((r) => r.code === "conflict")); });
});
