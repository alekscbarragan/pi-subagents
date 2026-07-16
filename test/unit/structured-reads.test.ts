import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { formatAsyncRunTranscript } from "../../src/runs/background/fleet-view.ts";
import {
	SUBAGENT_STRUCTURED_READ_VERSION,
	buildAsyncRunTranscriptPage,
	getRememberedForegroundSnapshot,
	getSubagentCapabilities,
	getSubagentRunSnapshotFromAsyncStatus,
	listSubagentRunSnapshots,
	resolveAllowedActions,
	snapshotFromNestedRun,
} from "../../src/runs/background/structured-reads.ts";
import type { AsyncStatus, NestedRunSummary, SubagentState } from "../../src/shared/types.ts";

describe("structured owner reads (P-02)", () => {
	it("advertises structured read and RPC capabilities", () => {
		const capabilities = getSubagentCapabilities();
		assert.equal(capabilities.version, SUBAGENT_STRUCTURED_READ_VERSION);
		assert.equal(capabilities.owner, "pi-subagents");
		assert.deepEqual(capabilities.structuredReads, {
			listRuns: true,
			getRun: true,
			awaitRuns: true,
			getTranscript: true,
			getCapabilities: true,
		});
		assert.deepEqual(capabilities.rpcMethods, ["ping", "status", "spawn", "interrupt", "stop", "listRuns", "getRun", "awaitRuns", "getTranscript", "getCapabilities", "steer", "resume", "allocateWorktree", "getWorktree", "acquireCheckoutLease", "getCheckoutLease", "releaseCheckoutLease", "recoverCheckoutLease"]);
		assert.deepEqual(capabilities.worktrees, { allocate: true, get: true });
		assert.deepEqual(capabilities.checkoutLeases, { acquire: true, get: true, release: true, recover: true });
		assert.equal(capabilities.controls.takeover, false);
		assert.equal(capabilities.toolActions.steer, true);
		assert.equal(capabilities.toolActions.resume, true);
		assert.equal(capabilities.toolActions.wait, true);
	});

	it("lists foreground and async runs with stable IDs and allowed actions", () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-structured-reads-list-"));
		try {
			const asyncRoot = path.join(root, "runs");
			const asyncDir = path.join(asyncRoot, "run-async");
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({
				runId: "run-async",
				sessionId: "session-1",
				mode: "single",
				state: "running",
				cwd: "/work/async",
				startedAt: 100,
				lastUpdate: 200,
				steps: [{ agent: "worker", status: "running", startedAt: 100 }],
			}, null, 2), "utf-8");

			const state = {
				currentSessionId: "session-1",
				foregroundControls: new Map([
					["fg-1", {
						runId: "fg-1",
						mode: "single",
						startedAt: 50,
						updatedAt: 250,
						currentAgent: "scout",
						currentIndex: 0,
					}],
				]),
			} as unknown as SubagentState;

			const snapshots = listSubagentRunSnapshots({
				asyncDirRoot: asyncRoot,
				resultsDir: path.join(root, "results"),
				state,
				kill: () => true,
				now: () => 300,
				ownerInstance: "owner-test",
			});

			assert.equal(snapshots.length, 2);
			assert.deepEqual(snapshots.map((snapshot) => snapshot.runId), ["fg-1", "run-async"]);
			assert.equal(snapshots[0]!.kind, "foreground");
			assert.equal(snapshots[0]!.state, "running");
			assert.equal(snapshots[0]!.ownerInstance, "owner-test");
			assert.ok(snapshots[0]!.allowedActions.some((action) => action.action === "interrupt" && action.availability === "available"));
			assert.ok(snapshots[0]!.allowedActions.some((action) => action.action === "stop" && action.availability === "unavailable"));

			assert.equal(snapshots[1]!.kind, "async");
			assert.equal(snapshots[1]!.cwd, "/work/async");
			assert.equal(snapshots[1]!.artifacts.asyncDir, asyncDir);
			assert.ok(snapshots[1]!.allowedActions.some((action) => action.action === "stop" && action.availability === "available"));
			assert.ok(snapshots[1]!.allowedActions.some((action) => action.action === "steer" && action.availability === "available"));
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("builds async snapshots and nested snapshots with parent linkage", () => {
		const status = {
			runId: "run-status",
			sessionId: "session-9",
			mode: "chain",
			state: "paused",
			cwd: "/repo",
			startedAt: 10,
			lastUpdate: 20,
			sessionFile: "/sessions/child.jsonl",
			steps: [{ agent: "planner", status: "paused", sessionFile: "/sessions/child.jsonl" }],
		} as AsyncStatus;
		const snapshot = getSubagentRunSnapshotFromAsyncStatus(status, "/tmp/run-status", { ownerInstance: "inst-1" });
		assert.equal(snapshot.version, SUBAGENT_STRUCTURED_READ_VERSION);
		assert.equal(snapshot.kind, "async");
		assert.equal(snapshot.runId, "run-status");
		assert.equal(snapshot.revision, "run-status:paused:20");
		assert.equal(snapshot.cwd, "/repo");
		assert.equal(snapshot.children[0]?.agent, "planner");
		assert.ok(snapshot.allowedActions.some((action) => action.action === "resume"));

		const nested: NestedRunSummary = {
			id: "nested-1",
			parentRunId: "run-status",
			path: [],
			state: "running",
			mode: "single",
			agent: "reviewer",
			startedAt: 11,
			asyncDir: "/tmp/nested-1",
		};
		const nestedSnapshot = snapshotFromNestedRun(nested, "run-status");
		assert.equal(nestedSnapshot.kind, "nested");
		assert.equal(nestedSnapshot.parentRunId, "run-status");
		assert.equal(nestedSnapshot.rootRunId, "run-status");
		assert.equal(nestedSnapshot.agent, "reviewer");
		assert.equal(nestedSnapshot.cwd, undefined);
	});

	it("does not claim remembered foreground output final without durable result evidence", () => {
		const snapshot = getRememberedForegroundSnapshot({
			runId: "remembered-1",
			mode: "single",
			cwd: "/repo",
			updatedAt: 10,
			children: [{ agent: "worker", index: 0, status: "completed", finalOutput: "done" }],
		});

		assert.equal(snapshot.settlement.terminal, true);
		assert.equal(snapshot.settlement.output, "incomplete");
		assert.equal(snapshot.settlement.artifacts, "incomplete");
		assert.match(snapshot.settlement.reasons[0] ?? "", /no durable result\/output evidence/i);
	});

	it("reports owner truncation on transcript pages and preserves human renderer output", () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-structured-reads-transcript-"));
		try {
			const asyncDir = path.join(root, "run-transcript");
			fs.mkdirSync(asyncDir, { recursive: true });
			const outputPath = path.join(asyncDir, "output-0.log");
			fs.writeFileSync(outputPath, "first line\nsecond line\nthird line\n", "utf-8");
			const status = {
				runId: "run-transcript",
				mode: "single",
				state: "running",
				startedAt: 100,
				lastUpdate: 200,
				currentStep: 0,
				steps: [{ agent: "worker", status: "running", startedAt: 100 }],
			} as AsyncStatus;

			const page = buildAsyncRunTranscriptPage(status, asyncDir, { lines: 2 });
			assert.equal(page.version, SUBAGENT_STRUCTURED_READ_VERSION);
			assert.equal(page.kind, "async");
			assert.equal(page.truncated, true);
			assert.deepEqual(page.lines, ["second line", "third line"]);
			assert.match(page.source, /Transcript tail from/);
			assert.ok(page.artifacts.outputPaths?.includes(outputPath));

			const rendered = formatAsyncRunTranscript(status, asyncDir, { lines: 2 });
			assert.match(rendered, /Run: run-transcript/);
			assert.match(rendered, /Step: 0 \(worker\) \| running/);
			assert.match(rendered, /\(tail truncated\):/);
			assert.doesNotMatch(rendered, /first line/);
			assert.match(rendered, /second line/);
			assert.match(rendered, /third line/);
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("resolves allowed actions truthfully for live, paused, and terminal states", () => {
		const live = resolveAllowedActions({ kind: "async", state: "running", hasSessionFile: false });
		assert.ok(live.find((entry) => entry.action === "interrupt")?.availability === "available");
		assert.ok(live.find((entry) => entry.action === "stop")?.availability === "available");
		assert.ok(live.find((entry) => entry.action === "steer")?.availability === "available");
		assert.ok(live.find((entry) => entry.action === "resume")?.availability === "available");

		const paused = resolveAllowedActions({ kind: "async", state: "paused", hasSessionFile: true });
		assert.ok(paused.find((entry) => entry.action === "resume")?.availability === "available");
		assert.ok(paused.find((entry) => entry.action === "steer")?.availability === "unavailable");

		const terminal = resolveAllowedActions({ kind: "async", state: "complete", hasSessionFile: false });
		assert.ok(terminal.find((entry) => entry.action === "resume")?.availability === "unavailable");
		assert.ok(terminal.find((entry) => entry.action === "interrupt")?.availability === "unavailable");

		const foreground = resolveAllowedActions({ kind: "foreground", state: "running" });
		assert.ok(foreground.find((entry) => entry.action === "stop")?.availability === "unavailable");
	});

	it("returns an empty fleet list in child-safe mode", () => {
		const snapshots = listSubagentRunSnapshots({ childSafe: true });
		assert.deepEqual(snapshots, []);
	});
});
