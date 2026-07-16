import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { timeoutRequestPath } from "../../src/runs/background/control-channel.ts";
import { SUBAGENT_ASYNC_COMPLETE_EVENT, SUBAGENT_ASYNC_STARTED_EVENT } from "../../src/shared/types.ts";
import {
	SUBAGENT_LIFECYCLE_EVENT,
	SUBAGENT_RPC_PROTOCOL_VERSION,
	SUBAGENT_RPC_READY_EVENT,
	SUBAGENT_RPC_REQUEST_EVENT,
	registerSubagentRpcBridge,
	subagentRpcReplyEvent,
	type SubagentRpcReplyEnvelope,
} from "../../src/extension/rpc.ts";

class FakeEvents {
	readonly emitted: Array<{ event: string; data: unknown }> = [];
	private handlers = new Map<string, Array<(data: unknown) => void>>();

	on(event: string, handler: (data: unknown) => void): () => void {
		const list = this.handlers.get(event) ?? [];
		list.push(handler);
		this.handlers.set(event, list);
		return () => {
			const current = this.handlers.get(event) ?? [];
			this.handlers.set(event, current.filter((candidate) => candidate !== handler));
		};
	}

	emit(event: string, data: unknown): void {
		this.emitted.push({ event, data });
		for (const handler of [...(this.handlers.get(event) ?? [])]) handler(data);
	}
}

function once(events: FakeEvents, event: string): Promise<unknown> {
	return new Promise((resolve) => {
		const unsubscribe = events.on(event, (payload) => {
			unsubscribe();
			resolve(payload);
		});
	});
}

function createGitRepo(root: string): string {
	const repo = path.join(root, "repo"); fs.mkdirSync(repo);
	const git = (args: string[]) => { const result = spawnSync("git", ["-C", repo, ...args], { encoding: "utf-8" }); if (result.status !== 0) throw new Error(result.stderr.trim() || result.stdout.trim()); };
	git(["init"]); git(["config", "user.email", "test@example.com"]); git(["config", "user.name", "Test"]);
	fs.writeFileSync(path.join(repo, "tracked.txt"), "initial\n"); git(["add", "tracked.txt"]); git(["commit", "--no-verify", "-m", "initial"]);
	return repo;
}

function ctx(cwd = "/repo") {
	return {
		cwd,
		sessionManager: {
			getSessionId: () => "session-123",
			getSessionFile: () => "/sessions/parent.jsonl",
		},
	} as any;
}

async function request(events: FakeEvents, requestId: string, method: string, params?: unknown): Promise<SubagentRpcReplyEnvelope> {
	const reply = once(events, subagentRpcReplyEvent(requestId)) as Promise<SubagentRpcReplyEnvelope>;
	events.emit(SUBAGENT_RPC_REQUEST_EVENT, {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		requestId,
		method,
		...(params !== undefined ? { params } : {}),
	});
	return reply;
}

describe("subagent extension RPC bridge", () => {
	it("emits ready and answers ping with versioned capability metadata", async () => {
		const events = new FakeEvents();
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async () => assert.fail("ping should not call executor"),
		});

		const readyPromise = once(events, SUBAGENT_RPC_READY_EVENT);
		bridge.emitReady(ctx());
		const ready = await readyPromise as { version?: number; events?: { request?: string }; session?: { cwd?: string } };
		assert.equal(ready.version, SUBAGENT_RPC_PROTOCOL_VERSION);
		assert.equal(ready.events?.request, SUBAGENT_RPC_REQUEST_EVENT);
		assert.equal(ready.session?.cwd, "/repo");

		const reply = await request(events, "ping-1", "ping");
		assert.equal(reply.success, true);
		assert.equal(reply.method, "ping");
		assert.equal((reply as { data: { version?: number } }).data.version, SUBAGENT_RPC_PROTOCOL_VERSION);

		bridge.dispose();
	});

	it("replies to malformed request ids on the safe unknown channel", async () => {
		const events = new FakeEvents();
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async () => assert.fail("malformed request should not call executor"),
		});
		const unsafeRequestId = "bad\nchannel";
		const replyPromise = once(events, subagentRpcReplyEvent("unknown")) as Promise<SubagentRpcReplyEnvelope>;

		events.emit(SUBAGENT_RPC_REQUEST_EVENT, {
			version: SUBAGENT_RPC_PROTOCOL_VERSION,
			requestId: unsafeRequestId,
			method: "ping",
		});
		const reply = await replyPromise;

		assert.equal(reply.success, false);
		assert.equal(reply.requestId, "unknown");
		assert.equal((reply as { error: { code: string } }).error.code, "invalid_request");
		assert.equal(events.emitted.some((entry) => entry.event === subagentRpcReplyEvent(unsafeRequestId)), false);

		bridge.dispose();
	});

	it("delegates status through the existing executor action", async () => {
		const events = new FakeEvents();
		let executedParams: unknown;
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async (_id, params) => {
				executedParams = params;
				return { content: [{ type: "text", text: "Run: abc123" }], details: { mode: "management", results: [] } } as any;
			},
		});

		const reply = await request(events, "status-1", "status", { id: "abc123" });

		assert.equal(reply.success, true);
		assert.deepEqual(executedParams, { action: "status", id: "abc123" });
		assert.equal((reply as { data: { text?: string } }).data.text, "Run: abc123");

		bridge.dispose();
	});

	it("forces spawn requests onto the existing async execution path", async () => {
		const events = new FakeEvents();
		let executedParams: any;
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async (_id, params) => {
				executedParams = params;
				return {
					content: [{ type: "text", text: "Async: worker [run-1]" }],
					details: { mode: "single", results: [], asyncId: "run-1", asyncDir: "/tmp/run-1" },
				} as any;
			},
		});

		const reply = await request(events, "spawn-1", "spawn", { agent: "worker", task: "Do work" });

		assert.equal(reply.success, true);
		assert.equal(executedParams.agent, "worker");
		assert.equal(executedParams.task, "Do work");
		assert.equal(executedParams.async, true);
		assert.equal(executedParams.clarify, false);
		assert.equal((reply as { data: { details?: { asyncId?: string } } }).data.details?.asyncId, "run-1");

		bridge.dispose();
	});

	it("rejects foreground or management spawn requests before executor dispatch", async () => {
		const events = new FakeEvents();
		let executeCalls = 0;
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async () => {
				executeCalls++;
				return { content: [{ type: "text", text: "unexpected" }], details: { mode: "single", results: [] } } as any;
			},
		});

		const foreground = await request(events, "spawn-foreground", "spawn", { agent: "worker", task: "Do work", async: false });
		const management = await request(events, "spawn-management", "spawn", { action: "list" });

		assert.equal(foreground.success, false);
		assert.equal((foreground as { error: { code: string; message: string } }).error.code, "invalid_params");
		assert.match((foreground as { error: { message: string } }).error.message, /detached async/);
		assert.equal(management.success, false);
		assert.match((management as { error: { message: string } }).error.message, /does not accept management/);
		assert.equal(executeCalls, 0);

		bridge.dispose();
	});

	it("delegates interrupt through the existing executor action", async () => {
		const events = new FakeEvents();
		let executedParams: unknown;
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async (_id, params) => {
				executedParams = params;
				return { content: [{ type: "text", text: "Interrupt requested for async run abc123." }], details: { mode: "management", results: [] } } as any;
			},
			state: { foregroundControls: new Map() } as any,
		});

		const reply = await request(events, "interrupt-1", "interrupt", { id: "abc123" });

		assert.equal(reply.success, true);
		assert.deepEqual(executedParams, { action: "interrupt", id: "abc123" });
		assert.deepEqual((reply as any).data, {
			text: "Interrupt requested for async run abc123.",
			details: { mode: "management", results: [] },
		});

		bridge.dispose();
	});

	it("uses the existing async timeout control path for stop", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-stop-"));
		try {
			const events = new FakeEvents();
			const asyncRoot = path.join(root, "runs");
			const resultsDir = path.join(root, "results");
			const asyncDir = path.join(asyncRoot, "run-stop");
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({
				runId: "run-stop",
				sessionId: "session-123",
				mode: "single",
				state: "running",
				pid: 4242,
				startedAt: 100,
				lastUpdate: 100,
				steps: [{ agent: "worker", status: "running", startedAt: 100 }],
			}, null, 2), "utf-8");
			const bridge = registerSubagentRpcBridge({
				events,
				getContext: () => ctx(),
				execute: async () => assert.fail("stop should not call executor"),
				asyncDirRoot: asyncRoot,
				resultsDir,
				kill: () => true,
				now: () => 150,
			});

			const reply = await request(events, "stop-1", "stop", { id: "run-stop" });

			assert.equal(reply.success, true);
			assert.equal((reply as { data: { runId?: string; state?: string } }).data.runId, "run-stop");
			assert.equal((reply as { data: { state?: string } }).data.state, "stopping");
			assert.equal(fs.existsSync(timeoutRequestPath(asyncDir)), true);

			bridge.dispose();
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("exposes structured lifecycle reads, capabilities, await, transcripts, and event-loss reconciliation", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-reads-"));
		try {
			const events = new FakeEvents();
			const asyncRoot = path.join(root, "runs");
			const resultsDir = path.join(root, "results");
			const asyncDir = path.join(asyncRoot, "run-read");
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({
				runId: "run-read", sessionId: "session-123", mode: "single", state: "running", cwd: root,
				startedAt: 100, lastUpdate: 200, currentStep: 0,
				steps: [{ agent: "worker", status: "running", startedAt: 100 }],
			}), "utf-8");
			fs.writeFileSync(path.join(asyncDir, "output-0.log"), "one\ntwo\nthree\n", "utf-8");
			const state = { currentSessionId: "session-123", baseCwd: root, foregroundControls: new Map(), foregroundRuns: new Map(), asyncJobs: new Map() } as any;
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("reads must not call executor"), state, asyncDirRoot: asyncRoot, resultsDir, kill: () => true, ownerInstance: "owner-read" });

			const capabilities = await request(events, "caps-1", "getCapabilities");
			assert.equal(capabilities.success, true);
			assert.equal((capabilities as any).data.structuredReads.awaitRuns, true);

			// No lifecycle event was emitted for this fixture: durable reread still discovers it.
			const listed = await request(events, "list-1", "listRuns");
			assert.equal(listed.success, true);
			assert.equal((listed as any).data.runs[0].runId, "run-read");
			assert.equal((listed as any).data.runs[0].checkoutIdentity.canonicalPath, fs.realpathSync(root));

			const got = await request(events, "get-1", "getRun", { id: "run-read" });
			assert.equal((got as any).data.revision, "run-read:running:200");
			assert.equal((got as any).data.settlement.output, "pending");

			const awaited = await request(events, "await-1", "awaitRuns", { runId: "run-read", revisions: { "run-read": "older" }, timeoutMs: 1 });
			assert.equal((awaited as any).data.reason, "changed");

			const transcript = await request(events, "transcript-1", "getTranscript", { id: "run-read", lines: 2 });
			assert.deepEqual((transcript as any).data.lines, ["two", "three"]);
			assert.equal((transcript as any).data.truncated, true);
			assert.equal((transcript as any).data.finality, "pending");
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("returns terminal structured outputs through awaitRuns", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-await-result-"));
		try {
			const events = new FakeEvents(); const asyncRoot = path.join(root, "runs"); const resultsDir = path.join(root, "results");
			const asyncDir = path.join(asyncRoot, "run-result"); fs.mkdirSync(asyncDir, { recursive: true }); fs.mkdirSync(resultsDir);
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId: "run-result", sessionId: "session-123", mode: "single", state: "complete", cwd: root, startedAt: 100, lastUpdate: 200, steps: [{ agent: "worker", status: "completed", startedAt: 100 }] }));
			fs.writeFileSync(path.join(resultsDir, "run-result.json"), JSON.stringify({ id: "run-result", success: true, state: "complete", results: [{ agent: "worker", success: true, structuredOutput: { summary: "done" } }] }));
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("await must not execute"), asyncDirRoot: asyncRoot, resultsDir });
			const awaited = await request(events, "await-result", "awaitRuns", { runId: "run-result", timeoutMs: 0 });
			assert.equal((awaited as any).data.reason, "terminal");
			assert.deepEqual((awaited as any).data.results, { "run-result": { summary: "done" } });
			fs.writeFileSync(path.join(resultsDir, "run-result.json"), JSON.stringify({ id: "run-result", success: true, state: "complete", results: [{ agent: "worker", success: true, output: "{\"summary\":\"raw\"}" }] }));
			const raw = await request(events, "await-raw-result", "awaitRuns", { runId: "run-result", timeoutMs: 0 });
			assert.deepEqual((raw as any).data.results, { "run-result": "{\"summary\":\"raw\"}" });
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("keeps evidence failures local to the affected run and rejects only its control", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-evidence-"));
		try {
			const events = new FakeEvents(); const asyncRoot = path.join(root, "runs"); const resultsDir = path.join(root, "results");
			for (const [runId, cwd, lastUpdate] of [["run-healthy", root, 20], ["run-no-evidence", undefined, 10]] as const) {
				const dir = path.join(asyncRoot, runId); fs.mkdirSync(dir, { recursive: true });
				fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ runId, sessionId: "session-123", mode: "single", state: "running", ...(cwd ? { cwd } : {}), startedAt: 1, lastUpdate, steps: [{ agent: "worker", status: "running" }] }), "utf-8");
			}
			let executeCalls = 0;
			const state = { currentSessionId: "session-123", baseCwd: root, foregroundControls: new Map(), foregroundRuns: new Map(), asyncJobs: new Map() } as any;
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => { executeCalls++; return { content: [], details: { mode: "management", results: [] } } as any; }, state, asyncDirRoot: asyncRoot, resultsDir, kill: () => true });
			const listed = await request(events, "evidence-list", "listRuns");
			assert.equal(listed.success, true);
			const runs = (listed as any).data.runs;
			assert.equal(runs.find((run: any) => run.runId === "run-healthy").evidence.status, "valid");
			assert.equal(runs.find((run: any) => run.runId === "run-no-evidence").evidence.status, "invalid");
			const healthy = await request(events, "evidence-get", "getRun", { id: "run-healthy" });
			assert.equal((healthy as any).data.runId, "run-healthy");
			const rejected = await request(events, "evidence-control", "steer", { id: "run-no-evidence", operationId: "op-no-evidence", message: "continue" });
			assert.equal((rejected as any).data.state, "rejected");
			assert.match((rejected as any).data.message, /cwd/i);
			assert.equal(executeCalls, 0);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("treats paused runs as non-terminal when awaiting", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-paused-"));
		try {
			const events = new FakeEvents(); const asyncRoot = path.join(root, "runs"); const resultsDir = path.join(root, "results"); const dir = path.join(asyncRoot, "run-paused");
			fs.mkdirSync(dir, { recursive: true });
			fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ runId: "run-paused", sessionId: "session-123", mode: "single", state: "paused", cwd: root, startedAt: 1, lastUpdate: 2, steps: [{ agent: "worker", status: "paused" }] }), "utf-8");
			const state = { currentSessionId: "session-123", baseCwd: root, foregroundControls: new Map(), foregroundRuns: new Map(), asyncJobs: new Map() } as any;
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("await must not execute"), state, asyncDirRoot: asyncRoot, resultsDir });
			const got = await request(events, "paused-get", "getRun", { id: "run-paused" });
			assert.equal((got as any).data.settlement.terminal, false);
			assert.equal((got as any).data.settlement.output, "pending");
			const awaited = await request(events, "paused-await", "awaitRuns", { runId: "run-paused", timeoutMs: 0 });
			assert.equal((awaited as any).data.reason, "timeout");
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("strictly validates new RPC params and control indexes", async () => {
		const events = new FakeEvents();
		const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("invalid params must not execute") });
		for (const [id, method, params] of [
			["invalid-empty", "getCapabilities", { extra: true }],
			["invalid-target", "getRun", { id: "run", extra: true }],
			["invalid-transcript", "getTranscript", { id: "run", lines: 0 }],
			["invalid-index", "interrupt", { id: "run", index: 1.5 }],
			["invalid-control", "steer", { id: "run", operationId: "op", message: "x", extra: true }],
		] as const) {
			const reply = await request(events, id, method, params);
			assert.equal(reply.success, false);
			assert.equal((reply as any).error.code, "invalid_params");
		}
		bridge.dispose();
	});

	it("emits additive lifecycle hints for existing lifecycle events", async () => {
		const events = new FakeEvents();
		const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("events must not execute"), ownerInstance: "owner-events", now: () => 444 });
		const started = once(events, SUBAGENT_LIFECYCLE_EVENT) as Promise<any>;
		events.emit(SUBAGENT_ASYNC_STARTED_EVENT, { id: "run-event" });
		assert.deepEqual(await started, { version: 1, owner: "pi-subagents", ownerInstance: "owner-events", runId: "run-event", reason: "started", observedAt: 444 });
		const settled = once(events, SUBAGENT_LIFECYCLE_EVENT) as Promise<any>;
		events.emit(SUBAGENT_ASYNC_COMPLETE_EVENT, { runId: "run-event" });
		assert.equal((await settled).reason, "settled");
		bridge.dispose();
	});

	it("persists revision-aware control outcomes, replays operation ids, and exposes them through getRun", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-controls-"));
		try {
			const events = new FakeEvents();
			const asyncRoot = path.join(root, "runs"); const resultsDir = path.join(root, "results"); const asyncDir = path.join(asyncRoot, "run-control");
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId: "run-control", sessionId: "session-123", mode: "single", state: "running", cwd: root, startedAt: 100, lastUpdate: 200, steps: [{ agent: "worker", status: "running" }] }), "utf-8");
			const state = { currentSessionId: "session-123", baseCwd: root, foregroundControls: new Map(), foregroundRuns: new Map(), asyncJobs: new Map() } as any;
			const calls: any[] = [];
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async (_id, params) => {
				calls.push(params);
				if (params.message === "missing-outcome") return { content: [{ type: "text", text: "owner result without structured outcome" }], details: { mode: "management", results: [] } } as any;
				const outcome = params.message === "unsupported" ? "unsupported" : params.action === "resume" ? "revived" : params.action === "steer" ? "queued" : "delivered";
				return { content: [{ type: "text", text: "owner result" }], ...(outcome === "unsupported" ? { isError: true } : {}), details: { mode: "management", results: [], control: { action: params.action, outcome, runId: params.id } } } as any;
			}, state, asyncDirRoot: asyncRoot, resultsDir, kill: () => true, now: () => 300 });

			const conflict = await request(events, "conflict", "interrupt", { id: "run-control", operationId: "op-conflict", expectedRevision: "stale" });
			assert.equal((conflict as any).data.state, "rejected");
			assert.equal((conflict as any).data.outcome, "revision_conflict");
			assert.equal(calls.length, 0);

			for (const [method, operationId, message, outcome] of [["interrupt", "op-interrupt", undefined, "delivered"], ["stop", "op-stop", undefined, "delivered"], ["steer", "op-steer", "focus", "queued"], ["resume", "op-resume", "continue", "revived"]] as const) {
				const reply = await request(events, operationId, method, { id: "run-control", operationId, expectedRevision: "run-control:running:200", ...(message ? { message } : {}) });
				assert.equal((reply as any).data.state, "completed");
				assert.equal((reply as any).data.outcome, outcome);
			}
			const unsupported = await request(events, "unsupported-request", "steer", { id: "run-control", operationId: "op-unsupported", message: "unsupported" });
			assert.equal((unsupported as any).data.state, "completed");
			assert.equal((unsupported as any).data.outcome, "unsupported");
			assert.doesNotMatch((unsupported as any).data.message, /owner result/);

			const missingOutcome = await request(events, "missing-outcome-request", "steer", { id: "run-control", operationId: "op-missing-outcome", message: "missing-outcome" });
			assert.equal((missingOutcome as any).data.state, "failed");
			assert.match((missingOutcome as any).data.message, /structured steer outcome/);

			const steerCallsBeforeReplay = calls.filter((entry) => entry.action === "steer").length;
			const replay = await request(events, "replay-request", "steer", { id: "run-control", operationId: "op-steer", expectedRevision: "run-control:running:200", message: "different ignored replay" });
			assert.equal((replay as any).data.operationId, "op-steer");
			assert.equal(calls.filter((entry) => entry.action === "steer").length, steerCallsBeforeReplay);

			const queried = await request(events, "query-operations", "getRun", { id: "run-control" });
			assert.deepEqual((queried as any).data.operations.map((entry: any) => entry.operationId).sort(), ["op-conflict", "op-interrupt", "op-missing-outcome", "op-resume", "op-steer", "op-stop", "op-unsupported"]);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("keeps an accepted operation durable after the registering caller disposes", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-abort-"));
		try {
			const events = new FakeEvents(); const asyncRoot = path.join(root, "runs"); const resultsDir = path.join(root, "results"); const asyncDir = path.join(asyncRoot, "run-abort");
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId: "run-abort", sessionId: "session-123", mode: "single", state: "running", cwd: root, startedAt: 1, lastUpdate: 2, steps: [{ agent: "worker", status: "running" }] }), "utf-8");
			const state = { currentSessionId: "session-123", baseCwd: root, foregroundControls: new Map(), foregroundRuns: new Map(), asyncJobs: new Map() } as any;
			let release!: () => void; const gate = new Promise<void>((resolve) => { release = resolve; });
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async (_id, params) => { await gate; return { content: [{ type: "text", text: "ok" }], details: { mode: "management", results: [], control: { action: "steer", outcome: "queued", runId: params.id } } } as any; }, state, asyncDirRoot: asyncRoot, resultsDir, now: () => 9 });
			const replyPromise = once(events, subagentRpcReplyEvent("abort-control")) as Promise<any>;
			events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 1, requestId: "abort-control", method: "steer", params: { id: "run-abort", operationId: "op-abort", message: "continue" } });
			await new Promise((resolve) => setImmediate(resolve));
			bridge.dispose(); release();
			assert.equal((await replyPromise).data.state, "completed");

			const reader = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("query must not execute"), state, asyncDirRoot: asyncRoot, resultsDir });
			const queried = await request(events, "abort-query", "getRun", { id: "run-abort" });
			assert.equal((queried as any).data.operations.find((entry: any) => entry.operationId === "op-abort").state, "completed");
			reader.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("reports terminal output and artifacts explicitly incomplete without durable final evidence", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-finality-"));
		try {
			const events = new FakeEvents(); const asyncRoot = path.join(root, "runs"); const resultsDir = path.join(root, "results"); const asyncDir = path.join(asyncRoot, "run-incomplete");
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId: "run-incomplete", sessionId: "session-123", mode: "single", state: "failed", cwd: root, startedAt: 1, endedAt: 2, lastUpdate: 2, currentStep: 0, steps: [{ agent: "worker", status: "failed" }] }), "utf-8");
			fs.writeFileSync(path.join(asyncDir, "output-0.log"), "one\ntwo\nthree\n", "utf-8");
			const state = { currentSessionId: "session-123", baseCwd: root, foregroundControls: new Map(), foregroundRuns: new Map(), asyncJobs: new Map() } as any;
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("read must not execute"), state, asyncDirRoot: asyncRoot, resultsDir, kill: () => true });
			const reply = await request(events, "incomplete", "getRun", { id: "run-incomplete" });
			assert.equal((reply as any).data.settlement.terminal, true);
			assert.equal((reply as any).data.settlement.output, "incomplete");
			assert.equal((reply as any).data.settlement.artifacts, "incomplete");
			assert.ok((reply as any).data.settlement.reasons.length >= 1);
			const transcript = await request(events, "incomplete-transcript", "getTranscript", { id: "run-incomplete", lines: 2 });
			assert.equal((transcript as any).data.truncated, true);
			assert.equal((transcript as any).data.finality, "incomplete");
			assert.equal("cursor" in (transcript as any).data, false);
			assert.equal("nextCursor" in (transcript as any).data, false);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("exercises owner worktree and authenticated checkout lease RPC methods", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-u3-"));
		try {
			const repo = createGitRepo(root); const events = new FakeEvents();
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(repo), execute: async () => assert.fail("owner operations must not execute a child"), checkoutLeaseRoot: path.join(root, "leases"), worktreeAllocationRoot: path.join(root, "allocations"), worktreeBaseDir: path.join(root, "candidates"), ownerInstance: "owner-u3" });
			const callerDestination = await request(events, "alloc-caller-dir", "allocateWorktree", { operationId: "candidate-op", ownerId: "workflow", cwd: repo, baseDir: path.join(root, "caller-selected") });
			assert.equal(callerDestination.success, false); assert.equal((callerDestination as any).error.code, "invalid_params");
			const allocated = await request(events, "alloc", "allocateWorktree", { operationId: "candidate-op", ownerId: "workflow", cwd: repo });
			assert.equal((allocated as any).data.state, "allocated");
			assert.ok((allocated as any).data.worktree.path.startsWith(path.join(root, "candidates")));
			const replay = await request(events, "alloc-replay", "allocateWorktree", { operationId: "candidate-op", ownerId: "workflow", cwd: repo });
			assert.equal((replay as any).data.generation, (allocated as any).data.generation);
			const gotWorktree = await request(events, "get-worktree", "getWorktree", { operationId: "candidate-op", ownerId: "workflow" });
			assert.equal((gotWorktree as any).data.worktree.path, (allocated as any).data.worktree.path);
			const acquired = await request(events, "acquire", "acquireCheckoutLease", { operationId: "lease-op", ownerId: "workflow", workflowRunId: "wf", nodeId: "integrate", attempt: 0, cwd: repo });
			assert.equal(acquired.success, true); const lease = (acquired as any).data;
			const gotLease = await request(events, "get-lease", "getCheckoutLease", { cwd: repo }); assert.equal((gotLease as any).data.lease.leaseId, lease.leaseId);
			const credential = { leaseId: lease.leaseId, operationId: lease.operationId, ownerId: lease.ownerId, generation: lease.generation, authToken: lease.authToken };
			const wrong = await request(events, "wrong-release", "releaseCheckoutLease", { ...credential, generation: "wrong", cwd: repo, terminalEvidence: { kind: "external-terminal", reference: "terminal", observedTerminalAt: Date.now() } });
			assert.equal(wrong.success, false); assert.equal((wrong as any).error.code, "authentication_failed");
			const released = await request(events, "release", "releaseCheckoutLease", { ...credential, cwd: repo, terminalEvidence: { kind: "external-terminal", reference: "terminal", observedTerminalAt: Date.now() } });
			assert.equal((released as any).data.state, "released");
			const orphan = await request(events, "orphan", "acquireCheckoutLease", { operationId: "orphan-op", ownerId: "workflow", workflowRunId: "wf", nodeId: "integrate", attempt: 1, cwd: repo }); const orphanLease = (orphan as any).data;
			const blockedRecovery = await request(events, "recover-blocked", "recoverCheckoutLease", { cwd: repo, leaseId: orphanLease.leaseId, generation: orphanLease.generation, humanConfirmed: true, humanActor: "operator", reason: "PID absent", terminalEvidence: { kind: "launch-failed", reference: "missing-pid", observedTerminalAt: Date.now() } });
			assert.equal((blockedRecovery as any).error.code, "evidence_required");
			const recovered = await request(events, "recover", "recoverCheckoutLease", { cwd: repo, leaseId: orphanLease.leaseId, generation: orphanLease.generation, humanConfirmed: true, humanActor: "operator", reason: "backend reviewed", terminalEvidence: { kind: "external-terminal", reference: "backend-terminal", observedTerminalAt: Date.now() } });
			assert.equal((recovered as any).data.state, "recovered");
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("enforces ordinary RPC spawn access and projects durable read authority", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-spawn-lease-"));
		try {
			const repo = createGitRepo(root); const events = new FakeEvents(); let calls = 0;
			const asyncRoot = path.join(root, "runs");
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(repo), asyncDirRoot: asyncRoot, checkoutLeaseRoot: path.join(root, "leases"), ownerInstance: "owner-spawn", execute: async () => {
				calls++;
				const runId = `run-${calls}`; const asyncDir = path.join(asyncRoot, runId);
				fs.mkdirSync(asyncDir, { recursive: true });
				fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId, sessionId: "session-123", mode: "single", state: "running", cwd: repo, startedAt: 100, lastUpdate: 100, steps: [{ agent: "worker", status: "running", startedAt: 100 }] }));
				return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [], asyncId: runId, asyncDir } } as any;
			} });
			const acquired = await request(events, "spawn-acquire", "acquireCheckoutLease", { operationId: "lease", ownerId: "workflow", workflowRunId: "wf", nodeId: "write", attempt: 0, cwd: repo }); const lease = (acquired as any).data;
			const unknown = await request(events, "unknown-spawn", "spawn", { agent: "worker", task: "unknown", cwd: repo }); assert.equal((unknown as any).error.code, "checkout_conflict");
			const write = await request(events, "write-spawn", "spawn", { agent: "worker", task: "write", cwd: repo, accessMode: "write" }); assert.equal((write as any).error.code, "checkout_conflict");
			const reader = await request(events, "read-spawn", "spawn", { agent: "worker", task: "read", cwd: repo, accessMode: "read" }); assert.equal(reader.success, true);
			const active = await request(events, "read-active", "getRun", { id: "run-1" });
			assert.deepEqual((active as any).data.authority, { access: "read", evidence: "observed" });
			events.emit(SUBAGENT_ASYNC_COMPLETE_EVENT, { runId: "run-1" });
			const settled = await request(events, "read-settled", "getRun", { id: "run-1" });
			assert.deepEqual((settled as any).data.authority, { access: "read", evidence: "observed" });
			const holder = await request(events, "holder-spawn", "spawn", { agent: "worker", task: "write", cwd: repo, accessMode: "write", checkoutLease: { leaseId: lease.leaseId, operationId: lease.operationId, ownerId: lease.ownerId, generation: lease.generation, authToken: lease.authToken } }); assert.equal(holder.success, true);
			assert.equal(calls, 2); bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("rejects stop requests for async runs from a different session", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-stop-session-"));
		try {
			const events = new FakeEvents();
			const asyncRoot = path.join(root, "runs");
			const resultsDir = path.join(root, "results");
			const asyncDir = path.join(asyncRoot, "run-other-session");
			let killCalls = 0;
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({
				runId: "run-other-session",
				sessionId: "other-session",
				mode: "single",
				state: "running",
				pid: 4242,
				startedAt: 100,
				lastUpdate: 100,
				steps: [{ agent: "worker", status: "running", startedAt: 100 }],
			}, null, 2), "utf-8");
			const bridge = registerSubagentRpcBridge({
				events,
				getContext: () => ctx(),
				execute: async () => assert.fail("stop should not call executor"),
				asyncDirRoot: asyncRoot,
				resultsDir,
				kill: () => {
					killCalls++;
					return true;
				},
				now: () => 150,
			});

			const reply = await request(events, "stop-other-session", "stop", { id: "run-other-session" });

			assert.equal(reply.success, false);
			assert.equal((reply as { error: { code: string; message: string } }).error.code, "not_found");
			assert.match((reply as { error: { message: string } }).error.message, /active session/);
			assert.equal(fs.existsSync(timeoutRequestPath(asyncDir)), false);
			assert.equal(killCalls, 0);

			bridge.dispose();
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});
});
