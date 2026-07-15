import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { WAIT_TOOL_ENABLED_ENV } from "../../src/runs/background/wait.ts";
import { SUBAGENT_CHILD_ENV, SUBAGENT_FANOUT_CHILD_ENV } from "../../src/runs/shared/pi-args.ts";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

function parentToolEnv(): NodeJS.ProcessEnv {
	const env = { ...process.env };
	delete env[SUBAGENT_CHILD_ENV];
	delete env[SUBAGENT_FANOUT_CHILD_ENV];
	delete env[WAIT_TOOL_ENABLED_ENV];
	return env;
}

describe("subagent extension child mode", () => {
	it("collapses tool detail before direct subagent tool execution", () => {
		const script = String.raw`
			import registerSubagentExtension from "./src/extension/index.ts";
			const events = { on() { return () => {}; }, emit() {} };
			let registeredTool;
			const fakePi = new Proxy({
				events,
				registerTool(tool) { if (tool.name === "subagent") registeredTool = tool; },
				registerCommand() {},
				registerShortcut() {},
				registerMessageRenderer() {},
				sendMessage() {},
				getSessionName() { return undefined; },
			}, {
				get(target, prop) {
					if (prop in target) return target[prop];
					return () => undefined;
				},
			});
			registerSubagentExtension(fakePi);
			if (!registeredTool) throw new Error("tool not registered");
			const calls = [];
			const ctx = {
				cwd: process.cwd(),
				hasUI: true,
				ui: {
					setToolsExpanded(value) { calls.push(value); },
					setWidget() {},
					requestRender() {},
					theme: { fg(_name, text) { return text; }, bg(_name, text) { return text; }, bold(text) { return text; } },
				},
				sessionManager: { getSessionId() { return "session-test"; }, getSessionFile() { return null; } },
				modelRegistry: { getAvailable() { return []; } },
			};
			await registeredTool.execute("collapse-check", { action: "list" }, new AbortController().signal, undefined, ctx);
			if (calls[0] !== false) throw new Error("expected setToolsExpanded(false), got " + JSON.stringify(calls));
		`;

		execFileSync(
			process.execPath,
			[
				"--experimental-transform-types",
				"--import",
				"./test/support/register-loader.mjs",
				"--input-type=module",
				"--eval",
				script,
			],
			{ cwd: projectRoot, env: parentToolEnv(), stdio: "pipe" },
		);
	});

	it("rejects an ordinary direct-tool writer behind an active workflow lease", () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagents-direct-lease-"));
		try {
			const repo = path.join(root, "repo"); fs.mkdirSync(repo);
			execFileSync("git", ["-C", repo, "init"]); execFileSync("git", ["-C", repo, "config", "user.email", "test@example.com"]); execFileSync("git", ["-C", repo, "config", "user.name", "Test"]);
			fs.writeFileSync(path.join(repo, "tracked.txt"), "initial\n"); execFileSync("git", ["-C", repo, "add", "tracked.txt"]); execFileSync("git", ["-C", repo, "commit", "--no-verify", "-m", "initial"]);
			const script = String.raw`
				import registerSubagentExtension from "./src/extension/index.ts";
				import { acquireCheckoutLease } from "./src/runs/shared/checkout-lease.ts";
				const repo = process.env.TEST_REPO;
				acquireCheckoutLease({ operationId: "lease", ownerId: "workflow", workflowRunId: "wf", nodeId: "write", attempt: 0, cwd: repo });
				let tool; const events = { on() { return () => {}; }, emit() {} };
				registerSubagentExtension(new Proxy({ events, registerTool(value) { if (value.name === "subagent") tool = value; }, registerCommand() {}, registerShortcut() {}, registerMessageRenderer() {}, sendMessage() {}, getSessionName() {} }, { get(target, prop) { return prop in target ? target[prop] : () => undefined; } }));
				const ctx = { cwd: repo, hasUI: false, ui: { setWidget() {}, requestRender() {}, theme: { fg(_n,t){return t}, bg(_n,t){return t}, bold(t){return t} } }, sessionManager: { getSessionId(){return "session"}, getSessionFile(){return null} }, modelRegistry: { getAvailable(){return []} } };
				try { await tool.execute("ordinary-write", { agent: "worker", task: "write" }, new AbortController().signal, undefined, ctx); throw new Error("expected conflict"); }
				catch (error) { if (error.code !== "conflict") throw error; }
			`;
			const env = parentToolEnv(); env.PI_CODING_AGENT_DIR = path.join(root, "agent"); env.TEST_REPO = repo;
			execFileSync(process.execPath, ["--experimental-transform-types", "--import", "./test/support/register-loader.mjs", "--input-type=module", "--eval", script], { cwd: projectRoot, env, stdio: "pipe" });
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("registered lifecycle settles a revived quarantined foreground admission on async completion", () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagents-foreground-lifecycle-"));
		try {
			const repo = path.join(root, "repo"); fs.mkdirSync(repo);
			execFileSync("git", ["-C", repo, "init"]); execFileSync("git", ["-C", repo, "config", "user.email", "test@example.com"]); execFileSync("git", ["-C", repo, "config", "user.name", "Test"]);
			fs.writeFileSync(path.join(repo, "tracked.txt"), "initial\n"); execFileSync("git", ["-C", repo, "add", "tracked.txt"]); execFileSync("git", ["-C", repo, "commit", "--no-verify", "-m", "initial"]);
			const script = String.raw`
				import registerSubagentExtension from "./src/extension/index.ts";
				import { admitOrdinaryCheckout, getCheckoutLease, recordOrdinaryExecutionOutcome, recordOrdinaryResumeOutcome } from "./src/runs/shared/checkout-lease.ts";
				import { SUBAGENT_ASYNC_COMPLETE_EVENT } from "./src/shared/types.ts";
				const handlers = new Map(); const events = { on(name, handler) { const list = handlers.get(name) ?? []; list.push(handler); handlers.set(name, list); return () => {}; }, emit(name, payload) { for (const handler of handlers.get(name) ?? []) handler(payload); } };
				const fakePi = new Proxy({ events, registerTool() {}, registerCommand() {}, registerShortcut() {}, registerMessageRenderer() {}, sendMessage() {}, getSessionName() {} }, { get(target, prop) { return prop in target ? target[prop] : () => undefined; } });
				registerSubagentExtension(fakePi);
				const repo = process.env.TEST_REPO;
				admitOrdinaryCheckout({ operationId: "foreground", ownerId: "ordinary", cwd: repo, access: "unknown" });
				recordOrdinaryExecutionOutcome({ operationId: "foreground", runId: "old-run", children: [{ interrupted: true }] });
				if (getCheckoutLease(repo).ordinary[0]?.state !== "quarantined") throw new Error("paused admission was not quarantined");
				recordOrdinaryResumeOutcome({ runId: "old-run", asyncId: "new-async", outcome: "revived" });
				if (getCheckoutLease(repo).ordinary[0]?.childRef !== "new-async") throw new Error("admission was not rebound");
				events.emit(SUBAGENT_ASYNC_COMPLETE_EVENT, { runId: "new-async" });
				if (getCheckoutLease(repo).ordinary.length !== 0) throw new Error("completion did not settle admission");
			`;
			const env = parentToolEnv(); env.PI_CODING_AGENT_DIR = path.join(root, "agent"); env.TEST_REPO = repo;
			execFileSync(process.execPath, ["--experimental-transform-types", "--import", "./test/support/register-loader.mjs", "--input-type=module", "--eval", script], { cwd: projectRoot, env, stdio: "pipe" });
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("does not show async badge for explicit foreground clarify chain calls", () => {
		const script = String.raw`
			import registerSubagentExtension from "./src/extension/index.ts";
			const events = { on() { return () => {}; }, emit() {} };
			let registeredTool;
			const fakePi = new Proxy({
				events,
				registerTool(tool) { if (tool.name === "subagent") registeredTool = tool; },
				registerCommand() {},
				registerShortcut() {},
				registerMessageRenderer() {},
				sendMessage() {},
				getSessionName() { return undefined; },
			}, {
				get(target, prop) {
					if (prop in target) return target[prop];
					return () => undefined;
				},
			});
			registerSubagentExtension(fakePi);
			if (!registeredTool) throw new Error("tool not registered");
			const theme = { fg(_name, text) { return text; }, bold(text) { return text; } };
			const asyncChain = registeredTool.renderCall({ chain: [{ agent: "worker" }, { agent: "reviewer" }], async: true }, theme).text;
			const asyncParallel = registeredTool.renderCall({ tasks: [{ agent: "worker" }, { agent: "reviewer", count: 2 }], async: true }, theme).text;
			const clarifyChain = registeredTool.renderCall({ chain: [{ agent: "worker" }, { agent: "reviewer" }], async: true, clarify: true }, theme).text;
			if (!asyncChain.includes("[async]")) throw new Error("expected async chain badge, got " + asyncChain);
			if (!asyncParallel.includes("parallel (3) [async]")) throw new Error("expected async parallel badge, got " + asyncParallel);
			if (clarifyChain.includes("[async]")) throw new Error("unexpected clarify async badge: " + clarifyChain);
		`;

		execFileSync(
			process.execPath,
			[
				"--experimental-transform-types",
				"--import",
				"./test/support/register-loader.mjs",
				"--input-type=module",
				"--eval",
				script,
			],
			{ cwd: projectRoot, env: parentToolEnv(), stdio: "pipe" },
		);
	});

	it("honors waitTool disabled config for the registered wait tool", () => {
		const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagents-wait-tool-config-"));
		try {
			const configDir = path.join(agentDir, "extensions", "subagent");
			fs.mkdirSync(configDir, { recursive: true });
			fs.writeFileSync(path.join(configDir, "config.json"), JSON.stringify({ waitTool: { enabled: false } }), "utf-8");

			const script = String.raw`
				import registerSubagentExtension from "./src/extension/index.ts";
				const events = { on() { return () => {}; }, emit() {} };
				let waitTool;
				const fakePi = new Proxy({
					events,
					registerTool(tool) { if (tool.name === "wait") waitTool = tool; },
					registerCommand() {},
					registerShortcut() {},
					registerMessageRenderer() {},
					sendMessage() {},
					getSessionName() { return undefined; },
				}, {
					get(target, prop) {
						if (prop in target) return target[prop];
						return () => undefined;
					},
				});
				registerSubagentExtension(fakePi);
				if (!waitTool) throw new Error("wait tool not registered");
				const result = await waitTool.execute("wait-disabled", {}, new AbortController().signal, undefined, {});
				process.stdout.write(JSON.stringify(result.content[0].text));
			`;

			const env = parentToolEnv();
			env.PI_CODING_AGENT_DIR = agentDir;
			const output = execFileSync(
				process.execPath,
				[
					"--experimental-transform-types",
					"--import",
					"./test/support/register-loader.mjs",
					"--input-type=module",
					"--eval",
					script,
				],
				{ cwd: projectRoot, env, encoding: "utf-8" },
			);
			assert.match(JSON.parse(output) as string, /disabled/i);
		} finally {
			fs.rmSync(agentDir, { recursive: true, force: true });
		}
	});

	it("returns before registering anything for non-fanout children", () => {
		const script = String.raw`
			import registerSubagentExtension from "./src/extension/index.ts";
			import { SUBAGENT_CHILD_ENV, SUBAGENT_FANOUT_CHILD_ENV } from "./src/runs/shared/pi-args.ts";
			process.env[SUBAGENT_CHILD_ENV] = "1";
			process.env[SUBAGENT_FANOUT_CHILD_ENV] = "0";
			const calls = [];
			const fakePi = new Proxy({}, {
				get(_target, prop) {
					return (..._args) => {
						calls.push(String(prop));
						return undefined;
					};
				},
			});
			registerSubagentExtension(fakePi);
			if (calls.length > 0) {
				throw new Error("Unexpected child-mode registrations: " + calls.join(", "));
			}
		`;

		execFileSync(
			process.execPath,
			[
				"--experimental-transform-types",
				"--import",
				"./test/support/register-loader.mjs",
				"--input-type=module",
				"--eval",
				script,
			],
			{ cwd: projectRoot, stdio: "pipe" },
		);
	});

	it("returns before registering anything for fanout children", () => {
		const script = String.raw`
			import registerSubagentExtension from "./src/extension/index.ts";
			import { SUBAGENT_CHILD_ENV, SUBAGENT_FANOUT_CHILD_ENV } from "./src/runs/shared/pi-args.ts";
			process.env[SUBAGENT_CHILD_ENV] = "1";
			process.env[SUBAGENT_FANOUT_CHILD_ENV] = "1";
			const calls = [];
			const fakePi = new Proxy({}, {
				get(target, prop) {
					if (prop in target) return target[prop];
					return (..._args) => {
						calls.push(String(prop));
						return undefined;
					};
				},
			});
			registerSubagentExtension(fakePi);
			if (calls.length > 0) {
				throw new Error("Unexpected child-mode registrations: " + calls.join(", "));
			}
		`;

		execFileSync(
			process.execPath,
			[
				"--experimental-transform-types",
				"--import",
				"./test/support/register-loader.mjs",
				"--input-type=module",
				"--eval",
				script,
			],
			{ cwd: projectRoot, stdio: "pipe" },
		);
	});

	it("does not double-register the child-safe subagent tool when index and fanout-child both load", () => {
		const script = String.raw`
			import registerSubagentExtension from "./src/extension/index.ts";
			import registerFanoutChildSubagentExtension from "./src/extension/fanout-child.ts";
			import { SUBAGENT_CHILD_ENV, SUBAGENT_FANOUT_CHILD_ENV } from "./src/runs/shared/pi-args.ts";
			process.env[SUBAGENT_CHILD_ENV] = "1";
			process.env[SUBAGENT_FANOUT_CHILD_ENV] = "1";

			const registeredNames = new Set();
			const registrations = [];
			function makePi(source) {
				return {
					events: { on() { return () => {}; }, emit() {} },
					registerTool(tool) {
						if (registeredNames.has(tool.name)) {
							throw new Error("Tool " + tool.name + " conflicts with " + source);
						}
						registeredNames.add(tool.name);
						registrations.push({ source, name: tool.name });
					},
					getSessionName() { return undefined; },
				};
			}

			registerSubagentExtension(makePi("index.ts"));
			registerFanoutChildSubagentExtension(makePi("fanout-child.ts"));
			if (registrations.length !== 1 || registrations[0].name !== "subagent" || registrations[0].source !== "fanout-child.ts") {
				throw new Error("expected only fanout-child.ts to register subagent, got " + JSON.stringify(registrations));
			}
		`;

		execFileSync(
			process.execPath,
			[
				"--experimental-transform-types",
				"--import",
				"./test/support/register-loader.mjs",
				"--input-type=module",
				"--eval",
				script,
			],
			{ cwd: projectRoot, stdio: "pipe" },
		);
	});

	it("lets fanout children call read-only list but blocks mutating management actions", () => {
		const script = String.raw`
			import registerFanoutChildSubagentExtension from "./src/extension/fanout-child.ts";
			import { SUBAGENT_CHILD_ENV, SUBAGENT_FANOUT_CHILD_ENV } from "./src/runs/shared/pi-args.ts";
			process.env[SUBAGENT_CHILD_ENV] = "1";
			process.env[SUBAGENT_FANOUT_CHILD_ENV] = "1";
			let registeredTool;
			const fakePi = {
				events: { on() { return () => {}; }, emit() {} },
				registerTool(tool) { registeredTool = tool; },
				getSessionName() { return undefined; },
			};
			registerFanoutChildSubagentExtension(fakePi);
			if (!registeredTool) throw new Error("tool not registered");
			const ctx = {
				cwd: process.cwd(),
				hasUI: false,
				sessionManager: { getSessionId() { return "session-test"; }, getSessionFile() { return null; } },
				modelRegistry: { getAvailable() { return []; } },
			};
			const list = await registeredTool.execute("list-check", { action: "list" }, new AbortController().signal, undefined, ctx);
			if (list.isError) throw new Error("list should be allowed: " + JSON.stringify(list.content));
			const create = await registeredTool.execute("create-check", { action: "create", config: { name: "x" } }, new AbortController().signal, undefined, ctx);
			if (!create.isError) throw new Error("create should be blocked");
			const text = create.content?.[0]?.text ?? "";
			if (!text.includes("not available from child-safe subagent fanout mode")) throw new Error("unexpected create error: " + text);
		`;

		execFileSync(
			process.execPath,
			[
				"--experimental-transform-types",
				"--import",
				"./test/support/register-loader.mjs",
				"--input-type=module",
				"--eval",
				script,
			],
			{ cwd: projectRoot, stdio: "pipe" },
		);
	});
});
