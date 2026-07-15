import * as fs from "node:fs";
import * as path from "node:path";
import { formatDuration, formatModelThinking, formatTokens, shortenPath } from "../../shared/formatters.ts";
import { formatActivityLabel } from "../../shared/status-format.ts";
import {
	ASYNC_DIR,
	RESULTS_DIR,
	SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
	type ActivityState,
	type AsyncJobStep,
	type AsyncStatus,
	type ForegroundResumeRun,
	type NestedRunSummary,
	type SubagentRunMode,
	type SubagentState,
} from "../../shared/types.ts";
import { readStatus } from "../../shared/utils.ts";
import { formatAsyncRunOutputPath, listAsyncRuns, type AsyncRunSummary } from "./async-status.ts";

export const SUBAGENT_STRUCTURED_READ_VERSION = 1 as const;

const DEFAULT_TRANSCRIPT_LINES = 80;
const MAX_TRANSCRIPT_LINES = 500;
const TRANSCRIPT_TAIL_BYTES = 256 * 1024;

export type SubagentRunKind = "foreground" | "async" | "nested" | "remembered-foreground";

export type SubagentControlAction = "status" | "transcript" | "interrupt" | "stop" | "steer" | "resume";

export type SubagentActionAvailability = "available" | "unsupported" | "unavailable";

export interface SubagentAllowedAction {
	action: SubagentControlAction;
	availability: SubagentActionAvailability;
	reason?: string;
}

export interface SubagentArtifactLinks {
	asyncDir?: string;
	outputPaths?: string[];
	sessionFile?: string;
	eventsPath?: string;
	logPath?: string;
	resultPath?: string;
	transcriptPath?: string;
}

export interface SubagentChildSnapshot {
	index: number;
	agent: string;
	state: string;
	sessionFile?: string;
	transcriptPath?: string;
	model?: string;
	thinking?: string;
	error?: string;
}

export interface SubagentRunSnapshot {
	version: typeof SUBAGENT_STRUCTURED_READ_VERSION;
	kind: SubagentRunKind;
	runId: string;
	lifecycleArtifactVersion?: number;
	ownerSessionId?: string;
	ownerInstance?: string;
	revision: string;
	state: string;
	mode?: SubagentRunMode | string;
	cwd?: string;
	parentRunId?: string;
	rootRunId?: string;
	agent?: string;
	agents?: string[];
	activityState?: ActivityState;
	lastActivityAt?: number;
	startedAt?: number;
	updatedAt?: number;
	endedAt?: number;
	error?: string;
	artifacts: SubagentArtifactLinks;
	children: SubagentChildSnapshot[];
	allowedActions: SubagentAllowedAction[];
}

export interface SubagentTranscriptPage {
	version: typeof SUBAGENT_STRUCTURED_READ_VERSION;
	runId: string;
	kind: "async" | "nested" | "result" | "remembered-foreground";
	state: string;
	mode?: string;
	index?: number;
	agent?: string;
	source: string;
	lines: string[];
	truncated: boolean;
	lineLimit: number;
	warnings: string[];
	artifacts: SubagentArtifactLinks;
	hint?: string;
	stepLabel?: string;
}

export interface SubagentCapabilities {
	version: typeof SUBAGENT_STRUCTURED_READ_VERSION;
	owner: "pi-subagents";
	lifecycleArtifactVersion: typeof SUBAGENT_LIFECYCLE_ARTIFACT_VERSION;
	structuredReads: {
		listRuns: true;
		getRun: true;
		getTranscript: true;
		getCapabilities: true;
	};
	rpcMethods: readonly ["ping", "status", "spawn", "interrupt", "stop"];
	toolActions: {
		status: true;
		fleet: true;
		transcript: true;
		interrupt: true;
		stop: true;
		steer: true;
		resume: true;
		wait: true;
	};
	controls: {
		takeover: false;
	};
}

type ForegroundControl = SubagentState["foregroundControls"] extends Map<string, infer T> ? T : never;

export interface StructuredReadDeps {
	asyncDirRoot?: string;
	resultsDir?: string;
	kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean;
	now?: () => number;
	state?: SubagentState;
	sessionRoots?: string[];
	childSafe?: boolean;
	ownerInstance?: string;
}

export interface TranscriptReadOptions {
	index?: number;
	lines?: number;
	sessionRoots?: string[];
}

interface TextTailResult {
	path: string;
	lines: string[];
	truncated: boolean;
	error?: string;
}

function transcriptLineLimit(value: number | undefined): number {
	if (value === undefined) return DEFAULT_TRANSCRIPT_LINES;
	if (!Number.isFinite(value)) return DEFAULT_TRANSCRIPT_LINES;
	return Math.max(1, Math.min(MAX_TRANSCRIPT_LINES, Math.trunc(value)));
}

function uniqueStrings(values: Array<string | undefined>): string[] {
	const seen = new Set<string>();
	const result: string[] = [];
	for (const value of values) {
		if (!value || seen.has(value)) continue;
		seen.add(value);
		result.push(value);
	}
	return result;
}

function resolveMaybeRelative(asyncDir: string, filePath: string | undefined): string | undefined {
	if (!filePath) return undefined;
	return path.resolve(asyncDir, filePath);
}

function pathWithin(base: string, candidate: string): boolean {
	const resolvedBase = path.resolve(base);
	const resolvedCandidate = path.resolve(candidate);
	return resolvedCandidate === resolvedBase || resolvedCandidate.startsWith(`${resolvedBase}${path.sep}`);
}

function getErrorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function isNotFoundError(error: unknown): boolean {
	return typeof error === "object"
		&& error !== null
		&& "code" in error
		&& (error as NodeJS.ErrnoException).code === "ENOENT";
}

function readTextTail(filePath: string, maxLines: number): TextTailResult {
	let stat: fs.Stats;
	try {
		stat = fs.statSync(filePath);
	} catch (error) {
		if (isNotFoundError(error)) return { path: filePath, lines: [], truncated: false };
		return { path: filePath, lines: [], truncated: false, error: getErrorMessage(error) };
	}
	if (stat.size === 0) return { path: filePath, lines: [], truncated: false };

	let fd: number | undefined;
	try {
		const bytesToRead = Math.min(stat.size, TRANSCRIPT_TAIL_BYTES);
		const start = stat.size - bytesToRead;
		const buffer = Buffer.alloc(bytesToRead);
		fd = fs.openSync(filePath, "r");
		const bytesRead = fs.readSync(fd, buffer, 0, bytesToRead, start);
		const content = buffer.subarray(0, bytesRead).toString("utf-8");
		let lines = content.split(/\r?\n/);
		if (start > 0 && lines.length > 0) lines = lines.slice(1);
		if (lines.at(-1) === "") lines = lines.slice(0, -1);
		return { path: filePath, lines: lines.slice(-maxLines), truncated: start > 0 || lines.length > maxLines };
	} catch (error) {
		return { path: filePath, lines: [], truncated: false, error: getErrorMessage(error) };
	} finally {
		if (fd !== undefined) fs.closeSync(fd);
	}
}

function readContainedTextTail(filePath: string, maxLines: number, trustedRoots: string[], label: string): TextTailResult {
	if (trustedRoots.length === 0) return { path: filePath, lines: [], truncated: false, error: `Refusing to read ${label} transcript path without a trusted root: ${filePath}` };
	const resolvedPath = path.resolve(filePath);
	if (!trustedRoots.some((root) => pathWithin(root, resolvedPath))) {
		return { path: filePath, lines: [], truncated: false, error: `Refusing to read ${label} transcript path outside trusted roots: ${filePath}` };
	}
	let lstat: fs.Stats;
	try {
		lstat = fs.lstatSync(resolvedPath);
	} catch (error) {
		if (isNotFoundError(error)) return { path: filePath, lines: [], truncated: false };
		return { path: filePath, lines: [], truncated: false, error: getErrorMessage(error) };
	}
	if (lstat.isSymbolicLink()) return { path: filePath, lines: [], truncated: false, error: `Refusing to read symlink ${label} transcript path: ${filePath}` };
	if (!lstat.isFile()) return { path: filePath, lines: [], truncated: false, error: `Refusing to read non-file ${label} transcript path: ${filePath}` };
	let realPath: string;
	let realRoots: string[];
	try {
		realPath = fs.realpathSync(resolvedPath);
		realRoots = trustedRoots.filter((root) => fs.existsSync(root)).map((root) => fs.realpathSync(root));
	} catch (error) {
		return { path: filePath, lines: [], truncated: false, error: getErrorMessage(error) };
	}
	if (!realRoots.some((root) => pathWithin(root, realPath))) {
		return { path: filePath, lines: [], truncated: false, error: `Refusing to read ${label} transcript path outside trusted roots: ${filePath}` };
	}
	return readTextTail(resolvedPath, maxLines);
}

function stringifyJsonPreview(value: unknown, maxLength = 240): string {
	let raw: string;
	if (typeof value === "string") raw = value;
	else raw = JSON.stringify(value);
	return raw.length > maxLength ? `${raw.slice(0, maxLength)}…` : raw;
}

function contentText(content: unknown): string {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return content.map((part) => {
		if (!part || typeof part !== "object") return "";
		const entry = part as { type?: unknown; text?: unknown; name?: unknown; toolName?: unknown; args?: unknown; result?: unknown; content?: unknown };
		if (typeof entry.text === "string") return entry.text;
		if (entry.type === "toolCall" || entry.type === "tool_call") {
			const name = typeof entry.name === "string" ? entry.name : typeof entry.toolName === "string" ? entry.toolName : "tool";
			return `[tool: ${name}${entry.args === undefined ? "" : ` ${stringifyJsonPreview(entry.args)}`}]`;
		}
		if (entry.type === "toolResult" || entry.type === "tool_result") {
			return `[tool result${entry.result === undefined ? "" : `: ${stringifyJsonPreview(entry.result)}`}]`;
		}
		if (entry.content !== undefined) return stringifyJsonPreview(entry.content);
		return "";
	}).filter(Boolean).join("\n");
}

function sessionMessageLine(record: unknown): string | undefined {
	if (!record || typeof record !== "object") return undefined;
	const outer = record as { message?: unknown; role?: unknown; content?: unknown; type?: unknown };
	const message = outer.message && typeof outer.message === "object" ? outer.message as { role?: unknown; content?: unknown } : outer;
	const role = typeof message.role === "string" ? message.role : undefined;
	if (!role) return undefined;
	const text = contentText(message.content).trim();
	if (!text) return undefined;
	return `${role}: ${text}`;
}

function readSessionTranscriptTail(sessionFile: string, maxLines: number, trustedRoots: string[]): { lines: string[]; warnings: string[] } {
	const tail = readContainedTextTail(sessionFile, Math.max(maxLines * 4, maxLines), trustedRoots, "session");
	const warnings: string[] = [];
	if (tail.error) warnings.push(`Session read failed for ${sessionFile}: ${tail.error}`);
	const lines: string[] = [];
	let malformed = 0;
	for (const line of tail.lines) {
		if (!line.trim()) continue;
		try {
			const parsed = JSON.parse(line) as unknown;
			const messageLine = sessionMessageLine(parsed);
			if (messageLine) lines.push(messageLine);
		} catch {
			malformed++;
		}
	}
	if (malformed > 0) warnings.push(`Skipped ${malformed} malformed session tail line${malformed === 1 ? "" : "s"}.`);
	return { lines: lines.slice(-maxLines), warnings };
}

function validateTranscriptIndex(index: number | undefined, steps: AsyncJobStep[]): number | undefined {
	if (index === undefined) return undefined;
	if (!Number.isInteger(index)) throw new Error("Transcript index must be an integer.");
	if (index < 0 || index >= steps.length) throw new Error(`Transcript index ${index} is out of range for ${steps.length} child step${steps.length === 1 ? "" : "s"}.`);
	return index;
}

function selectTranscriptStep(status: AsyncStatus, options: TranscriptReadOptions): { index?: number; step?: AsyncJobStep; hint?: string } {
	const steps = status.steps ?? [];
	let selectedIndex = validateTranscriptIndex(options.index, steps);
	if (selectedIndex === undefined) {
		if (status.state === "running" && typeof status.currentStep === "number" && status.currentStep >= 0 && status.currentStep < steps.length) {
			selectedIndex = status.currentStep;
		} else if (steps.length === 1) {
			selectedIndex = 0;
		}
	}
	const step = selectedIndex !== undefined ? steps[selectedIndex] : undefined;
	const hint = options.index === undefined && steps.length > 1
		? `Tip: pass index to inspect a specific child transcript (${steps.map((candidate, index) => `${index}=${candidate.agent}`).join(", ")}).`
		: undefined;
	return { index: selectedIndex, step, hint };
}

export function formatActivityFacts(input: {
	activityState?: ActivityState;
	lastActivityAt?: number;
	currentTool?: string;
	currentToolStartedAt?: number;
	currentPath?: string;
	turnCount?: number;
	toolCount?: number;
	tokens?: { total: number };
}): string | undefined {
	const facts: string[] = [];
	if (input.currentTool && input.currentToolStartedAt !== undefined) facts.push(`tool ${input.currentTool} ${formatDuration(Math.max(0, Date.now() - input.currentToolStartedAt))}`);
	else if (input.currentTool) facts.push(`tool ${input.currentTool}`);
	if (input.currentPath) facts.push(shortenPath(input.currentPath));
	if (input.turnCount !== undefined) facts.push(`${input.turnCount} turns`);
	if (input.toolCount !== undefined) facts.push(`${input.toolCount} tools`);
	if (input.tokens?.total) facts.push(`${formatTokens(input.tokens.total)} tok`);
	const activity = formatActivityLabel(input.lastActivityAt, input.activityState);
	return activity || facts.length ? [activity, ...facts].filter(Boolean).join(" | ") : undefined;
}

function stepStateLine(mode: SubagentRunMode, index: number | undefined, step: AsyncJobStep | undefined): string | undefined {
	if (index === undefined || !step) return undefined;
	const modelThinking = formatModelThinking(step.model, step.thinking);
	const parts = [
		`${mode === "parallel" ? "Agent" : "Step"}: ${index} (${step.agent})`,
		step.status,
		formatActivityFacts(step),
		modelThinking,
		step.error ? `error: ${step.error}` : undefined,
	].filter(Boolean);
	return parts.join(" | ");
}

function revisionFrom(...parts: Array<string | number | undefined>): string {
	return parts.filter((part) => part !== undefined && part !== "").join(":");
}

function hasExistingSessionFile(value: unknown): value is string {
	return typeof value === "string" && fs.existsSync(value);
}

function available(action: SubagentControlAction): SubagentAllowedAction {
	return { action, availability: "available" };
}

function unavailable(action: SubagentControlAction, reason: string): SubagentAllowedAction {
	return { action, availability: "unavailable", reason };
}

export function resolveAllowedActions(input: {
	kind: SubagentRunKind;
	state: string;
	hasSessionFile?: boolean;
}): SubagentAllowedAction[] {
	const actions: SubagentAllowedAction[] = [available("status"), available("transcript")];
	const live = input.state === "running" || input.state === "queued";
	const paused = input.state === "paused";
	const terminal = input.state === "complete" || input.state === "failed" || input.state === "completed"
		|| input.state === "remembered foreground"
		|| input.kind === "remembered-foreground";

	if (live) {
		actions.push(available("interrupt"));
		if (input.kind === "async") actions.push(available("stop"));
		else actions.push(unavailable("stop", "Stop is only supported for running async runs."));
		actions.push(available("steer"));
		actions.push(unavailable("resume", "Resume requires a paused or terminal run with a persisted session."));
	} else if (paused) {
		actions.push(unavailable("interrupt", "Run is already paused."));
		actions.push(unavailable("stop", "Stop only supports running async runs."));
		actions.push(unavailable("steer", "Steer requires a live running child."));
		actions.push(input.hasSessionFile
			? available("resume")
			: unavailable("resume", "No child session file was persisted."));
	} else if (terminal) {
		actions.push(unavailable("interrupt", "Run is terminal."));
		actions.push(unavailable("stop", "Stop only supports running async runs."));
		actions.push(unavailable("steer", "Steer requires a live running child."));
		actions.push(input.hasSessionFile
			? available("resume")
			: unavailable("resume", "No child session file was persisted."));
	} else {
		actions.push(unavailable("interrupt", `Unsupported state '${input.state}'.`));
		actions.push(unavailable("stop", `Unsupported state '${input.state}'.`));
		actions.push(unavailable("steer", `Unsupported state '${input.state}'.`));
		actions.push(unavailable("resume", `Unsupported state '${input.state}'.`));
	}

	return actions;
}

function allowedActionsForAsync(run: Pick<AsyncRunSummary, "state" | "sessionFile" | "steps">): SubagentAllowedAction[] {
	const hasSession = hasExistingSessionFile(run.sessionFile)
		|| (run.steps ?? []).some((step) => hasExistingSessionFile(step.sessionFile));
	return resolveAllowedActions({ kind: "async", state: run.state, hasSessionFile: hasSession });
}

function snapshotFromAsyncSummary(run: AsyncRunSummary, ownerInstance?: string): SubagentRunSnapshot {
	const outputPath = formatAsyncRunOutputPath(run);
	const eventsPath = path.join(run.asyncDir, "events.jsonl");
	const logPath = path.join(run.asyncDir, `subagent-log-${run.id}.md`);
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		kind: "async",
		runId: run.id,
		lifecycleArtifactVersion: SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
		ownerSessionId: run.sessionId,
		...(ownerInstance ? { ownerInstance } : {}),
		revision: revisionFrom(run.id, run.state, run.lastUpdate ?? run.endedAt ?? run.startedAt),
		state: run.state,
		mode: run.mode,
		cwd: run.cwd,
		activityState: run.activityState,
		lastActivityAt: run.lastActivityAt,
		startedAt: run.startedAt,
		updatedAt: run.lastUpdate,
		endedAt: run.endedAt,
		error: run.error,
		artifacts: {
			asyncDir: run.asyncDir,
			...(outputPath ? { outputPaths: [outputPath] } : {}),
			...(run.sessionFile ? { sessionFile: run.sessionFile } : {}),
			...(fs.existsSync(eventsPath) ? { eventsPath } : {}),
			...(fs.existsSync(logPath) ? { logPath } : {}),
		},
		children: run.steps.map((step) => ({
			index: step.index,
			agent: step.agent,
			state: step.status,
			...(step.sessionFile ? { sessionFile: step.sessionFile } : {}),
			...(step.model ? { model: step.model } : {}),
			...(step.thinking ? { thinking: step.thinking } : {}),
			...(step.error ? { error: step.error } : {}),
		})),
		allowedActions: allowedActionsForAsync(run),
	};
}

function snapshotFromAsyncStatus(status: AsyncStatus, asyncDir: string, ownerInstance?: string): SubagentRunSnapshot {
	const summaryLike: AsyncRunSummary = {
		id: status.runId,
		asyncDir,
		sessionId: status.sessionId,
		state: status.state,
		error: status.error,
		activityState: status.activityState,
		lastActivityAt: status.lastActivityAt,
		mode: status.mode,
		cwd: status.cwd,
		startedAt: status.startedAt,
		lastUpdate: status.lastUpdate,
		endedAt: status.endedAt,
		outputFile: status.outputFile,
		sessionFile: status.sessionFile,
		steps: (status.steps ?? []).map((step, index) => ({
			index,
			agent: step.agent,
			status: step.status,
			sessionFile: step.sessionFile,
			model: step.model,
			thinking: step.thinking,
			error: step.error,
		})),
	};
	return snapshotFromAsyncSummary(summaryLike, ownerInstance);
}

function snapshotFromForegroundControl(control: ForegroundControl, ownerInstance?: string): SubagentRunSnapshot {
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		kind: "foreground",
		runId: control.runId,
		...(ownerInstance ? { ownerInstance } : {}),
		revision: revisionFrom(control.runId, "running", control.updatedAt),
		state: "running",
		mode: control.mode,
		agent: control.currentAgent,
		activityState: control.currentActivityState,
		lastActivityAt: control.lastActivityAt,
		startedAt: control.startedAt,
		updatedAt: control.updatedAt,
		artifacts: {},
		children: control.currentAgent
			? [{
				index: control.currentIndex ?? 0,
				agent: control.currentAgent,
				state: "running",
			}]
			: [],
		allowedActions: resolveAllowedActions({ kind: "foreground", state: "running", hasSessionFile: false }),
	};
}

function snapshotFromRememberedForeground(run: ForegroundResumeRun, ownerInstance?: string): SubagentRunSnapshot {
	const hasSession = run.children.some((child) => child.status !== "detached" && hasExistingSessionFile(child.sessionFile));
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		kind: "remembered-foreground",
		runId: run.runId,
		...(ownerInstance ? { ownerInstance } : {}),
		revision: revisionFrom(run.runId, "remembered", run.updatedAt),
		state: "remembered foreground",
		mode: run.mode,
		cwd: run.cwd,
		updatedAt: run.updatedAt,
		artifacts: {
			...(run.children.find((child) => child.sessionFile)?.sessionFile
				? { sessionFile: run.children.find((child) => child.sessionFile)!.sessionFile }
				: {}),
		},
		children: run.children.map((child) => ({
			index: child.index,
			agent: child.agent,
			state: child.status,
			...(child.sessionFile ? { sessionFile: child.sessionFile } : {}),
			...(child.transcriptPath ? { transcriptPath: child.transcriptPath } : {}),
		})),
		allowedActions: resolveAllowedActions({ kind: "remembered-foreground", state: "remembered foreground", hasSessionFile: hasSession }),
	};
}

export function snapshotFromNestedRun(run: NestedRunSummary, rootRunId?: string, ownerInstance?: string): SubagentRunSnapshot {
	const hasSession = hasExistingSessionFile(run.sessionFile)
		|| (run.steps ?? []).some((step) => hasExistingSessionFile(step.sessionFile));
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		kind: "nested",
		runId: run.id,
		...(ownerInstance ? { ownerInstance } : {}),
		revision: revisionFrom(run.id, run.state, run.lastActivityAt ?? run.startedAt),
		state: run.state,
		mode: run.mode,
		parentRunId: run.parentRunId,
		rootRunId: rootRunId,
		agent: run.agent,
		agents: run.agents,
		activityState: run.activityState,
		lastActivityAt: run.lastActivityAt,
		startedAt: run.startedAt,
		error: run.error,
		artifacts: {
			...(run.asyncDir ? { asyncDir: run.asyncDir } : {}),
			...(run.sessionFile ? { sessionFile: run.sessionFile } : {}),
		},
		children: (run.steps ?? []).map((step, index) => ({
			index,
			agent: step.agent,
			state: step.status,
			...(step.sessionFile ? { sessionFile: step.sessionFile } : {}),
			...(step.error ? { error: step.error } : {}),
		})),
		allowedActions: resolveAllowedActions({ kind: "nested", state: run.state, hasSessionFile: hasSession }),
	};
}

export function getSubagentCapabilities(): SubagentCapabilities {
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		owner: "pi-subagents",
		lifecycleArtifactVersion: SUBAGENT_LIFECYCLE_ARTIFACT_VERSION,
		structuredReads: {
			listRuns: true,
			getRun: true,
			getTranscript: true,
			getCapabilities: true,
		},
		rpcMethods: ["ping", "status", "spawn", "interrupt", "stop"],
		toolActions: {
			status: true,
			fleet: true,
			transcript: true,
			interrupt: true,
			stop: true,
			steer: true,
			resume: true,
			wait: true,
		},
		controls: {
			takeover: false,
		},
	};
}

export function listSubagentRunSnapshots(deps: StructuredReadDeps = {}): SubagentRunSnapshot[] {
	if (deps.childSafe) return [];

	const asyncRuns = listAsyncRuns(deps.asyncDirRoot ?? ASYNC_DIR, {
		states: ["queued", "running"],
		sessionId: deps.state?.currentSessionId ?? undefined,
		resultsDir: deps.resultsDir ?? RESULTS_DIR,
		kill: deps.kill,
		now: deps.now,
	});
	const foreground = deps.state ? [...deps.state.foregroundControls.values()] : [];
	const snapshots = [
		...foreground.map((control) => snapshotFromForegroundControl(control, deps.ownerInstance)),
		...asyncRuns.map((run) => snapshotFromAsyncSummary(run, deps.ownerInstance)),
	];
	return snapshots.sort((left, right) => (right.updatedAt ?? right.startedAt ?? 0) - (left.updatedAt ?? left.startedAt ?? 0));
}

export function getSubagentRunSnapshotFromAsyncStatus(
	status: AsyncStatus,
	asyncDir: string,
	deps: Pick<StructuredReadDeps, "ownerInstance"> = {},
): SubagentRunSnapshot {
	return snapshotFromAsyncStatus(status, asyncDir, deps.ownerInstance);
}

export function getRememberedForegroundSnapshot(
	run: ForegroundResumeRun,
	deps: Pick<StructuredReadDeps, "ownerInstance"> = {},
): SubagentRunSnapshot {
	return snapshotFromRememberedForeground(run, deps.ownerInstance);
}

export function buildAsyncRunTranscriptPage(
	status: AsyncStatus,
	asyncDir: string,
	options: TranscriptReadOptions = {},
): SubagentTranscriptPage {
	const lineLimit = transcriptLineLimit(options.lines);
	const selected = selectTranscriptStep(status, options);
	const stepOutputPath = selected.index !== undefined ? path.join(asyncDir, `output-${selected.index}.log`) : undefined;
	const runOutputPath = resolveMaybeRelative(asyncDir, status.outputFile);
	const logPath = path.join(asyncDir, `subagent-log-${status.runId}.md`);
	const outputPaths = selected.index !== undefined
		? uniqueStrings([stepOutputPath, runOutputPath && stepOutputPath && path.resolve(runOutputPath) === path.resolve(stepOutputPath) ? runOutputPath : undefined])
		: uniqueStrings([runOutputPath]);
	const sessionFile = selected.index !== undefined ? selected.step?.sessionFile : status.sessionFile;
	const eventsPath = path.join(asyncDir, "events.jsonl");
	const warnings: string[] = [];
	let transcriptLines: string[] = [];
	let transcriptSource = "Transcript tail";
	let truncated = false;

	for (const outputPath of outputPaths) {
		const tail = readContainedTextTail(outputPath, lineLimit, [asyncDir], "output");
		if (tail.error) warnings.push(`Output read failed for ${tail.path}: ${tail.error}`);
		if (tail.lines.length === 0) continue;
		transcriptLines = tail.lines;
		transcriptSource = `Transcript tail from ${tail.path}`;
		truncated = tail.truncated;
		break;
	}
	if (transcriptLines.length === 0 && selected.step?.recentOutput?.length) {
		transcriptLines = selected.step.recentOutput.slice(-lineLimit);
		transcriptSource = "Recent output from status.json";
	}
	if (transcriptLines.length === 0 && sessionFile) {
		const sessionTail = readSessionTranscriptTail(sessionFile, lineLimit, options.sessionRoots ?? []);
		transcriptLines = sessionTail.lines;
		warnings.push(...sessionTail.warnings);
		if (transcriptLines.length > 0) transcriptSource = `Session transcript tail from ${sessionFile}`;
	}

	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		runId: status.runId,
		kind: "async",
		state: status.state,
		mode: status.mode,
		...(selected.index !== undefined ? { index: selected.index } : {}),
		...(selected.step ? { agent: selected.step.agent } : {}),
		source: transcriptSource,
		lines: transcriptLines,
		truncated,
		lineLimit,
		warnings,
		artifacts: {
			asyncDir,
			outputPaths,
			...(sessionFile ? { sessionFile } : {}),
			...(fs.existsSync(eventsPath) ? { eventsPath } : {}),
			...(fs.existsSync(logPath) ? { logPath } : {}),
		},
		...(selected.hint ? { hint: selected.hint } : {}),
		...(stepStateLine(status.mode, selected.index, selected.step)
			? { stepLabel: stepStateLine(status.mode, selected.index, selected.step) }
			: {}),
	};
}

export function buildNestedRunTranscriptPage(run: NestedRunSummary, options: TranscriptReadOptions = {}): SubagentTranscriptPage {
	if (run.asyncDir) {
		const status = readStatus(run.asyncDir);
		if (status) return buildAsyncRunTranscriptPage(status, run.asyncDir, options);
	}
	const lineLimit = transcriptLineLimit(options.lines);
	if (!run.sessionFile) {
		return {
			version: SUBAGENT_STRUCTURED_READ_VERSION,
			runId: run.id,
			kind: "nested",
			state: run.state,
			mode: run.mode,
			...(run.agent ? { agent: run.agent } : {}),
			source: "Transcript tail",
			lines: [],
			truncated: false,
			lineLimit,
			warnings: [],
			artifacts: {
				...(run.sessionFile ? { sessionFile: run.sessionFile } : {}),
			},
		};
	}
	const sessionTail = readSessionTranscriptTail(run.sessionFile, lineLimit, options.sessionRoots ?? []);
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		runId: run.id,
		kind: "nested",
		state: run.state,
		mode: run.mode,
		...(run.agent ? { agent: run.agent } : {}),
		source: `Session transcript tail from ${run.sessionFile}`,
		lines: sessionTail.lines,
		truncated: false,
		lineLimit,
		warnings: sessionTail.warnings,
		artifacts: {
			sessionFile: run.sessionFile,
		},
	};
}

export function buildAsyncResultTranscriptPage(data: {
	id?: string;
	runId?: string;
	state?: string;
	success?: boolean;
	summary?: string;
	output?: string;
	sessionFile?: string;
	agent?: string;
	exitCode?: number | null;
	results?: Array<{ agent?: string; output?: string; summary?: string; sessionFile?: string; state?: string; success?: boolean; exitCode?: number | null }>;
}, resultPath: string, options: TranscriptReadOptions = {}): SubagentTranscriptPage {
	const lineLimit = transcriptLineLimit(options.lines);
	const runId = data.runId ?? data.id ?? path.basename(resultPath, ".json");
	const children = Array.isArray(data.results)
		? data.results
		: data.agent
			? [{ agent: data.agent, output: data.output, summary: data.summary, sessionFile: data.sessionFile, state: data.state, success: data.success, exitCode: data.exitCode }]
			: [];
	let index = options.index;
	if (index !== undefined && !Number.isInteger(index)) throw new Error("Transcript index must be an integer.");
	if (index === undefined && children.length === 1) index = 0;
	if (index !== undefined && (index < 0 || index >= children.length)) throw new Error(`Transcript index ${index} is out of range for ${children.length} result child${children.length === 1 ? "" : "ren"}.`);
	const child = index !== undefined ? children[index] : undefined;
	const output = index !== undefined
		? child?.output ?? child?.summary ?? (children.length === 1 ? data.output ?? data.summary : undefined) ?? ""
		: data.output ?? data.summary ?? "";
	const allLines = output.split(/\r?\n/);
	const transcriptLines = allLines.slice(-lineLimit).filter((line) => line.trim());
	const sessionFile = child?.sessionFile ?? data.sessionFile;
	return {
		version: SUBAGENT_STRUCTURED_READ_VERSION,
		runId,
		kind: "result",
		state: data.state ?? (data.success ? "complete" : "failed"),
		...(index !== undefined ? { index } : {}),
		...(child?.agent ? { agent: child.agent } : {}),
		source: "Result transcript tail",
		lines: transcriptLines,
		truncated: allLines.length > lineLimit,
		lineLimit,
		warnings: [],
		artifacts: {
			...(sessionFile ? { sessionFile } : {}),
			resultPath,
		},
		...(index === undefined && children.length > 1
			? { hint: `Tip: pass index to inspect a specific child transcript (${children.map((candidate, childIndex) => `${childIndex}=${candidate.agent ?? "subagent"}`).join(", ")}).` }
			: {}),
		...(index !== undefined && child ? { stepLabel: `Child: ${index} (${child.agent ?? "subagent"})` } : {}),
	};
}

function appendKnownArtifacts(lines: string[], artifacts: SubagentArtifactLinks): void {
	const entries: string[] = [];
	for (const outputPath of artifacts.outputPaths ?? []) entries.push(`Output: ${outputPath}`);
	if (artifacts.sessionFile) entries.push(`Session: ${artifacts.sessionFile}`);
	if (artifacts.eventsPath) entries.push(`Events: ${artifacts.eventsPath}`);
	if (artifacts.logPath) entries.push(`Log: ${artifacts.logPath}`);
	if (artifacts.resultPath) entries.push(`Result: ${artifacts.resultPath}`);
	if (!entries.length) return;
	lines.push("Artifacts:");
	for (const entry of entries) lines.push(`  ${entry}`);
}

function appendTranscriptBody(lines: string[], sourceLabel: string, sourceLines: string[], truncated: boolean): void {
	lines.push(`${sourceLabel}${truncated ? " (tail truncated)" : ""}:`);
	if (sourceLines.length === 0) {
		lines.push("  (no transcript lines available yet)");
		return;
	}
	for (const line of sourceLines) lines.push(`  ${line}`);
}

export function renderAsyncRunTranscriptPage(page: SubagentTranscriptPage): string {
	const lines = [
		`Run: ${page.runId}`,
		`State: ${page.state}`,
		page.mode ? `Mode: ${page.mode}` : undefined,
		page.stepLabel,
		page.hint,
	].filter((line): line is string => Boolean(line));
	appendKnownArtifacts(lines, page.artifacts);
	if (page.warnings.length) {
		lines.push("Warnings:");
		for (const warning of page.warnings) lines.push(`  ${warning}`);
	}
	appendTranscriptBody(lines, page.source, page.lines, page.truncated);
	return lines.join("\n");
}

export function renderNestedRunTranscriptPage(page: SubagentTranscriptPage, run: NestedRunSummary): string {
	if (page.kind === "async") return renderAsyncRunTranscriptPage(page);
	const lines = [
		`Nested run: ${run.id}`,
		`State: ${run.state}`,
		run.mode ? `Mode: ${run.mode}` : undefined,
		run.agent ? `Agent: ${run.agent}` : run.agents?.length ? `Agents: ${run.agents.join(", ")}` : undefined,
	].filter((line): line is string => Boolean(line));
	appendKnownArtifacts(lines, page.artifacts);
	if (page.warnings.length) {
		lines.push("Warnings:");
		for (const warning of page.warnings) lines.push(`  ${warning}`);
	}
	appendTranscriptBody(lines, page.source, page.lines, page.truncated);
	return lines.join("\n");
}

export function renderAsyncResultTranscriptPage(page: SubagentTranscriptPage): string {
	const lines = [
		`Run: ${page.runId}`,
		`State: ${page.state}`,
		page.stepLabel,
		page.hint,
	].filter((line): line is string => Boolean(line));
	appendKnownArtifacts(lines, page.artifacts);
	appendTranscriptBody(lines, page.source, page.lines, page.truncated);
	return lines.join("\n");
}
