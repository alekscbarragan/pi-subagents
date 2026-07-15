import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Compile } from "typebox/compile";
import { resolveAsyncRunLocation } from "../runs/background/async-resume.ts";
import { deliverTimeoutRequest } from "../runs/background/control-channel.ts";
import {
	buildAsyncResultTranscriptPage,
	buildAsyncRunTranscriptPage,
	buildNestedRunTranscriptPage,
	getSubagentCapabilities,
	listSubagentRunSnapshots,
	type SubagentRunSnapshot,
} from "../runs/background/structured-reads.ts";
import { reconcileAsyncRun } from "../runs/background/stale-run-reconciler.ts";
import type { SubagentParamsLike } from "../runs/foreground/subagent-executor.ts";
import {
	acquireCheckoutLease,
	admitOrdinaryCheckout,
	bindOrdinaryCheckoutChild,
	CheckoutLeaseError,
	getCheckoutLease,
	recoverCheckoutLease,
	releaseCheckoutLease,
	settleOrdinaryCheckout,
	settleOrdinaryCheckoutByChild,
	type CheckoutTerminalEvidenceV1,
} from "../runs/shared/checkout-lease.ts";
import { allocateOwnerWorktree, getOwnerWorktree } from "../runs/shared/worktree.ts";
import {
	type Details,
	type SubagentControlOperationV1,
	type SubagentState,
	ASYNC_DIR,
	RESULTS_DIR,
	SUBAGENT_ASYNC_COMPLETE_EVENT,
	SUBAGENT_ASYNC_STARTED_EVENT,
	SUBAGENT_CONTROL_EVENT,
} from "../shared/types.ts";
import { readStatus } from "../shared/utils.ts";
import { writeAtomicJson } from "../shared/atomic-json.ts";
import {
	SubagentParams,
	SubagentRpcAcquireCheckoutLeaseV1,
	SubagentRpcAllocateWorktreeV1,
	SubagentRpcAwaitRunsV1,
	SubagentRpcControlV1,
	SubagentRpcEmptyV1,
	SubagentRpcGetCheckoutLeaseV1,
	SubagentRpcGetWorktreeV1,
	SubagentRpcLegacyControlV1,
	SubagentRpcRecoverCheckoutLeaseV1,
	SubagentRpcReleaseCheckoutLeaseV1,
	SubagentRpcRunTargetV1,
	SubagentRpcTranscriptV1,
} from "./schemas.ts";

export const SUBAGENT_RPC_PROTOCOL_VERSION = 1;
export const SUBAGENT_RPC_REQUEST_EVENT = "subagents:rpc:v1:request";
export const SUBAGENT_RPC_READY_EVENT = "subagents:rpc:v1:ready";
export const SUBAGENT_RPC_REPLY_EVENT_PREFIX = "subagents:rpc:v1:reply:";
export const SUBAGENT_LIFECYCLE_EVENT = "subagents:lifecycle:v1:event";

export const SUBAGENT_RPC_METHODS = [
	"ping",
	"status",
	"spawn",
	"interrupt",
	"stop",
	"listRuns",
	"getRun",
	"awaitRuns",
	"getTranscript",
	"getCapabilities",
	"steer",
	"resume",
	"allocateWorktree",
	"getWorktree",
	"acquireCheckoutLease",
	"getCheckoutLease",
	"releaseCheckoutLease",
	"recoverCheckoutLease",
] as const;
export type SubagentRpcMethod = typeof SUBAGENT_RPC_METHODS[number];

export interface SubagentRpcRequestEnvelope {
	version: typeof SUBAGENT_RPC_PROTOCOL_VERSION;
	requestId: string;
	method: SubagentRpcMethod;
	params?: unknown;
	source?: {
		extension?: string;
		[key: string]: unknown;
	};
}

export type SubagentRpcReplyEnvelope<T = unknown> = {
	version: typeof SUBAGENT_RPC_PROTOCOL_VERSION;
	requestId: string;
	method?: SubagentRpcMethod;
	success: true;
	data: T;
} | {
	version: typeof SUBAGENT_RPC_PROTOCOL_VERSION;
	requestId: string;
	method?: SubagentRpcMethod;
	success: false;
	error: {
		code: SubagentRpcErrorCode;
		message: string;
	};
};

type SubagentRpcErrorCode =
	| "invalid_request"
	| "invalid_params"
	| "unsupported_version"
	| "unsupported_method"
	| "no_active_session"
	| "execution_failed"
	| "not_found"
	| "invalid_state"
	| "revision_conflict"
	| "checkout_conflict"
	| "authentication_failed"
	| "evidence_required";

interface EventBus {
	on(event: string, handler: (data: unknown) => void): (() => void) | void;
	emit(event: string, data: unknown): void;
}

interface RegisterSubagentRpcBridgeOptions {
	events: EventBus;
	getContext: () => ExtensionContext | null;
	execute: (
		id: string,
		params: SubagentParamsLike,
		signal: AbortSignal,
		onUpdate: ((result: AgentToolResult<Details>) => void) | undefined,
		ctx: ExtensionContext,
	) => Promise<AgentToolResult<Details>>;
	state?: SubagentState;
	ownerInstance?: string;
	asyncDirRoot?: string;
	resultsDir?: string;
	kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	pollIntervalMs?: number;
	checkoutLeaseRoot?: string;
	worktreeAllocationRoot?: string;
	worktreeBaseDir?: string;
	worktreeSetupHook?: string;
	worktreeSetupHookTimeoutMs?: number;
}

class SubagentRpcError extends Error {
	readonly code: SubagentRpcErrorCode;

	constructor(code: SubagentRpcErrorCode, message: string) {
		super(message);
		this.name = "SubagentRpcError";
		this.code = code;
	}
}

const subagentParamsValidator = Compile(SubagentParams);
const rpcParamValidators = {
	empty: Compile(SubagentRpcEmptyV1),
	target: Compile(SubagentRpcRunTargetV1),
	legacyControl: Compile(SubagentRpcLegacyControlV1),
	control: Compile(SubagentRpcControlV1),
	awaitRuns: Compile(SubagentRpcAwaitRunsV1),
	transcript: Compile(SubagentRpcTranscriptV1),
	allocateWorktree: Compile(SubagentRpcAllocateWorktreeV1),
	getWorktree: Compile(SubagentRpcGetWorktreeV1),
	acquireCheckoutLease: Compile(SubagentRpcAcquireCheckoutLeaseV1),
	getCheckoutLease: Compile(SubagentRpcGetCheckoutLeaseV1),
	releaseCheckoutLease: Compile(SubagentRpcReleaseCheckoutLeaseV1),
	recoverCheckoutLease: Compile(SubagentRpcRecoverCheckoutLeaseV1),
};

export function subagentRpcReplyEvent(requestId: string): string {
	return `${SUBAGENT_RPC_REPLY_EVENT_PREFIX}${requestId}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function now(options: RegisterSubagentRpcBridgeOptions): number {
	return options.now?.() ?? Date.now();
}

function assertRequestId(value: unknown): string {
	if (typeof value !== "string" || value.trim().length === 0 || /[\r\n]/.test(value)) {
		throw new SubagentRpcError("invalid_request", "RPC requestId must be a non-empty string without newlines.");
	}
	return value;
}

function assertRecordParams(params: unknown, method: SubagentRpcMethod): Record<string, unknown> {
	if (params === undefined) return {};
	if (!isRecord(params)) throw new SubagentRpcError("invalid_params", `RPC ${method} params must be an object.`);
	return params;
}

function validateRpcParams(
	params: unknown,
	method: SubagentRpcMethod,
	validator: { Check(value: unknown): boolean; Errors(value: unknown): Iterable<{ message: string }> },
): Record<string, unknown> {
	const input = assertRecordParams(params, method);
	if (validator.Check(input)) return input;
	const messages = [...validator.Errors(input)]
		.slice(0, 4)
		.map((error) => error.message);
	throw new SubagentRpcError("invalid_params", `RPC ${method} params: ${messages.join("; ")}`);
}

function optionalString(input: Record<string, unknown>, key: string): string | undefined {
	const value = input[key];
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !value.trim()) {
		throw new SubagentRpcError("invalid_params", `${key} must be a non-empty string.`);
	}
	return value;
}

function optionalInteger(input: Record<string, unknown>, key: string): number | undefined {
	const value = input[key];
	if (value === undefined) return undefined;
	if (!Number.isInteger(value) || (value as number) < 0) {
		throw new SubagentRpcError("invalid_params", `${key} must be a non-negative integer.`);
	}
	return value as number;
}

function assertSubagentParams(params: SubagentParamsLike, label: string): void {
	if (subagentParamsValidator.Check(params)) return;
	const messages = [...subagentParamsValidator.Errors(params)]
		.slice(0, 4)
		.map((error) => error.message);
	throw new SubagentRpcError("invalid_params", `${label}: ${messages.join("; ") || "invalid subagent parameters"}`);
}

function textFromToolResult(result: AgentToolResult<Details>): string {
	return result.content
		.filter((part): part is { type: "text"; text: string } => part.type === "text")
		.map((part) => part.text)
		.join("\n");
}

function dataFromToolResult(result: AgentToolResult<Details>): { text: string; details?: Details; isError?: boolean } {
	return {
		text: textFromToolResult(result),
		...(result.details ? { details: result.details } : {}),
		...(result.isError ? { isError: true } : {}),
	};
}

function failIfToolError(result: AgentToolResult<Details>): void {
	if (!result.isError) return;
	throw new SubagentRpcError("execution_failed", textFromToolResult(result) || "Subagent RPC execution failed.");
}

function normalizeTargetParams(params: unknown, method: SubagentRpcMethod): Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> {
	const input = assertRecordParams(params, method);
	const output: Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> = {};
	if (input.id !== undefined) output.id = input.id as string;
	if (input.runId !== undefined) output.runId = input.runId as string;
	if (input.dir !== undefined) output.dir = input.dir as string;
	if (input.index !== undefined) output.index = input.index as number;
	return output;
}

function sessionData(ctx: ExtensionContext | null): { cwd?: string; sessionId?: string; sessionFile?: string | null } {
	if (!ctx) return {};
	return {
		cwd: ctx.cwd,
		sessionId: ctx.sessionManager.getSessionId() ?? undefined,
		sessionFile: ctx.sessionManager.getSessionFile() ?? null,
	};
}

function ownerInstance(options: RegisterSubagentRpcBridgeOptions): string {
	return options.ownerInstance ?? "pi-subagents";
}

function leaseOptions(options: RegisterSubagentRpcBridgeOptions) {
	return { root: options.checkoutLeaseRoot, now: options.now };
}

function validateTerminalEvidence(evidence: CheckoutTerminalEvidenceV1, options: RegisterSubagentRpcBridgeOptions): void {
	if (evidence.kind !== "owner-run") return;
	const run = findRun(options, evidence.reference);
	if (!run.settlement.terminal || run.settlement.output === "pending" || run.settlement.artifacts === "pending") {
		throw new SubagentRpcError("evidence_required", `Owner run '${evidence.reference}' has not reached canonical terminal settlement.`);
	}
}

function pingData(ctx: ExtensionContext | null, options: RegisterSubagentRpcBridgeOptions) {
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		methods: [...SUBAGENT_RPC_METHODS],
		capabilities: getSubagentCapabilities(),
		events: {
			ready: SUBAGENT_RPC_READY_EVENT,
			request: SUBAGENT_RPC_REQUEST_EVENT,
			replyPrefix: SUBAGENT_RPC_REPLY_EVENT_PREFIX,
			lifecycle: SUBAGENT_LIFECYCLE_EVENT,
		},
		ownerInstance: ownerInstance(options),
		session: sessionData(ctx),
	};
}

async function executeChecked(
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	requestId: string,
	method: SubagentRpcMethod,
	params: SubagentParamsLike,
): Promise<{ text: string; details?: Details; isError?: boolean }> {
	assertSubagentParams(params, `RPC ${method} params`);
	const controller = new AbortController();
	const result = await options.execute(`rpc-${method}-${requestId}`, params, controller.signal, undefined, ctx);
	failIfToolError(result);
	return dataFromToolResult(result);
}

function spawnParams(params: unknown): SubagentParamsLike {
	const input = assertRecordParams(params, "spawn");
	if (input.action !== undefined) {
		throw new SubagentRpcError("invalid_params", "RPC spawn does not accept management/control actions. Use status or interrupt RPC methods instead.");
	}
	if (input.async === false) {
		throw new SubagentRpcError("invalid_params", "RPC spawn only supports detached async launches; omit async or set async: true.");
	}
	if (input.clarify === true) {
		throw new SubagentRpcError("invalid_params", "RPC spawn cannot open the clarify UI; omit clarify or set clarify: false.");
	}
	return { ...(input as SubagentParamsLike), async: true, clarify: false };
}

async function executeSpawnChecked(
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	requestId: string,
	params: unknown,
): Promise<{ text: string; details?: Details; isError?: boolean }> {
	const spawn = spawnParams(params);
	assertSubagentParams(spawn, "RPC spawn params");
	const operationId = `rpc-spawn:${requestId}`;
	const cwd = spawn.cwd ? path.resolve(ctx.cwd, spawn.cwd) : ctx.cwd;
	let admission;
	try {
		admission = admitOrdinaryCheckout({
			operationId,
			ownerId: ownerInstance(options),
			cwd,
			access: spawn.accessMode ?? "unknown",
			...(spawn.checkoutLease ? { lease: spawn.checkoutLease } : {}),
		}, leaseOptions(options));
	} catch (error) {
		if (!(error instanceof CheckoutLeaseError) || error.code !== "invalid_state" || spawn.checkoutLease) throw error;
		// Ordinary launches outside a git checkout retain their historical behavior.
	}
	try {
		const result = await options.execute(`rpc-spawn-${requestId}`, spawn, new AbortController().signal, undefined, ctx);
		failIfToolError(result);
		const data = dataFromToolResult(result);
		if (admission && data.details?.asyncId) bindOrdinaryCheckoutChild(operationId, data.details.asyncId, leaseOptions(options));
		else if (admission) settleOrdinaryCheckout(operationId, { kind: "external-terminal", reference: `rpc-spawn:${requestId}:returned`, observedTerminalAt: now(options), details: "Owner executor returned without a detached child reference." }, leaseOptions(options));
		return data;
	} catch (error) {
		if (admission) settleOrdinaryCheckout(operationId, { kind: "launch-failed", reference: `rpc-spawn:${requestId}:failed`, observedTerminalAt: now(options), details: error instanceof Error ? error.message : String(error) }, leaseOptions(options));
		throw error;
	}
}

function snapshots(options: RegisterSubagentRpcBridgeOptions): SubagentRunSnapshot[] {
	const runs = listSubagentRunSnapshots({
		asyncDirRoot: options.asyncDirRoot,
		resultsDir: options.resultsDir,
		kill: options.kill,
		now: options.now,
		state: options.state,
		ownerInstance: ownerInstance(options),
	});
	const operations = readOperationsByRun(options);
	return runs.map((run) => ({ ...run, operations: operations.get(run.runId) ?? [] }));
}

function targetId(input: Record<string, unknown>): string {
	const id = optionalString(input, "id") ?? optionalString(input, "runId");
	if (!id) throw new SubagentRpcError("invalid_params", "id or runId is required.");
	return id;
}

function findRun(options: RegisterSubagentRpcBridgeOptions, id: string): SubagentRunSnapshot {
	const matches = snapshots(options).filter((run) => run.runId === id || run.runId.startsWith(id));
	const exact = matches.find((run) => run.runId === id);
	if (exact) return exact;
	if (matches.length === 0) throw new SubagentRpcError("not_found", `Run '${id}' was not found.`);
	if (matches.length > 1) throw new SubagentRpcError("invalid_params", `Run id prefix '${id}' is ambiguous.`);
	return matches[0]!;
}

function operationDir(options: RegisterSubagentRpcBridgeOptions): string {
	return path.join(options.resultsDir ?? RESULTS_DIR, ".rpc-operations");
}

function operationPath(options: RegisterSubagentRpcBridgeOptions, operationId: string): string {
	return path.join(operationDir(options), `${Buffer.from(operationId).toString("base64url")}.json`);
}

function readOperation(
	options: RegisterSubagentRpcBridgeOptions,
	operationId: string,
): SubagentControlOperationV1 | undefined {
	try {
		return JSON.parse(fs.readFileSync(operationPath(options, operationId), "utf-8")) as SubagentControlOperationV1;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}

function readOperationsByRun(options: RegisterSubagentRpcBridgeOptions): Map<string, SubagentControlOperationV1[]> {
	const byRun = new Map<string, SubagentControlOperationV1[]>();
	const dir = operationDir(options);
	if (!fs.existsSync(dir)) return byRun;
	for (const name of fs.readdirSync(dir)) {
		try {
			const value = JSON.parse(fs.readFileSync(path.join(dir, name), "utf-8")) as SubagentControlOperationV1;
			const values = byRun.get(value.runId) ?? [];
			values.push(value);
			byRun.set(value.runId, values);
		} catch {
			// Ignore malformed operation evidence while preserving other queryable outcomes.
		}
	}
	for (const values of byRun.values()) {
		values.sort((a, b) => a.createdAt - b.createdAt);
	}
	return byRun;
}

function writeOperation(options: RegisterSubagentRpcBridgeOptions, operation: SubagentControlOperationV1): void {
	writeAtomicJson(operationPath(options, operation.operationId), operation);
}

function ownerControlOutcome(
	result: AgentToolResult<Details>,
	action: "interrupt" | "steer" | "resume",
	runId: string,
): SubagentControlOperationV1["outcome"] {
	const control = result.details?.control;
	if (!control || control.action !== action || (control.runId && control.runId !== runId)) {
		throw new SubagentRpcError("execution_failed", `Owner did not report a valid structured ${action} outcome for run '${runId}'.`);
	}
	if (result.isError && control.outcome !== "unsupported") {
		throw new SubagentRpcError("execution_failed", `Owner ${action} operation failed.`);
	}
	return control.outcome;
}

async function controlOperation(
	request: SubagentRpcRequestEnvelope,
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
): Promise<SubagentControlOperationV1> {
	const input = validateRpcParams(request.params, request.method, rpcParamValidators.control);
	const runId = targetId(input);
	const operationId = optionalString(input, "operationId")!;
	const prior = readOperation(options, operationId);
	if (prior) {
		if (prior.runId !== runId || prior.action !== request.method) {
			throw new SubagentRpcError("invalid_params", `operationId '${operationId}' is already bound to another control.`);
		}
		return prior;
	}

	const run = findRun(options, runId);
	const expectedRevision = optionalString(input, "expectedRevision");
	let operation: SubagentControlOperationV1 = {
		version: 1,
		operationId,
		action: request.method as SubagentControlOperationV1["action"],
		runId: run.runId,
		expectedRevision,
		observedRevision: run.revision,
		state: "accepted",
		outcome: "queued",
		createdAt: now(options),
		updatedAt: now(options),
	};
	if (run.evidence.status === "invalid") {
		operation = {
			...operation,
			state: "rejected",
			outcome: "failed",
			message: run.evidence.failures.join(" "),
			updatedAt: now(options),
		};
		writeOperation(options, operation);
		return operation;
	}
	if (expectedRevision && expectedRevision !== run.revision) {
		operation = {
			...operation,
			state: "rejected",
			outcome: "revision_conflict",
			message: `Expected revision '${expectedRevision}', observed '${run.revision}'.`,
			updatedAt: now(options),
		};
		writeOperation(options, operation);
		return operation;
	}
	const availability = run.allowedActions.find((entry) => entry.action === request.method);
	if (availability && availability.availability !== "available") {
		operation = {
			...operation,
			state: "completed",
			outcome: "unsupported",
			message: availability.reason ?? `Owner reported ${request.method} unsupported.`,
			updatedAt: now(options),
		};
		writeOperation(options, operation);
		return operation;
	}

	writeOperation(options, operation);
	try {
		let outcome: SubagentControlOperationV1["outcome"];
		if (request.method === "stop") {
			stopAsyncRun({ ...input, id: run.runId }, options, ctx);
			outcome = "delivered";
		} else {
			const params: SubagentParamsLike = {
				action: request.method,
				id: run.runId,
				...(input.index !== undefined ? { index: input.index as number } : {}),
				...(typeof input.message === "string" ? { message: input.message } : {}),
			};
			const controller = new AbortController();
			const result = await options.execute(
				`rpc-${request.method}-${operationId}`,
				params,
				controller.signal,
				undefined,
				ctx,
			);
			outcome = ownerControlOutcome(result, request.method, run.runId);
		}
		operation = {
			...operation,
			state: "completed",
			outcome,
			...(outcome === "unsupported" ? { message: `Owner reported ${request.method} unsupported.` } : {}),
			updatedAt: now(options),
		};
	} catch (error) {
		operation = {
			...operation,
			state: "failed",
			outcome: "failed",
			message: error instanceof Error ? error.message : String(error),
			updatedAt: now(options),
		};
	}
	writeOperation(options, operation);
	return operation;
}

function stopAsyncRun(
	params: unknown,
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
): { runId: string; asyncDir: string; previousState: string; state: "stopping"; message: string } {
	const target = normalizeTargetParams(params, "stop");
	assertSubagentParams({ action: "status", ...target }, "RPC stop target params");
	const asyncDirRoot = options.asyncDirRoot ?? ASYNC_DIR;
	const resultsDir = options.resultsDir ?? RESULTS_DIR;
	let location;
	try {
		location = resolveAsyncRunLocation(target, asyncDirRoot, resultsDir);
	} catch (error) {
		throw new SubagentRpcError("invalid_params", error instanceof Error ? error.message : String(error));
	}
	if (!location.asyncDir) {
		throw new SubagentRpcError("not_found", "Async run not found or already completed; stop requires a live async run directory.");
	}

	const currentSessionId = ctx.sessionManager.getSessionId();
	const initialStatus = readStatus(location.asyncDir);
	const initialRunId = initialStatus?.runId ?? location.resolvedId ?? path.basename(location.asyncDir);
	if (!initialStatus) throw new SubagentRpcError("not_found", `Status file not found for async run '${initialRunId}'.`);
	if (!currentSessionId || initialStatus.sessionId !== currentSessionId) {
		throw new SubagentRpcError("not_found", `Async run '${initialRunId}' was not found in the active session.`);
	}

	let status;
	try {
		status = reconcileAsyncRun(location.asyncDir, { resultsDir, kill: options.kill, now: options.now }).status;
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}
	const runId = status?.runId ?? initialRunId;
	if (!status) throw new SubagentRpcError("not_found", `Status file not found for async run '${runId}'.`);
	if (status.sessionId !== currentSessionId) {
		throw new SubagentRpcError("not_found", `Async run '${runId}' was not found in the active session.`);
	}
	if (status.state !== "running") {
		throw new SubagentRpcError("invalid_state", `Async run ${runId} is ${status.state}; stop only supports running async runs.`);
	}

	try {
		deliverTimeoutRequest({
			asyncDir: location.asyncDir,
			pid: status.pid,
			kill: options.kill,
			now: options.now,
			source: "rpc-stop",
		});
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}

	return {
		runId,
		asyncDir: location.asyncDir,
		previousState: status.state,
		state: "stopping",
		message: `Stop requested for async run ${runId}.`,
	};
}

async function awaitRuns(params: unknown, options: RegisterSubagentRpcBridgeOptions) {
	const input = validateRpcParams(params, "awaitRuns", rpcParamValidators.awaitRuns);
	const ids = Array.isArray(input.runIds)
		? input.runIds.map((value) => {
			if (typeof value !== "string" || !value) {
				throw new SubagentRpcError("invalid_params", "runIds must contain non-empty strings.");
			}
			return value;
		})
		: [targetId(input)];
	const revisions = isRecord(input.revisions) ? input.revisions : {};
	const timeoutMs = optionalInteger(input, "timeoutMs") ?? 30 * 60 * 1000;
	const started = now(options);
	while (true) {
		const runs = ids.map((id) => findRun(options, id));
		const terminal = runs.some((run) => run.settlement.terminal);
		const changed = runs.some((run) => (
			typeof revisions[run.runId] === "string" && revisions[run.runId] !== run.revision
		));
		if (terminal || changed || now(options) - started >= timeoutMs) {
			return {
				reason: terminal ? "terminal" : changed ? "changed" : "timeout",
				runs,
			};
		}
		const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
		await sleep(Math.max(10, options.pollIntervalMs ?? 250));
	}
}

function transcript(params: unknown, options: RegisterSubagentRpcBridgeOptions) {
	const input = validateRpcParams(params, "getTranscript", rpcParamValidators.transcript);
	const run = findRun(options, targetId(input));
	const index = optionalInteger(input, "index");
	const lines = optionalInteger(input, "lines");
	if (run.kind === "remembered-foreground") {
		throw new SubagentRpcError("invalid_state", "Remembered foreground transcript paging is unavailable; inspect its artifact links.");
	}
	if (run.kind === "nested") {
		const candidates = [
			...(options.state?.foregroundControls.values() ?? []),
			...(options.state?.asyncJobs.values() ?? []),
		].flatMap((entry) => entry.nestedChildren ?? []);
		const nested = candidates.find((entry) => entry.id === run.runId);
		if (!nested) throw new SubagentRpcError("not_found", `Nested run '${run.runId}' was not found.`);
		return buildNestedRunTranscriptPage(nested, { index, lines });
	}
	const location = resolveAsyncRunLocation(
		{ id: run.runId },
		options.asyncDirRoot ?? ASYNC_DIR,
		options.resultsDir ?? RESULTS_DIR,
	);
	if (location.asyncDir) {
		const status = readStatus(location.asyncDir);
		if (status) return buildAsyncRunTranscriptPage(status, location.asyncDir, { index, lines });
	}
	if (location.resultPath) {
		const data = JSON.parse(fs.readFileSync(location.resultPath, "utf-8"));
		return buildAsyncResultTranscriptPage(data, location.resultPath, { index, lines });
	}
	throw new SubagentRpcError("not_found", `Transcript for '${run.runId}' was not found.`);
}

async function handleRequest(
	request: SubagentRpcRequestEnvelope,
	options: RegisterSubagentRpcBridgeOptions,
): Promise<unknown> {
	const ctx = options.getContext();
	if (request.method === "ping") return pingData(ctx, options);
	if (!ctx) throw new SubagentRpcError("no_active_session", "No active extension context for subagent RPC.");

	if (request.method === "spawn") {
		return executeSpawnChecked(options, ctx, request.requestId, request.params);
	}
	if (request.method === "status") {
		return executeChecked(options, ctx, request.requestId, request.method, {
			action: "status",
			...normalizeTargetParams(request.params, "status"),
		});
	}
	if (["interrupt", "stop", "steer", "resume"].includes(request.method)) {
		const rawInput = assertRecordParams(request.params, request.method);
		const legacy = rawInput.operationId === undefined && rawInput.expectedRevision === undefined;
		if (legacy && request.method === "interrupt") {
			validateRpcParams(request.params, request.method, rpcParamValidators.legacyControl);
			return executeChecked(options, ctx, request.requestId, request.method, {
				action: "interrupt",
				...normalizeTargetParams(request.params, "interrupt"),
			});
		}
		if (legacy && request.method === "stop") {
			validateRpcParams(request.params, request.method, rpcParamValidators.legacyControl);
			return stopAsyncRun(request.params, options, ctx);
		}
		return controlOperation(request, options, ctx);
	}
	if (request.method === "getCapabilities") {
		validateRpcParams(request.params, request.method, rpcParamValidators.empty);
		return getSubagentCapabilities();
	}
	if (request.method === "listRuns") {
		validateRpcParams(request.params, request.method, rpcParamValidators.empty);
		return { version: 1, runs: snapshots(options) };
	}
	if (request.method === "getRun") {
		const input = validateRpcParams(request.params, "getRun", rpcParamValidators.target);
		return findRun(options, targetId(input));
	}
	if (request.method === "awaitRuns") return awaitRuns(request.params, options);
	if (request.method === "getTranscript") return transcript(request.params, options);
	if (request.method === "allocateWorktree") {
		const input = validateRpcParams(request.params, request.method, rpcParamValidators.allocateWorktree) as any;
		return allocateOwnerWorktree(input, {
			root: options.worktreeAllocationRoot, now: options.now, worktreeBaseDir: options.worktreeBaseDir,
			...(options.worktreeSetupHook ? { setupHook: { hookPath: options.worktreeSetupHook, timeoutMs: options.worktreeSetupHookTimeoutMs } } : {}),
		});
	}
	if (request.method === "getWorktree") {
		const input = validateRpcParams(request.params, request.method, rpcParamValidators.getWorktree);
		const allocation = getOwnerWorktree(input.operationId as string, input.ownerId as string, { root: options.worktreeAllocationRoot, now: options.now });
		if (!allocation) throw new SubagentRpcError("not_found", `Worktree allocation '${String(input.operationId)}' was not found.`);
		return allocation;
	}
	if (request.method === "acquireCheckoutLease") {
		const input = validateRpcParams(request.params, request.method, rpcParamValidators.acquireCheckoutLease) as any;
		return acquireCheckoutLease(input, leaseOptions(options));
	}
	if (request.method === "getCheckoutLease") {
		const input = validateRpcParams(request.params, request.method, rpcParamValidators.getCheckoutLease);
		return getCheckoutLease(input.cwd as string, leaseOptions(options));
	}
	if (request.method === "releaseCheckoutLease") {
		const input = validateRpcParams(request.params, request.method, rpcParamValidators.releaseCheckoutLease) as any;
		validateTerminalEvidence(input.terminalEvidence, options);
		return releaseCheckoutLease(input, leaseOptions(options));
	}
	if (request.method === "recoverCheckoutLease") {
		const input = validateRpcParams(request.params, request.method, rpcParamValidators.recoverCheckoutLease) as any;
		validateTerminalEvidence(input.terminalEvidence, options);
		return recoverCheckoutLease(input, leaseOptions(options));
	}
	throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC method: ${String(request.method)}`);
}

function parseRequest(raw: unknown): SubagentRpcRequestEnvelope {
	if (!isRecord(raw)) throw new SubagentRpcError("invalid_request", "Subagent RPC request must be an object.");
	const requestId = assertRequestId(raw.requestId);
	if (raw.version !== SUBAGENT_RPC_PROTOCOL_VERSION) {
		throw new SubagentRpcError("unsupported_version", `Unsupported subagent RPC version: ${String(raw.version)}.`);
	}
	if (typeof raw.method !== "string" || !(SUBAGENT_RPC_METHODS as readonly string[]).includes(raw.method)) {
		throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC method: ${String(raw.method)}.`);
	}
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		requestId,
		method: raw.method as SubagentRpcMethod,
		...(raw.params !== undefined ? { params: raw.params } : {}),
		...(isRecord(raw.source) ? { source: raw.source as SubagentRpcRequestEnvelope["source"] } : {}),
	};
}

function safeReplyRequestId(raw: unknown): string {
	if (!isRecord(raw)) return "unknown";
	const requestId = raw.requestId;
	return typeof requestId === "string" && requestId.trim().length > 0 && !/[\r\n]/.test(requestId)
		? requestId
		: "unknown";
}

function errorReply(raw: unknown, error: unknown): SubagentRpcReplyEnvelope {
	const requestId = safeReplyRequestId(raw);
	const method = isRecord(raw) && typeof raw.method === "string" && (SUBAGENT_RPC_METHODS as readonly string[]).includes(raw.method)
		? raw.method as SubagentRpcMethod
		: undefined;
	const rpcError = error instanceof SubagentRpcError
		? error
		: error instanceof CheckoutLeaseError
			? new SubagentRpcError(
				error.code === "conflict" ? "checkout_conflict" : error.code === "authentication_failed" ? "authentication_failed" : error.code === "evidence_required" ? "evidence_required" : error.code === "not_found" ? "not_found" : "invalid_state",
				error.message,
			)
			: new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		requestId,
		...(method ? { method } : {}),
		success: false,
		error: {
			code: rpcError.code,
			message: rpcError.message,
		},
	};
}

function lifecycleRunId(payload: unknown): string | undefined {
	if (!isRecord(payload)) return undefined;
	if (typeof payload.runId === "string") return payload.runId;
	return typeof payload.id === "string" ? payload.id : undefined;
}

export function registerSubagentRpcBridge(options: RegisterSubagentRpcBridgeOptions): {
	emitReady: (ctx?: ExtensionContext | null) => void;
	dispose: () => void;
} {
	const instance = options.ownerInstance ?? randomUUID();
	const effective = { ...options, ownerInstance: instance };
	const unsubscribes: Array<() => void> = [];
	const requestUnsubscribe = options.events.on(SUBAGENT_RPC_REQUEST_EVENT, async (raw) => {
		let request: SubagentRpcRequestEnvelope | undefined;
		try {
			request = parseRequest(raw);
			const data = await handleRequest(request, effective);
			options.events.emit(subagentRpcReplyEvent(request.requestId), {
				version: SUBAGENT_RPC_PROTOCOL_VERSION,
				requestId: request.requestId,
				method: request.method,
				success: true,
				data,
			} satisfies SubagentRpcReplyEnvelope);
		} catch (error) {
			const reply = errorReply(request ?? raw, error);
			options.events.emit(subagentRpcReplyEvent(reply.requestId), reply);
		}
	});
	if (typeof requestUnsubscribe === "function") unsubscribes.push(requestUnsubscribe);

	const lifecycleEvents = [
		[SUBAGENT_ASYNC_STARTED_EVENT, "started"],
		[SUBAGENT_ASYNC_COMPLETE_EVENT, "settled"],
		[SUBAGENT_CONTROL_EVENT, "control"],
	] as const;
	for (const [source, reason] of lifecycleEvents) {
		const unsubscribe = options.events.on(source, (payload) => {
			const runId = lifecycleRunId(payload);
			if (source === SUBAGENT_ASYNC_COMPLETE_EVENT && runId) {
				try {
					settleOrdinaryCheckoutByChild(runId, { kind: "owner-run", reference: runId, observedTerminalAt: now(options) }, leaseOptions(options));
				} catch {
					// Preserve the admission when canonical checkout evidence cannot be captured.
				}
			}
			options.events.emit(SUBAGENT_LIFECYCLE_EVENT, {
				version: 1,
				owner: "pi-subagents",
				ownerInstance: instance,
				...(runId ? { runId } : {}),
				reason,
				observedAt: now(options),
			});
		});
		if (typeof unsubscribe === "function") unsubscribes.push(unsubscribe);
	}

	return {
		emitReady: (ctx) => {
			options.events.emit(SUBAGENT_RPC_READY_EVENT, pingData(ctx ?? options.getContext(), effective));
		},
		dispose: () => {
			for (const unsubscribe of unsubscribes.splice(0)) unsubscribe();
		},
	};
}
