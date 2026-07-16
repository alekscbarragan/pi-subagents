import { randomBytes, randomUUID, createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { RESULTS_DIR } from "../../shared/types.ts";
import { writeAtomicJson } from "../../shared/atomic-json.ts";

export type CheckoutAccessMode = "read" | "write" | "unknown";

export interface CheckoutIdentityV1 {
	version: 1;
	canonicalPath: string;
	repositoryRoot: string;
	gitCommonDir: string;
	head: string;
}

export interface CheckoutLeaseCredentialV1 {
	leaseId: string;
	operationId: string;
	ownerId: string;
	generation: string;
	authToken: string;
}

export interface CheckoutTerminalEvidenceV1 {
	kind: "owner-run" | "external-terminal" | "launch-failed";
	reference: string;
	observedTerminalAt: number;
	details?: string;
}

export interface CheckoutEvidenceV1 {
	capturedAt: number;
	head: string;
	statusPorcelain: string;
	canonicalPath: string;
}

export interface CheckoutLeaseV1 extends CheckoutLeaseCredentialV1 {
	version: 1;
	state: "active" | "released" | "recovered" | "quarantined";
	workflowRunId: string;
	nodeId: string;
	attempt: number;
	checkout: CheckoutIdentityV1;
	childRef?: string;
	createdAt: number;
	updatedAt: number;
	terminalEvidence?: CheckoutTerminalEvidenceV1;
	checkoutEvidence?: CheckoutEvidenceV1;
	recovery?: { humanActor: string; confirmedAt: number; reason: string };
}

export interface OrdinaryCheckoutAdmissionV1 {
	version: 1;
	admissionId: string;
	operationId: string;
	ownerId: string;
	access: CheckoutAccessMode;
	checkout: CheckoutIdentityV1;
	childRef?: string;
	state: "active" | "quarantined" | "settled" | "recovered";
	createdAt: number;
	updatedAt: number;
	terminalEvidence?: CheckoutTerminalEvidenceV1;
	checkoutEvidence?: CheckoutEvidenceV1;
	recovery?: { humanActor: string; confirmedAt: number; reason: string };
	quarantine?: { reason: string; observedAt: number };
}

export class CheckoutLeaseError extends Error {
	readonly code: "conflict" | "not_found" | "authentication_failed" | "evidence_required" | "invalid_state";

	constructor(code: "conflict" | "not_found" | "authentication_failed" | "evidence_required" | "invalid_state", message: string) {
		super(message);
		this.name = "CheckoutLeaseError";
		this.code = code;
	}
}

export interface CheckoutLeaseStoreOptions {
	root?: string;
	now?: () => number;
	randomId?: () => string;
	/** Test-only negative control. Production must always use O_EXCL admission. */
	unsafeDisableAtomicCreate?: boolean;
	/** Test-only hook used to make the disabled-atomic race deterministic. */
	unsafeBeforeCreate?: () => void;
}

interface AcquireCheckoutLeaseInput {
	operationId: string;
	ownerId: string;
	workflowRunId: string;
	nodeId: string;
	attempt: number;
	cwd: string;
	childRef?: string;
}

interface ReleaseCheckoutLeaseInput extends CheckoutLeaseCredentialV1 {
	cwd: string;
	terminalEvidence: CheckoutTerminalEvidenceV1;
}

interface RecoverCheckoutLeaseInput {
	cwd: string;
	leaseId?: string;
	ordinaryOperationId?: string;
	generation?: string;
	humanConfirmed: true;
	humanActor: string;
	reason: string;
	terminalEvidence: CheckoutTerminalEvidenceV1;
}

interface AdmitOrdinaryCheckoutInput {
	operationId: string;
	ownerId: string;
	cwd: string;
	access: CheckoutAccessMode;
	lease?: CheckoutLeaseCredentialV1;
}

const nonEmpty = (value: string, label: string): string => {
	if (!value.trim() || /[\r\n]/.test(value)) throw new CheckoutLeaseError("invalid_state", `${label} must be a non-empty single-line string.`);
	return value;
};

function runGit(cwd: string, args: string[]): string {
	const result = spawnSync("git", ["-C", cwd, ...args], { encoding: "utf-8" });
	if (result.status !== 0) {
		throw new CheckoutLeaseError("invalid_state", result.stderr.trim() || result.stdout.trim() || `git ${args.join(" ")} failed`);
	}
	return result.stdout.trim();
}

function realpath(value: string): string {
	try { return fs.realpathSync(value); } catch { return path.resolve(value); }
}

export function resolveCheckoutIdentity(cwd: string): CheckoutIdentityV1 {
	const repositoryRoot = realpath(runGit(cwd, ["rev-parse", "--show-toplevel"]));
	const rawCommon = runGit(cwd, ["rev-parse", "--git-common-dir"]);
	const gitCommonDir = realpath(path.isAbsolute(rawCommon) ? rawCommon : path.resolve(repositoryRoot, rawCommon));
	return {
		version: 1,
		canonicalPath: repositoryRoot,
		repositoryRoot,
		gitCommonDir,
		head: runGit(cwd, ["rev-parse", "HEAD"]),
	};
}

function storeRoot(options: CheckoutLeaseStoreOptions): string {
	return options.root ?? path.join(RESULTS_DIR, ".checkout-leases-v1");
}

function digest(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}

function checkoutKey(identity: CheckoutIdentityV1): string {
	return digest(identity.canonicalPath);
}

function activeLeasePath(root: string, identity: CheckoutIdentityV1): string {
	return path.join(root, "active", `${checkoutKey(identity)}.json`);
}

function admissionPath(root: string, operationId: string): string {
	return path.join(root, "ordinary", `${digest(operationId)}.json`);
}

function auditPath(root: string, kind: "leases" | "ordinary", id: string, suffix: string): string {
	return path.join(root, "audit", kind, `${digest(id)}-${suffix}.json`);
}

function ensureStore(root: string): void {
	for (const dir of [root, path.join(root, "active"), path.join(root, "ordinary"), path.join(root, "audit", "leases"), path.join(root, "audit", "ordinary")]) {
		fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
		fs.chmodSync(dir, 0o700);
	}
}

function writeSecureJson(file: string, value: object): void {
	fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
	fs.chmodSync(path.dirname(file), 0o700);
	writeAtomicJson(file, value);
	fs.chmodSync(file, 0o600);
}

function readJson<T>(file: string): T | undefined {
	try { return JSON.parse(fs.readFileSync(file, "utf-8")) as T; }
	catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		if (error instanceof SyntaxError) throw new CheckoutLeaseError("conflict", "Durable checkout admission evidence is incomplete; checkout remains blocked.");
		throw error;
	}
}

function createExclusive(file: string, value: object, options: CheckoutLeaseStoreOptions): boolean {
	fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
	fs.chmodSync(path.dirname(file), 0o700);
	if (options.unsafeDisableAtomicCreate) {
		if (fs.existsSync(file)) return false;
		options.unsafeBeforeCreate?.();
		fs.writeFileSync(file, JSON.stringify(value, null, 2), { encoding: "utf-8", mode: 0o600 });
		fs.chmodSync(file, 0o600);
		return true;
	}
	try {
		const fd = fs.openSync(file, "wx", 0o600);
		try { fs.writeFileSync(fd, JSON.stringify(value, null, 2), "utf-8"); fs.fsyncSync(fd); }
		finally { fs.closeSync(fd); }
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
		throw error;
	}
}

function safeUnlinkIf<T>(file: string, predicate: (record: T) => boolean): void {
	const current = readJson<T>(file);
	if (!current || !predicate(current)) return;
	try { fs.unlinkSync(file); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}

function captureCheckoutEvidence(identity: CheckoutIdentityV1, now: number): CheckoutEvidenceV1 {
	return {
		capturedAt: now,
		head: runGit(identity.canonicalPath, ["rev-parse", "HEAD"]),
		statusPorcelain: runGit(identity.canonicalPath, ["status", "--porcelain=v1"]),
		canonicalPath: identity.canonicalPath,
	};
}

function assertSameCheckout(expected: CheckoutIdentityV1, observed: CheckoutIdentityV1): void {
	if (expected.canonicalPath !== observed.canonicalPath || expected.gitCommonDir !== observed.gitCommonDir) {
		throw new CheckoutLeaseError("authentication_failed", "Checkout identity does not match the held lease.");
	}
}

function activeOrdinaryAdmissions(root: string, identity: CheckoutIdentityV1): OrdinaryCheckoutAdmissionV1[] {
	const dir = path.join(root, "ordinary");
	if (!fs.existsSync(dir)) return [];
	const records: OrdinaryCheckoutAdmissionV1[] = [];
	for (const name of fs.readdirSync(dir)) {
		if (!name.endsWith(".json")) continue;
		try {
			const record = JSON.parse(fs.readFileSync(path.join(dir, name), "utf-8")) as OrdinaryCheckoutAdmissionV1;
			if ((record.state === "active" || record.state === "quarantined") && record.checkout.canonicalPath === identity.canonicalPath) records.push(record);
		} catch {}
	}
	return records;
}

export function acquireCheckoutLease(input: AcquireCheckoutLeaseInput, options: CheckoutLeaseStoreOptions = {}): CheckoutLeaseV1 {
	for (const [value, label] of [[input.operationId, "operationId"], [input.ownerId, "ownerId"], [input.workflowRunId, "workflowRunId"], [input.nodeId, "nodeId"]] as const) nonEmpty(value, label);
	if (!Number.isInteger(input.attempt) || input.attempt < 0) throw new CheckoutLeaseError("invalid_state", "attempt must be a non-negative integer.");
	const root = storeRoot(options); ensureStore(root);
	const checkout = resolveCheckoutIdentity(input.cwd);
	const file = activeLeasePath(root, checkout);
	const timestamp = options.now?.() ?? Date.now();
	const randomId = options.randomId ?? randomUUID;
	const lease: CheckoutLeaseV1 = {
		version: 1,
		leaseId: randomId(), operationId: input.operationId, ownerId: input.ownerId,
		generation: randomId(), authToken: randomBytes(32).toString("base64url"),
		state: "active", workflowRunId: input.workflowRunId, nodeId: input.nodeId, attempt: input.attempt,
		checkout, ...(input.childRef ? { childRef: input.childRef } : {}), createdAt: timestamp, updatedAt: timestamp,
	};
	if (!createExclusive(file, lease, options)) {
		const existing = readJson<CheckoutLeaseV1>(file);
		if (existing?.state === "active" && existing.operationId === input.operationId && existing.ownerId === input.ownerId
			&& existing.workflowRunId === input.workflowRunId && existing.nodeId === input.nodeId && existing.attempt === input.attempt) return existing;
		throw new CheckoutLeaseError("conflict", `Checkout '${checkout.canonicalPath}' already has an active writer lease.`);
	}
	const conflicts = activeOrdinaryAdmissions(root, checkout).filter((record) => record.access !== "read");
	if (conflicts.length) {
		safeUnlinkIf<CheckoutLeaseV1>(file, (current) => current.leaseId === lease.leaseId && current.generation === lease.generation);
		throw new CheckoutLeaseError("conflict", `Checkout '${checkout.canonicalPath}' has an active ordinary ${conflicts[0]!.access}-access run (${conflicts[0]!.operationId}).`);
	}
	return lease;
}

export function getCheckoutLease(cwd: string, options: CheckoutLeaseStoreOptions = {}): { checkout: CheckoutIdentityV1; lease?: CheckoutLeaseV1; ordinary: OrdinaryCheckoutAdmissionV1[] } {
	const root = storeRoot(options); ensureStore(root);
	const checkout = resolveCheckoutIdentity(cwd);
	return { checkout, lease: readJson<CheckoutLeaseV1>(activeLeasePath(root, checkout)), ordinary: activeOrdinaryAdmissions(root, checkout) };
}

function authenticateLease(lease: CheckoutLeaseV1 | undefined, credential: CheckoutLeaseCredentialV1): CheckoutLeaseV1 {
	if (!lease) throw new CheckoutLeaseError("not_found", "No active checkout lease was found.");
	if (lease.leaseId !== credential.leaseId || lease.operationId !== credential.operationId || lease.ownerId !== credential.ownerId
		|| lease.generation !== credential.generation || lease.authToken !== credential.authToken) {
		throw new CheckoutLeaseError("authentication_failed", "Lease operation, owner, generation, or authentication token does not match the active holder.");
	}
	return lease;
}

export function assertCheckoutLeaseCredential(cwd: string, credential: CheckoutLeaseCredentialV1, options: CheckoutLeaseStoreOptions = {}): CheckoutLeaseV1 {
	const checkout = resolveCheckoutIdentity(cwd);
	const lease = authenticateLease(readJson<CheckoutLeaseV1>(activeLeasePath(storeRoot(options), checkout)), credential);
	assertSameCheckout(lease.checkout, checkout);
	return lease;
}

export function releaseCheckoutLease(input: ReleaseCheckoutLeaseInput, options: CheckoutLeaseStoreOptions = {}): CheckoutLeaseV1 {
	if (!input.terminalEvidence?.reference || !Number.isFinite(input.terminalEvidence.observedTerminalAt)) throw new CheckoutLeaseError("evidence_required", "Canonical terminal evidence is required before lease release.");
	const root = storeRoot(options); ensureStore(root);
	const checkout = resolveCheckoutIdentity(input.cwd); const file = activeLeasePath(root, checkout);
	const lease = authenticateLease(readJson<CheckoutLeaseV1>(file), input); assertSameCheckout(lease.checkout, checkout);
	const timestamp = options.now?.() ?? Date.now();
	const released: CheckoutLeaseV1 = { ...lease, state: "released", terminalEvidence: input.terminalEvidence, checkoutEvidence: captureCheckoutEvidence(checkout, timestamp), updatedAt: timestamp };
	writeSecureJson(auditPath(root, "leases", lease.leaseId, lease.generation), released);
	safeUnlinkIf<CheckoutLeaseV1>(file, (current) => current.leaseId === lease.leaseId && current.generation === lease.generation && current.authToken === lease.authToken);
	return released;
}

export function recoverCheckoutLease(input: RecoverCheckoutLeaseInput, options: CheckoutLeaseStoreOptions = {}): CheckoutLeaseV1 | OrdinaryCheckoutAdmissionV1 {
	if (input.humanConfirmed !== true || !input.humanActor?.trim() || !input.reason?.trim()) throw new CheckoutLeaseError("evidence_required", "Audited human confirmation, actor, and reason are required.");
	if (!input.terminalEvidence?.reference || !Number.isFinite(input.terminalEvidence.observedTerminalAt) || input.terminalEvidence.kind === "launch-failed") {
		throw new CheckoutLeaseError("evidence_required", "Canonical owner-run or external terminal evidence is required for recovery.");
	}
	const root = storeRoot(options); ensureStore(root); const checkout = resolveCheckoutIdentity(input.cwd); const timestamp = options.now?.() ?? Date.now();
	const recovery = { humanActor: input.humanActor, confirmedAt: timestamp, reason: input.reason };
	if (input.ordinaryOperationId) {
		const file = admissionPath(root, input.ordinaryOperationId); const record = readJson<OrdinaryCheckoutAdmissionV1>(file);
		if (!record || record.operationId !== input.ordinaryOperationId) throw new CheckoutLeaseError("not_found", "Ordinary checkout admission was not found.");
		assertSameCheckout(record.checkout, checkout);
		const recovered: OrdinaryCheckoutAdmissionV1 = { ...record, state: "recovered", terminalEvidence: input.terminalEvidence, checkoutEvidence: captureCheckoutEvidence(checkout, timestamp), recovery, updatedAt: timestamp };
		writeSecureJson(auditPath(root, "ordinary", record.admissionId, String(timestamp)), recovered);
		safeUnlinkIf<OrdinaryCheckoutAdmissionV1>(file, (current) => current.admissionId === record.admissionId);
		return recovered;
	}
	const file = activeLeasePath(root, checkout); const lease = readJson<CheckoutLeaseV1>(file);
	if (!lease || lease.leaseId !== input.leaseId || lease.generation !== input.generation) throw new CheckoutLeaseError("authentication_failed", "Recovery lease identity or generation does not match the active holder.");
	const recovered: CheckoutLeaseV1 = { ...lease, state: "recovered", terminalEvidence: input.terminalEvidence, checkoutEvidence: captureCheckoutEvidence(checkout, timestamp), recovery, updatedAt: timestamp };
	writeSecureJson(auditPath(root, "leases", lease.leaseId, lease.generation), recovered);
	safeUnlinkIf<CheckoutLeaseV1>(file, (current) => current.leaseId === lease.leaseId && current.generation === lease.generation);
	return recovered;
}

export function admitOrdinaryCheckout(input: AdmitOrdinaryCheckoutInput, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1 | undefined {
	const root = storeRoot(options); ensureStore(root); const checkout = resolveCheckoutIdentity(input.cwd);
	if (input.lease) { assertCheckoutLeaseCredential(input.cwd, input.lease, options); return undefined; }
	const timestamp = options.now?.() ?? Date.now();
	const record: OrdinaryCheckoutAdmissionV1 = { version: 1, admissionId: (options.randomId ?? randomUUID)(), operationId: input.operationId, ownerId: input.ownerId, access: input.access, checkout, state: "active", createdAt: timestamp, updatedAt: timestamp };
	const file = admissionPath(root, input.operationId);
	if (!createExclusive(file, record, options)) {
		const existing = readJson<OrdinaryCheckoutAdmissionV1>(file);
		if (existing?.operationId === input.operationId && existing.ownerId === input.ownerId && existing.checkout.canonicalPath === checkout.canonicalPath) return existing;
		throw new CheckoutLeaseError("conflict", `Ordinary admission operation '${input.operationId}' is already in use.`);
	}
	let lease: CheckoutLeaseV1 | undefined;
	try {
		lease = readJson<CheckoutLeaseV1>(activeLeasePath(root, checkout));
	} catch (error) {
		safeUnlinkIf<OrdinaryCheckoutAdmissionV1>(file, (current) => current.admissionId === record.admissionId);
		throw error;
	}
	if (lease?.state === "active" && input.access !== "read") {
		safeUnlinkIf<OrdinaryCheckoutAdmissionV1>(file, (current) => current.admissionId === record.admissionId);
		throw new CheckoutLeaseError("conflict", `Checkout '${checkout.canonicalPath}' has an active workflow writer lease; ordinary ${input.access}-access launch rejected.`);
	}
	return record;
}

export function bindOrdinaryCheckoutChild(operationId: string, childRef: string, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1 {
	const file = admissionPath(storeRoot(options), operationId); const record = readJson<OrdinaryCheckoutAdmissionV1>(file);
	if (!record) throw new CheckoutLeaseError("not_found", `Ordinary admission '${operationId}' was not found.`);
	const updated = { ...record, childRef, updatedAt: options.now?.() ?? Date.now() }; writeSecureJson(file, updated); return updated;
}

export function quarantineOrdinaryCheckout(operationId: string, reason: string, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1 {
	const file = admissionPath(storeRoot(options), operationId); const record = readJson<OrdinaryCheckoutAdmissionV1>(file);
	if (!record) throw new CheckoutLeaseError("not_found", `Ordinary admission '${operationId}' was not found.`);
	const observedAt = options.now?.() ?? Date.now();
	const updated: OrdinaryCheckoutAdmissionV1 = { ...record, state: "quarantined", quarantine: { reason, observedAt }, updatedAt: observedAt };
	writeSecureJson(file, updated);
	return updated;
}

export function recordOrdinaryExecutionOutcome(input: {
	operationId: string;
	runId?: string;
	asyncId?: string;
	isError?: boolean;
	children: ReadonlyArray<{ detached?: boolean; interrupted?: boolean }>;
	observedAt?: number;
}, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1 | undefined {
	if (input.asyncId) return bindOrdinaryCheckoutChild(input.operationId, input.asyncId, options);
	if (input.runId) bindOrdinaryCheckoutChild(input.operationId, input.runId, options);
	const detached = input.children.some((child) => child.detached);
	const paused = input.children.some((child) => child.interrupted);
	if (detached || paused) {
		return quarantineOrdinaryCheckout(input.operationId, detached
			? "Foreground owner returned with one or more detached children; terminal settlement is uncertain."
			: "Foreground owner returned with one or more paused/interrupted children; terminal settlement is pending resume or recovery.", options);
	}
	return settleOrdinaryCheckout(input.operationId, {
		kind: input.isError ? "launch-failed" : "external-terminal",
		reference: input.runId ?? `ordinary:${input.operationId}:returned`,
		observedTerminalAt: input.observedAt ?? options.now?.() ?? Date.now(),
		details: input.isError ? "Owner executor returned an error before an unsettled child was recorded." : "Foreground owner execution returned terminally.",
	}, options);
}

export function recordOrdinaryResumeOutcome(input: {
	runId?: string;
	asyncId?: string;
	outcome: "queued" | "delivered" | "revived" | "unsupported";
}, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1[] {
	if (input.outcome !== "revived" || !input.runId || !input.asyncId) return [];
	return rebindOrdinaryCheckoutChild(input.runId, input.asyncId, options);
}

export function rebindOrdinaryCheckoutChild(oldChildRef: string, newChildRef: string, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1[] {
	const root = storeRoot(options); ensureStore(root); const rebound: OrdinaryCheckoutAdmissionV1[] = [];
	for (const name of fs.readdirSync(path.join(root, "ordinary"))) {
		const file = path.join(root, "ordinary", name); const record = readJson<OrdinaryCheckoutAdmissionV1>(file);
		if (record?.childRef !== oldChildRef || (record.state !== "active" && record.state !== "quarantined")) continue;
		const updated = { ...record, childRef: newChildRef, updatedAt: options.now?.() ?? Date.now() };
		writeSecureJson(file, updated); rebound.push(updated);
	}
	return rebound;
}

export function settleOrdinaryCheckout(operationId: string, evidence: CheckoutTerminalEvidenceV1, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1 | undefined {
	const root = storeRoot(options); const file = admissionPath(root, operationId); const record = readJson<OrdinaryCheckoutAdmissionV1>(file); if (!record) return undefined;
	const timestamp = options.now?.() ?? Date.now();
	const settled: OrdinaryCheckoutAdmissionV1 = { ...record, state: "settled", terminalEvidence: evidence, checkoutEvidence: captureCheckoutEvidence(record.checkout, timestamp), updatedAt: timestamp };
	writeSecureJson(auditPath(root, "ordinary", record.admissionId, String(timestamp)), settled);
	safeUnlinkIf<OrdinaryCheckoutAdmissionV1>(file, (current) => current.admissionId === record.admissionId); return settled;
}

export function settleOrdinaryCheckoutByChild(childRef: string, evidence: CheckoutTerminalEvidenceV1, options: CheckoutLeaseStoreOptions = {}): OrdinaryCheckoutAdmissionV1[] {
	const root = storeRoot(options); ensureStore(root); const settled: OrdinaryCheckoutAdmissionV1[] = [];
	for (const name of fs.readdirSync(path.join(root, "ordinary"))) {
		const record = readJson<OrdinaryCheckoutAdmissionV1>(path.join(root, "ordinary", name));
		if (record?.childRef === childRef) { const value = settleOrdinaryCheckout(record.operationId, evidence, options); if (value) settled.push(value); }
	}
	return settled;
}
