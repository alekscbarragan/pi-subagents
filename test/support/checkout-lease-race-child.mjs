import * as fs from "node:fs";
import * as path from "node:path";
import { acquireCheckoutLease, admitOrdinaryCheckout } from "../../src/runs/shared/checkout-lease.ts";

const [repo, store, operationId, barrierDir, unsafeRaw, mode = "workflow"] = process.argv.slice(2);
const unsafe = unsafeRaw === "true";
const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
fs.mkdirSync(barrierDir, { recursive: true });
fs.writeFileSync(path.join(barrierDir, `ready-${operationId}`), "ready");
while (!fs.existsSync(path.join(barrierDir, "go"))) sleep(2);
try {
	const options = {
		root: store,
		unsafeDisableAtomicCreate: unsafe,
		unsafeBeforeCreate: unsafe ? () => {
			fs.writeFileSync(path.join(barrierDir, `checked-${operationId}`), "checked");
			while (fs.readdirSync(barrierDir).filter((name) => name.startsWith("checked-")).length < 2) sleep(2);
		} : undefined,
	};
	const admitted = mode === "ordinary"
		? admitOrdinaryCheckout({ operationId, ownerId: `owner-${operationId}`, cwd: repo, access: "unknown" }, options)
		: acquireCheckoutLease({ operationId, ownerId: `owner-${operationId}`, workflowRunId: `wf-${operationId}`, nodeId: "integrate", attempt: 0, cwd: repo }, options);
	fs.writeFileSync(path.join(barrierDir, `result-${operationId}.json`), JSON.stringify({ success: true, id: admitted?.leaseId ?? admitted?.admissionId }));
} catch (error) {
	fs.writeFileSync(path.join(barrierDir, `result-${operationId}.json`), JSON.stringify({ success: false, code: error?.code, message: error?.message }));
}
