// Matrix-owned event plans make queue replay idempotent across process restarts.
import { createHash } from "node:crypto";
import path from "node:path";
import type {
  ChannelMessageUnknownSendContext,
  ChannelMessageUnknownSendReconciliationResult,
  MessageReceiptPartKind,
} from "openclaw/plugin-sdk/channel-outbound";
import { getMatrixRuntime } from "../runtime.js";
import type { MatrixClient } from "./sdk.js";
import type { MatrixMessageWireDispatch } from "./sdk/client-base.js";
import { withResolvedMatrixSendClient } from "./send/client.js";
import { resolveMatrixRoomId } from "./send/targets.js";
import type { MatrixOutboundContent } from "./send/types.js";

const DELIVERY_PLAN_VERSION = 2;
const DELIVERY_PLAN_NAMESPACE = "outbound-delivery-plans";
const DELIVERY_PLAN_MAX_ENTRIES = 10_000;
const DELIVERY_PLAN_MAX_BYTES = 8 * 1024 * 1024;
const DELIVERY_PLAN_NAMESPACE_MAX_BYTES = 256 * 1024 * 1024;
// Recovery plans are temporary queue custody. A long TTL preserves extended
// offline recovery while the shipped snapshot sanitizer excludes them.
const DELIVERY_PLAN_TTL_MS = 365 * 24 * 60 * 60 * 1000;

class MatrixDeliveryPlanInvariantError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MatrixDeliveryPlanInvariantError";
  }
}

export type MatrixPreparedEvent = {
  transactionId: string;
  receiptKind: MessageReceiptPartKind;
  content: MatrixOutboundContent;
};

type MatrixPlannedEvent = MatrixPreparedEvent & {
  requestPath: string;
};

type MatrixDeliveryPlan = {
  version: typeof DELIVERY_PLAN_VERSION;
  queueId: string;
  queueStateDir: string;
  accountId: string;
  roomId: string;
  wireEventType: "m.room.message" | "m.room.encrypted";
  transactionScopeId: string;
  payloadIndex: number;
  payloadCount: number;
  partIndex: number;
  partIndexes: number[];
  createdAt: number;
  events: MatrixPlannedEvent[];
};

type MatrixDeliveryPlanMetadata = Omit<MatrixDeliveryPlan, "events">;

type MatrixDeliveryIdentity = {
  queueId: string;
  queueStateDir: string;
  payloadIndex: number;
  payloadCount: number;
  partIndex: number;
  partIndexes: readonly number[];
};

function createDeliveryPlanStore() {
  return getMatrixRuntime().state.openBlobStore<MatrixDeliveryPlanMetadata>({
    namespace: DELIVERY_PLAN_NAMESPACE,
    maxEntries: DELIVERY_PLAN_MAX_ENTRIES,
    maxBytesPerEntry: DELIVERY_PLAN_MAX_BYTES,
    maxBytesPerNamespace: DELIVERY_PLAN_NAMESPACE_MAX_BYTES,
    overflowPolicy: "reject-new",
    defaultTtlMs: DELIVERY_PLAN_TTL_MS,
  });
}

function requireIndex(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Matrix durable delivery ${label} must be a non-negative integer`);
  }
  return value;
}

function isOrderedPartIndexes(value: unknown): value is number[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every(
      (partIndex, index) =>
        Number.isSafeInteger(partIndex) &&
        partIndex >= 0 &&
        (index === 0 || partIndex > value[index - 1]!),
    )
  );
}

function requirePartIndexes(value: readonly number[] | undefined): number[] {
  if (!isOrderedPartIndexes(value)) {
    throw new Error(
      "Matrix durable delivery part indexes must be ordered unique non-negative integers",
    );
  }
  return [...value];
}

function samePartIndexes(left: readonly number[], right: readonly number[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function resolveMatrixRequestPathPrefix(requestPath: string, transactionId: string): string | null {
  if (!requestPath.startsWith("/") || requestPath.includes("?") || requestPath.includes("#")) {
    return null;
  }
  const encodedTransactionId = encodeURIComponent(transactionId);
  const suffix = `/${encodedTransactionId}`;
  if (!encodedTransactionId || !requestPath.endsWith(suffix)) {
    return null;
  }
  return requestPath.slice(0, -encodedTransactionId.length);
}

function bindMatrixRequestPaths(params: {
  events: readonly MatrixPreparedEvent[];
  dispatch: MatrixMessageWireDispatch;
}): MatrixPlannedEvent[] {
  const requestPathPrefix = resolveMatrixRequestPathPrefix(
    params.dispatch.requestPath,
    params.dispatch.transactionId,
  );
  if (
    !requestPathPrefix ||
    !params.events.some((event) => event.transactionId === params.dispatch.transactionId)
  ) {
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery observed an invalid SDK request path",
    );
  }
  return params.events.map((event) => ({
    ...structuredClone(event),
    requestPath: `${requestPathPrefix}${encodeURIComponent(event.transactionId)}`,
  }));
}

function resolveQueueStateDir(queueStateDir?: string): string {
  return path.resolve(queueStateDir?.trim() || getMatrixRuntime().state.resolveStateDir());
}

function queueDigest(identity: Pick<MatrixDeliveryIdentity, "queueId" | "queueStateDir">): string {
  return createHash("sha256")
    .update(resolveQueueStateDir(identity.queueStateDir))
    .update("\0")
    .update(identity.queueId)
    .digest("hex");
}

function queuePrefix(identity: Pick<MatrixDeliveryIdentity, "queueId" | "queueStateDir">): string {
  return `${queueDigest(identity)}.`;
}

function planKey(identity: MatrixDeliveryIdentity): string {
  return `${queuePrefix(identity)}${requireIndex(identity.payloadIndex, "payload index")}.${requireIndex(identity.partIndex, "part index")}`;
}

const RECEIPT_KINDS = new Set<MessageReceiptPartKind>([
  "text",
  "media",
  "voice",
  "poll",
  "card",
  "preview",
  "unknown",
]);

function isPlan(value: unknown): value is MatrixDeliveryPlan {
  if (!value || typeof value !== "object") {
    return false;
  }
  const plan = value as Partial<MatrixDeliveryPlan>;
  return (
    plan.version === DELIVERY_PLAN_VERSION &&
    typeof plan.queueId === "string" &&
    Boolean(plan.queueId.trim()) &&
    typeof plan.queueStateDir === "string" &&
    Boolean(plan.queueStateDir.trim()) &&
    typeof plan.accountId === "string" &&
    typeof plan.roomId === "string" &&
    Boolean(plan.roomId.trim()) &&
    (plan.wireEventType === "m.room.message" || plan.wireEventType === "m.room.encrypted") &&
    typeof plan.transactionScopeId === "string" &&
    Boolean(plan.transactionScopeId.trim()) &&
    Number.isSafeInteger(plan.payloadIndex) &&
    (plan.payloadIndex ?? -1) >= 0 &&
    Number.isSafeInteger(plan.payloadCount) &&
    (plan.payloadCount ?? 0) > 0 &&
    (plan.payloadIndex ?? Number.MAX_SAFE_INTEGER) < (plan.payloadCount ?? 0) &&
    Number.isSafeInteger(plan.partIndex) &&
    (plan.partIndex ?? -1) >= 0 &&
    isOrderedPartIndexes(plan.partIndexes) &&
    plan.partIndexes.includes(plan.partIndex ?? -1) &&
    typeof plan.createdAt === "number" &&
    Number.isFinite(plan.createdAt) &&
    Array.isArray(plan.events) &&
    plan.events.length > 0 &&
    plan.events.every(
      (event) =>
        event &&
        typeof event === "object" &&
        typeof event.transactionId === "string" &&
        Boolean(event.transactionId.trim()) &&
        typeof event.requestPath === "string" &&
        resolveMatrixRequestPathPrefix(event.requestPath, event.transactionId) !== null &&
        RECEIPT_KINDS.has(event.receiptKind) &&
        Boolean(event.content) &&
        typeof event.content === "object",
    )
  );
}

function planMetadata(plan: MatrixDeliveryPlan): MatrixDeliveryPlanMetadata {
  const { events: _events, ...metadata } = plan;
  return metadata;
}

function isPlanMetadata(value: unknown): value is MatrixDeliveryPlanMetadata {
  if (!value || typeof value !== "object") {
    return false;
  }
  const metadata = value as Partial<MatrixDeliveryPlanMetadata>;
  return (
    metadata.version === DELIVERY_PLAN_VERSION &&
    typeof metadata.queueId === "string" &&
    Boolean(metadata.queueId.trim()) &&
    typeof metadata.queueStateDir === "string" &&
    Boolean(metadata.queueStateDir.trim()) &&
    typeof metadata.accountId === "string" &&
    typeof metadata.roomId === "string" &&
    Boolean(metadata.roomId.trim()) &&
    (metadata.wireEventType === "m.room.message" ||
      metadata.wireEventType === "m.room.encrypted") &&
    typeof metadata.transactionScopeId === "string" &&
    Boolean(metadata.transactionScopeId.trim()) &&
    Number.isSafeInteger(metadata.payloadIndex) &&
    (metadata.payloadIndex ?? -1) >= 0 &&
    Number.isSafeInteger(metadata.payloadCount) &&
    (metadata.payloadCount ?? 0) > 0 &&
    (metadata.payloadIndex ?? Number.MAX_SAFE_INTEGER) < (metadata.payloadCount ?? 0) &&
    Number.isSafeInteger(metadata.partIndex) &&
    (metadata.partIndex ?? -1) >= 0 &&
    isOrderedPartIndexes(metadata.partIndexes) &&
    metadata.partIndexes.includes(metadata.partIndex ?? -1) &&
    typeof metadata.createdAt === "number" &&
    Number.isFinite(metadata.createdAt)
  );
}

function decodePlan(bytes: Uint8Array): MatrixDeliveryPlan {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new MatrixDeliveryPlanInvariantError("Matrix durable delivery plan is invalid JSON");
  }
  if (!isPlan(value)) {
    throw new MatrixDeliveryPlanInvariantError("Matrix durable delivery plan is invalid");
  }
  return value;
}

function metadataMatchesPlan(
  metadata: MatrixDeliveryPlanMetadata,
  plan: MatrixDeliveryPlan,
): boolean {
  return JSON.stringify(metadata) === JSON.stringify(planMetadata(plan));
}

function transactionId(identity: MatrixDeliveryIdentity, eventIndex: number): string {
  const digest = createHash("sha256")
    .update(resolveQueueStateDir(identity.queueStateDir))
    .update("\0")
    .update(identity.queueId)
    .update("\0")
    .update(String(requireIndex(identity.payloadIndex, "payload index")))
    .update("\0")
    .update(String(requireIndex(identity.partIndex, "part index")))
    .update("\0")
    .update(String(requireIndex(eventIndex, "event index")))
    .digest("base64url");
  return `oc_${digest}`;
}

export function createMatrixPlannedEvents(params: {
  identity: MatrixDeliveryIdentity;
  events: readonly Omit<MatrixPreparedEvent, "transactionId">[];
}): MatrixPreparedEvent[] {
  return params.events.map((event, index) => ({
    ...structuredClone(event),
    transactionId: transactionId(params.identity, index),
  }));
}

function assertPlanIdentity(
  plan: MatrixDeliveryPlan,
  params: {
    identity: MatrixDeliveryIdentity;
    accountId?: string | null;
    roomId: string;
    transactionScopeId: string;
    wireEventType: "m.room.message" | "m.room.encrypted";
  },
): void {
  if (
    plan.queueId !== params.identity.queueId ||
    plan.queueStateDir !== resolveQueueStateDir(params.identity.queueStateDir) ||
    plan.payloadIndex !== params.identity.payloadIndex ||
    plan.payloadCount !== params.identity.payloadCount ||
    plan.partIndex !== params.identity.partIndex ||
    !samePartIndexes(plan.partIndexes, params.identity.partIndexes) ||
    plan.accountId !== (params.accountId ?? "") ||
    plan.roomId !== params.roomId ||
    plan.transactionScopeId !== params.transactionScopeId ||
    plan.wireEventType !== params.wireEventType
  ) {
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery plan no longer matches the active delivery target",
    );
  }
}

export async function loadMatrixDeliveryPlan(params: {
  identity: MatrixDeliveryIdentity;
  accountId?: string | null;
  roomId: string;
  transactionScopeId: string;
  wireEventType: "m.room.message" | "m.room.encrypted";
}): Promise<MatrixDeliveryPlan | null> {
  const entry = await createDeliveryPlanStore().lookup(planKey(params.identity));
  if (entry === undefined) {
    return null;
  }
  const plan = decodePlan(entry.bytes);
  if (!metadataMatchesPlan(entry.metadata, plan)) {
    throw new MatrixDeliveryPlanInvariantError("Matrix durable delivery metadata is invalid");
  }
  assertPlanIdentity(plan, params);
  return structuredClone(plan);
}

export async function persistMatrixDeliveryPlan(params: {
  identity: MatrixDeliveryIdentity;
  accountId?: string | null;
  roomId: string;
  transactionScopeId: string;
  wireEventType: "m.room.message" | "m.room.encrypted";
  events: readonly MatrixPreparedEvent[];
  dispatch: MatrixMessageWireDispatch;
}): Promise<MatrixDeliveryPlan> {
  if (params.events.length === 0) {
    throw new Error("Matrix durable delivery plan must contain at least one event");
  }
  await ensureMatrixDeliveryPlanGarbageCollection();
  const identity = {
    ...params.identity,
    queueStateDir: resolveQueueStateDir(params.identity.queueStateDir),
  };
  if (
    params.dispatch.roomId !== params.roomId ||
    params.dispatch.eventType !== params.wireEventType
  ) {
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery was dispatched to an unexpected endpoint",
    );
  }
  const preparedEvents = params.events.map((event, index) => {
    const expectedTransactionId = transactionId(identity, index);
    if (event.transactionId !== expectedTransactionId) {
      throw new MatrixDeliveryPlanInvariantError(
        "Matrix durable delivery plan has an invalid transaction identifier",
      );
    }
    return structuredClone(event);
  });
  const events = bindMatrixRequestPaths({ events: preparedEvents, dispatch: params.dispatch });
  const plan: MatrixDeliveryPlan = {
    version: DELIVERY_PLAN_VERSION,
    queueId: identity.queueId,
    queueStateDir: identity.queueStateDir,
    accountId: params.accountId ?? "",
    roomId: params.roomId,
    wireEventType: params.wireEventType,
    transactionScopeId: params.transactionScopeId,
    payloadIndex: requireIndex(identity.payloadIndex, "payload index"),
    payloadCount: requireIndex(identity.payloadCount, "payload count"),
    partIndex: requireIndex(identity.partIndex, "part index"),
    partIndexes: requirePartIndexes(identity.partIndexes),
    createdAt: Date.now(),
    events,
  };
  const store = createDeliveryPlanStore();
  const bytes = new TextEncoder().encode(JSON.stringify(plan));
  if (await store.registerIfAbsent(planKey(identity), bytes, planMetadata(plan))) {
    return plan;
  }
  const existing = await loadMatrixDeliveryPlan(params);
  if (!existing) {
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery plan disappeared after registration",
    );
  }
  const withoutRequestPaths = (planned: readonly MatrixPlannedEvent[]) =>
    planned.map(({ requestPath: _requestPath, ...event }) => event);
  if (
    JSON.stringify(withoutRequestPaths(existing.events)) !==
    JSON.stringify(withoutRequestPaths(plan.events))
  ) {
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery plan no longer matches the prepared event batch",
    );
  }
  if (
    existing.events.some((event, index) => event.requestPath !== plan.events[index]?.requestPath)
  ) {
    // Matrix idempotency is scoped to the transaction ID plus full HTTP path.
    // Route drift must fail before fetch or the same ID can create a duplicate.
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery SDK request path no longer matches the persisted plan",
    );
  }
  return existing;
}

export function resolveMatrixDurableDeliveryIdentity(params: {
  queueId?: string;
  queueStateDir?: string;
  payloadIndex?: number;
  payloadCount?: number;
  partIndex?: number;
  partIndexes?: readonly number[];
}): MatrixDeliveryIdentity | null {
  if (params.queueId === undefined) {
    return null;
  }
  if (
    params.payloadIndex === undefined ||
    params.payloadCount === undefined ||
    params.partIndex === undefined ||
    params.partIndexes === undefined
  ) {
    throw new Error("Matrix durable delivery requires stable payload and part topology");
  }
  const payloadCount = requireIndex(params.payloadCount, "payload count");
  if (payloadCount === 0 || params.payloadIndex >= payloadCount) {
    throw new Error("Matrix durable delivery payload index must be within the payload count");
  }
  const partIndexes = requirePartIndexes(params.partIndexes);
  if (!partIndexes.includes(params.partIndex)) {
    throw new Error("Matrix durable delivery part index must be present in the part topology");
  }
  return {
    queueId: params.queueId,
    queueStateDir: resolveQueueStateDir(params.queueStateDir),
    payloadIndex: requireIndex(params.payloadIndex, "payload index"),
    payloadCount,
    partIndex: requireIndex(params.partIndex, "part index"),
    partIndexes,
  };
}

async function requireTransactionScope(client: MatrixClient): Promise<string> {
  const scope = (await client.getTransactionScopeId()).trim();
  if (!scope) {
    throw new MatrixDeliveryPlanInvariantError(
      "Matrix durable delivery requires a stable transaction scope",
    );
  }
  return scope;
}

async function loadQueuePlans(
  identity: Pick<MatrixDeliveryIdentity, "queueId" | "queueStateDir">,
): Promise<MatrixDeliveryPlan[]> {
  const store = createDeliveryPlanStore();
  const keys = (await store.entries())
    .filter((entry) => entry.key.startsWith(queuePrefix(identity)))
    .map((entry) => entry.key);
  return await Promise.all(
    keys.map(async (key) => {
      const entry = await store.lookup(key);
      if (!entry) {
        throw new MatrixDeliveryPlanInvariantError(
          "Matrix durable delivery plan disappeared during reconciliation",
        );
      }
      const plan = decodePlan(entry.bytes);
      if (key !== planKey(plan) || !metadataMatchesPlan(entry.metadata, plan)) {
        throw new MatrixDeliveryPlanInvariantError("Matrix durable delivery metadata is invalid");
      }
      return plan;
    }),
  );
}

export async function reconcileMatrixUnknownSend(
  ctx: ChannelMessageUnknownSendContext,
): Promise<ChannelMessageUnknownSendReconciliationResult> {
  try {
    const plans = await loadQueuePlans({
      queueId: ctx.queueId,
      queueStateDir: resolveQueueStateDir(ctx.deliveryQueueStateDir),
    });
    if (plans.length === 0) {
      return {
        status: "unresolved",
        error: "Matrix ambiguous delivery has no persisted event plan",
        retryable: false,
      };
    }
    return await withResolvedMatrixSendClient(
      { cfg: ctx.cfg, accountId: ctx.accountId },
      async (client) => {
        const transactionScopeId = await requireTransactionScope(client);
        const roomId = await resolveMatrixRoomId(client, ctx.to);
        const wireEventType = await client.getMessageWireEventType(roomId);
        const payloadCount = ctx.payloads.length;
        const expectedPartIndexes = new Map<number, readonly number[]>();
        const storedPartsByPayload = new Map<number, Set<number>>();
        for (const plan of plans) {
          assertPlanIdentity(plan, {
            identity: plan,
            accountId: ctx.accountId,
            roomId,
            transactionScopeId,
            wireEventType,
          });
          if (plan.payloadCount !== payloadCount) {
            throw new MatrixDeliveryPlanInvariantError(
              "Matrix durable delivery plan payload topology no longer matches the queued batch",
            );
          }
          const existingPartIndexes = expectedPartIndexes.get(plan.payloadIndex);
          if (existingPartIndexes && !samePartIndexes(existingPartIndexes, plan.partIndexes)) {
            throw new MatrixDeliveryPlanInvariantError(
              "Matrix durable delivery plan part topology is inconsistent",
            );
          }
          expectedPartIndexes.set(plan.payloadIndex, plan.partIndexes);
          const storedParts = storedPartsByPayload.get(plan.payloadIndex) ?? new Set<number>();
          storedParts.add(plan.partIndex);
          storedPartsByPayload.set(plan.payloadIndex, storedParts);
        }
        const highestPayloadIndex = Math.max(...storedPartsByPayload.keys());
        for (let payloadIndex = 0; payloadIndex <= highestPayloadIndex; payloadIndex += 1) {
          const storedParts = storedPartsByPayload.get(payloadIndex);
          const partIndexes = expectedPartIndexes.get(payloadIndex);
          if (!storedParts || !partIndexes) {
            throw new MatrixDeliveryPlanInvariantError(
              "Matrix durable delivery plan has a missing dispatched payload coordinate",
            );
          }
          const highestStoredPosition = Math.max(
            ...[...storedParts].map((partIndex) => partIndexes.indexOf(partIndex)),
          );
          for (let position = 0; position <= highestStoredPosition; position += 1) {
            if (!storedParts.has(partIndexes[position]!)) {
              // A missing tail was never dispatched: every part persists its plan
              // first. A gap inside the authoritative persisted prefix is invalid.
              throw new MatrixDeliveryPlanInvariantError(
                "Matrix durable delivery plan has a missing dispatched coordinate",
              );
            }
          }
        }
        return { status: "replay_safe" };
      },
    );
  } catch (error) {
    return {
      status: "unresolved",
      error: error instanceof Error ? error.message : String(error),
      retryable: !(error instanceof MatrixDeliveryPlanInvariantError),
    };
  }
}

export async function cleanupMatrixDeliveryPlans(ctx: {
  queueId: string;
  deliveryQueueStateDir?: string;
}): Promise<void> {
  const runtime = getMatrixRuntime();
  const store = createDeliveryPlanStore();
  try {
    await store.deleteExpired();
    const keys = (await store.entries())
      .filter((entry) =>
        entry.key.startsWith(
          queuePrefix({
            queueId: ctx.queueId,
            queueStateDir: resolveQueueStateDir(ctx.deliveryQueueStateDir),
          }),
        ),
      )
      .map((entry) => entry.key);
    await Promise.all(keys.map(async (key) => await store.delete(key)));
  } catch (error) {
    // A failed terminal cleanup must re-arm GC for the next send in this process.
    initialPlanPrunes.delete(runtime);
    throw error;
  }
}

type MatrixDeliveryPlanPruneResult = {
  deleted: number;
  retained: number;
  invalid: number;
};

async function pruneMatrixTerminalDeliveryPlans(): Promise<MatrixDeliveryPlanPruneResult> {
  const getQueueStatus = getMatrixRuntime().state.getOutboundDeliveryQueueStatus;
  if (!getQueueStatus) {
    throw new Error("Matrix durable delivery cleanup requires queue status support");
  }
  const store = createDeliveryPlanStore();
  const expired = await store.deleteExpired();
  const entries = await store.entries();
  const statusByQueue = new Map<string, "pending" | "terminal" | "absent">();
  const deletions: string[] = [];
  let retained = 0;
  let invalid = 0;
  for (const entry of entries) {
    const metadata = entry.metadata;
    if (!isPlanMetadata(metadata) || entry.key !== planKey(metadata)) {
      invalid += 1;
      continue;
    }
    const location = JSON.stringify([metadata.queueId, metadata.queueStateDir]);
    let status = statusByQueue.get(location);
    if (!status) {
      status = await getQueueStatus(metadata.queueId, metadata.queueStateDir);
      statusByQueue.set(location, status);
    }
    if (status === "pending") {
      retained += 1;
    } else {
      // Queue ownership commits before plan registration in the same SQLite
      // store, so terminal/absent both prove this plan is no longer replayable.
      deletions.push(entry.key);
    }
  }
  await Promise.all(deletions.map(async (key) => await store.delete(key)));
  return { deleted: expired.length + deletions.length, retained, invalid };
}

const initialPlanPrunes = new WeakMap<object, Promise<MatrixDeliveryPlanPruneResult>>();

export async function ensureMatrixDeliveryPlanGarbageCollection(options?: {
  force?: boolean;
}): Promise<MatrixDeliveryPlanPruneResult> {
  // The plugin runtime proxy is stable, while its state facade is recreated
  // on access. Cache on the proxy so every durable part does not rescan plans.
  const runtime = getMatrixRuntime();
  const current =
    options?.force === true
      ? pruneMatrixTerminalDeliveryPlans()
      : (initialPlanPrunes.get(runtime) ?? pruneMatrixTerminalDeliveryPlans());
  initialPlanPrunes.set(runtime, current);
  try {
    return await current;
  } catch (error) {
    if (initialPlanPrunes.get(runtime) === current) {
      initialPlanPrunes.delete(runtime);
    }
    throw error;
  }
}
