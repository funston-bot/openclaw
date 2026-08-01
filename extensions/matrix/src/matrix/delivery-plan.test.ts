import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createPluginBlobStoreForTests,
  resetPluginBlobStoreForTests,
  resetPluginStateStoreForTests,
} from "openclaw/plugin-sdk/plugin-state-test-runtime";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installMatrixTestRuntime } from "../test-runtime.js";
import {
  cleanupMatrixDeliveryPlans,
  createMatrixPlannedEvents,
  ensureMatrixDeliveryPlanGarbageCollection,
  loadMatrixDeliveryPlan,
  persistMatrixDeliveryPlan as persistMatrixDeliveryPlanImpl,
  reconcileMatrixUnknownSend,
  resolveMatrixDurableDeliveryIdentity,
} from "./delivery-plan.js";
import type { MatrixPreparedEvent } from "./delivery-plan.js";

const client = {
  getTransactionScopeId: vi.fn(async () => "scope-1"),
  getMessageWireEventType: vi.fn<() => Promise<"m.room.message" | "m.room.encrypted">>(
    async () => "m.room.message",
  ),
};

vi.mock("./send/client.js", () => ({
  withResolvedMatrixSendClient: async (
    _opts: unknown,
    run: (resolved: typeof client) => Promise<unknown>,
  ) => await run(client),
}));

vi.mock("./send/targets.js", () => ({
  resolveMatrixRoomId: vi.fn(async () => "!room:example.org"),
}));

let identity = {
  queueId: "queue-1",
  queueStateDir: "",
  payloadIndex: 0,
  payloadCount: 1,
  partIndex: 0,
  partIndexes: [0],
};
let target = {
  identity,
  accountId: "default",
  roomId: "!room:example.org",
  transactionScopeId: "scope-1",
  wireEventType: "m.room.message" as const,
};

let stateDir = "";
const DELIVERY_PLAN_TTL_MS = 365 * 24 * 60 * 60 * 1000;

function createDeliveryPlanTestStore() {
  return createPluginBlobStoreForTests<unknown>(
    "matrix",
    {
      namespace: "outbound-delivery-plans",
      maxEntries: 10_000,
      maxBytesPerEntry: 8 * 1024 * 1024,
      maxBytesPerNamespace: 256 * 1024 * 1024,
      overflowPolicy: "reject-new",
      defaultTtlMs: DELIVERY_PLAN_TTL_MS,
    },
    { ...process.env, OPENCLAW_STATE_DIR: stateDir },
  );
}

function installDeliveryPlanTestRuntime(
  options: Parameters<typeof installMatrixTestRuntime>[0] = {},
): void {
  installMatrixTestRuntime({ ...options, stateDir });
}

function plannedEvents(
  planIdentity: typeof identity,
  events: readonly Omit<MatrixPreparedEvent, "transactionId">[],
) {
  return createMatrixPlannedEvents({ identity: planIdentity, events });
}

async function persistMatrixDeliveryPlan(
  params: Omit<Parameters<typeof persistMatrixDeliveryPlanImpl>[0], "dispatch"> & {
    requestPrefix?: string;
  },
) {
  const { requestPrefix = "/_matrix/client/v3", ...plan } = params;
  const transactionId = plan.events[0]?.transactionId;
  if (!transactionId) {
    throw new Error("test delivery plan requires an event");
  }
  return await persistMatrixDeliveryPlanImpl({
    ...plan,
    dispatch: {
      roomId: plan.roomId,
      eventType: plan.wireEventType,
      transactionId,
      requestPath: `${requestPrefix}/rooms/${encodeURIComponent(plan.roomId)}/send/${encodeURIComponent(plan.wireEventType)}/${encodeURIComponent(transactionId)}`,
    },
  });
}

describe("Matrix durable delivery plans", () => {
  beforeEach(() => {
    resetPluginStateStoreForTests();
    resetPluginBlobStoreForTests();
    stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-matrix-delivery-plan-"));
    identity = { ...identity, queueStateDir: stateDir };
    target = { ...target, identity };
    client.getTransactionScopeId.mockReset().mockResolvedValue("scope-1");
    client.getMessageWireEventType.mockReset().mockResolvedValue("m.room.message");
  });

  afterEach(async () => {
    await Promise.all([
      cleanupMatrixDeliveryPlans({ queueId: "queue-1" }),
      cleanupMatrixDeliveryPlans({ queueId: "queue-long" }),
      cleanupMatrixDeliveryPlans({ queueId: "queue-gap" }),
      cleanupMatrixDeliveryPlans({ queueId: "queue-sparse" }),
      cleanupMatrixDeliveryPlans({ queueId: "queue-topology-change" }),
      cleanupMatrixDeliveryPlans({
        queueId: "queue-shared",
        deliveryQueueStateDir: "/tmp/matrix-queue-a",
      }),
      cleanupMatrixDeliveryPlans({
        queueId: "queue-shared",
        deliveryQueueStateDir: "/tmp/matrix-queue-b",
      }),
    ]);
    resetPluginStateStoreForTests();
    resetPluginBlobStoreForTests();
    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  it("keeps the first exact event batch and deterministic transaction ids", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const first = await persistMatrixDeliveryPlan({
      ...target,
      events: plannedEvents(identity, [
        { receiptKind: "text", content: { msgtype: "m.text", body: "first" } },
      ]),
    });
    await expect(
      persistMatrixDeliveryPlan({
        ...target,
        events: plannedEvents(identity, [
          { receiptKind: "text", content: { msgtype: "m.text", body: "different" } },
        ]),
      }),
    ).rejects.toThrow("prepared event batch");
    expect(first.events[0]?.content.body).toBe("first");
    expect(first.events[0]?.transactionId).toMatch(/^oc_[A-Za-z0-9_-]+$/u);
    await expect(loadMatrixDeliveryPlan(target)).resolves.toEqual(first);
    const [stored] = await createDeliveryPlanTestStore().entries();
    expect(stored?.expiresAt).toBeGreaterThan(Date.now() + DELIVERY_PLAN_TTL_MS - 5_000);
  });

  it("rejects an SDK request-path change before replay can reach the provider", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const events = plannedEvents(identity, [
      { receiptKind: "text", content: { msgtype: "m.text", body: "first" } },
    ]);
    const first = await persistMatrixDeliveryPlan({ ...target, events });

    expect(first.events[0]?.requestPath).toBe(
      `/_matrix/client/v3/rooms/${encodeURIComponent(target.roomId)}/send/m.room.message/${encodeURIComponent(events[0]!.transactionId)}`,
    );
    await expect(
      persistMatrixDeliveryPlan({
        ...target,
        events,
        requestPrefix: "/_matrix/client/v4",
      }),
    ).rejects.toThrow("SDK request path");
  });

  it("stores long exact plans without the keyed-state JSON value limit", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const body = "x".repeat(100_000);
    const longTarget = {
      ...target,
      identity: { ...identity, queueId: "queue-long" },
    };
    const plan = await persistMatrixDeliveryPlan({
      ...longTarget,
      events: plannedEvents(longTarget.identity, [
        { receiptKind: "text", content: { msgtype: "m.text", body } },
      ]),
    });

    expect(plan.events[0]?.content.body).toBe(body);
    await expect(loadMatrixDeliveryPlan(longTarget)).resolves.toEqual(plan);
  });

  it("isolates identical queue ids owned by different queue stores", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const identityA = {
      ...identity,
      queueId: "queue-shared",
      queueStateDir: "/tmp/matrix-queue-a",
    };
    const identityB = {
      ...identity,
      queueId: "queue-shared",
      queueStateDir: "/tmp/matrix-queue-b",
    };
    const planA = await persistMatrixDeliveryPlan({
      ...target,
      identity: identityA,
      events: plannedEvents(identityA, [
        { receiptKind: "text", content: { msgtype: "m.text", body: "a" } },
      ]),
    });
    const planB = await persistMatrixDeliveryPlan({
      ...target,
      identity: identityB,
      events: plannedEvents(identityB, [
        { receiptKind: "text", content: { msgtype: "m.text", body: "b" } },
      ]),
    });

    expect(planA.events[0]?.transactionId).not.toBe(planB.events[0]?.transactionId);
    await cleanupMatrixDeliveryPlans({
      queueId: identityA.queueId,
      deliveryQueueStateDir: identityA.queueStateDir,
    });
    await expect(loadMatrixDeliveryPlan({ ...target, identity: identityA })).resolves.toBeNull();
    await expect(loadMatrixDeliveryPlan({ ...target, identity: identityB })).resolves.toEqual(
      planB,
    );
  });

  it("canonicalizes implicit and explicit references to the same queue store", () => {
    installDeliveryPlanTestRuntime();

    expect(
      resolveMatrixDurableDeliveryIdentity({
        queueId: "queue-canonical",
        payloadIndex: 0,
        payloadCount: 1,
        partIndex: 0,
        partIndexes: [0],
      }),
    ).toEqual(
      resolveMatrixDurableDeliveryIdentity({
        queueId: "queue-canonical",
        queueStateDir: `${stateDir}/.`,
        payloadIndex: 0,
        payloadCount: 1,
        partIndex: 0,
        partIndexes: [0],
      }),
    );
  });

  it("authorizes replay only while the stored account, room, scope, and wire type match", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    await persistMatrixDeliveryPlan({
      ...target,
      events: plannedEvents(identity, [
        { receiptKind: "text", content: { msgtype: "m.text", body: "hello" } },
      ]),
    });
    const context = {
      cfg: {},
      queueId: identity.queueId,
      channel: "matrix",
      to: "room:!room:example.org",
      accountId: "default",
      enqueuedAt: 1,
      retryCount: 1,
      payloads: [{ text: "hello" }],
      renderedBatchPlan: {
        payloadCount: 1,
        textCount: 1,
        mediaCount: 0,
        voiceCount: 0,
        presentationCount: 0,
        interactiveCount: 0,
        channelDataCount: 0,
        items: [{ index: 0, kinds: ["text" as const], mediaUrls: [] }],
      },
    };

    await expect(reconcileMatrixUnknownSend(context)).resolves.toEqual({ status: "replay_safe" });
    client.getMessageWireEventType.mockResolvedValueOnce("m.room.encrypted");
    await expect(reconcileMatrixUnknownSend(context)).resolves.toMatchObject({
      status: "unresolved",
      retryable: false,
    });
  });

  it("fails closed when persisted part coordinates contain a gap", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const gapIdentity = { ...identity, queueId: "queue-gap" };
    for (const partIndex of [0, 2]) {
      await persistMatrixDeliveryPlan({
        ...target,
        identity: { ...gapIdentity, partIndex, partIndexes: [0, 1, 2] },
        events: plannedEvents({ ...gapIdentity, partIndex, partIndexes: [0, 1, 2] }, [
          { receiptKind: "media", content: { msgtype: "m.image", body: `part-${partIndex}` } },
        ]),
      });
    }

    await expect(
      reconcileMatrixUnknownSend({
        cfg: {},
        queueId: gapIdentity.queueId,
        channel: "matrix",
        to: "room:!room:example.org",
        accountId: "default",
        enqueuedAt: 1,
        retryCount: 1,
        payloads: [{ mediaUrls: ["one", "two", "three"] }],
        renderedBatchPlan: {
          payloadCount: 1,
          textCount: 0,
          mediaCount: 3,
          voiceCount: 0,
          presentationCount: 0,
          interactiveCount: 0,
          channelDataCount: 0,
          items: [
            {
              index: gapIdentity.payloadIndex,
              kinds: ["media"],
              mediaUrls: ["one", "two", "three"],
            },
          ],
        },
      }),
    ).resolves.toMatchObject({ status: "unresolved", retryable: false });
  });

  it("accepts a complete sparse provider part topology", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const sparseIdentity = {
      ...identity,
      queueId: "queue-sparse",
      partIndexes: [0, 2],
    };
    for (const partIndex of sparseIdentity.partIndexes) {
      const partIdentity = { ...sparseIdentity, partIndex };
      await persistMatrixDeliveryPlan({
        ...target,
        identity: partIdentity,
        events: plannedEvents(partIdentity, [
          { receiptKind: "media", content: { msgtype: "m.image", body: `part-${partIndex}` } },
        ]),
      });
    }

    await expect(
      reconcileMatrixUnknownSend({
        cfg: {},
        queueId: sparseIdentity.queueId,
        channel: "matrix",
        to: "room:!room:example.org",
        accountId: "default",
        enqueuedAt: 1,
        retryCount: 1,
        payloads: [{ mediaUrls: ["one", "three"] }],
      }),
    ).resolves.toEqual({ status: "replay_safe" });
  });

  it("fails closed when persisted parts disagree about their authoritative topology", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    const queueId = "queue-topology-change";
    const firstIdentity = { ...identity, queueId, partIndex: 0, partIndexes: [0, 1] };
    const secondIdentity = { ...identity, queueId, partIndex: 1, partIndexes: [0, 1, 2] };
    for (const partIdentity of [firstIdentity, secondIdentity]) {
      await persistMatrixDeliveryPlan({
        ...target,
        identity: partIdentity,
        events: plannedEvents(partIdentity, [
          { receiptKind: "text", content: { msgtype: "m.text", body: "part" } },
        ]),
      });
    }

    await expect(
      reconcileMatrixUnknownSend({
        cfg: {},
        queueId,
        channel: "matrix",
        to: "room:!room:example.org",
        accountId: "default",
        enqueuedAt: 1,
        retryCount: 1,
        payloads: [{ text: "long" }],
      }),
    ).resolves.toMatchObject({ status: "unresolved", retryable: false });
  });

  it("refuses ambiguous replay when no event plan exists", async () => {
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
    await expect(
      reconcileMatrixUnknownSend({
        cfg: {},
        queueId: "missing",
        channel: "matrix",
        to: "room:!room:example.org",
        accountId: "default",
        enqueuedAt: 1,
        retryCount: 1,
        payloads: [{ text: "hello" }],
      }),
    ).resolves.toMatchObject({ status: "unresolved", retryable: false });
  });

  it("retains pending plans and deletes terminal or explicitly cleaned plans", async () => {
    let status: "pending" | "terminal" = "pending";
    installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => status });
    await persistMatrixDeliveryPlan({
      ...target,
      events: plannedEvents(identity, [
        { receiptKind: "text", content: { msgtype: "m.text", body: "hello" } },
      ]),
    });

    await expect(ensureMatrixDeliveryPlanGarbageCollection({ force: true })).resolves.toEqual({
      deleted: 0,
      retained: 1,
      invalid: 0,
    });
    status = "terminal";
    await expect(ensureMatrixDeliveryPlanGarbageCollection({ force: true })).resolves.toEqual({
      deleted: 1,
      retained: 0,
      invalid: 0,
    });
    await expect(loadMatrixDeliveryPlan(target)).resolves.toBeNull();

    await persistMatrixDeliveryPlan({
      ...target,
      events: plannedEvents(identity, [
        { receiptKind: "text", content: { msgtype: "m.text", body: "again" } },
      ]),
    });
    await cleanupMatrixDeliveryPlans({ queueId: identity.queueId });
    await expect(loadMatrixDeliveryPlan(target)).resolves.toBeNull();
  });

  it("physically sweeps expired plans while retaining live pending plans", async () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
      installDeliveryPlanTestRuntime({ getOutboundDeliveryQueueStatus: async () => "pending" });
      const liveIdentity = { ...identity, queueId: "queue-long" };
      await persistMatrixDeliveryPlan({
        ...target,
        events: plannedEvents(identity, [
          { receiptKind: "text", content: { msgtype: "m.text", body: "expired" } },
        ]),
      });
      const livePlan = await persistMatrixDeliveryPlan({
        ...target,
        identity: liveIdentity,
        events: plannedEvents(liveIdentity, [
          { receiptKind: "text", content: { msgtype: "m.text", body: "live" } },
        ]),
      });
      const store = createDeliveryPlanTestStore();
      const storedPlans = await store.entries();
      const expiredEntry = storedPlans.find(
        (entry) => (entry.metadata as { queueId?: string }).queueId === identity.queueId,
      );
      if (!expiredEntry) {
        throw new Error("expected the expiring Matrix delivery plan");
      }
      const storedExpiredPlan = await store.lookup(expiredEntry.key);
      if (!storedExpiredPlan) {
        throw new Error("expected the stored expiring Matrix delivery plan");
      }
      await store.register(expiredEntry.key, storedExpiredPlan.bytes, storedExpiredPlan.metadata, {
        ttlMs: 1,
      });

      vi.advanceTimersByTime(2);
      await expect(store.entries()).resolves.toHaveLength(1);
      await expect(ensureMatrixDeliveryPlanGarbageCollection({ force: true })).resolves.toEqual({
        deleted: 1,
        retained: 1,
        invalid: 0,
      });
      await expect(store.delete(expiredEntry.key)).resolves.toBe(false);
      await expect(loadMatrixDeliveryPlan({ ...target, identity: liveIdentity })).resolves.toEqual(
        livePlan,
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps a successful initial prune latched with fresh runtime state facades", async () => {
    const baseStore = createDeliveryPlanTestStore();
    const deleteExpired = vi.fn(async () => await baseStore.deleteExpired());
    const entries = vi.fn(async () => await baseStore.entries());
    installDeliveryPlanTestRuntime({
      openBlobStore: () => ({ ...baseStore, deleteExpired, entries }) as never,
      freshStateFacade: true,
    });

    await ensureMatrixDeliveryPlanGarbageCollection();
    expect(deleteExpired).toHaveBeenCalledTimes(1);
    expect(entries).toHaveBeenCalledTimes(1);
    await cleanupMatrixDeliveryPlans({ queueId: "queue-none" });
    expect(deleteExpired).toHaveBeenCalledTimes(2);
    expect(entries).toHaveBeenCalledTimes(2);
    await ensureMatrixDeliveryPlanGarbageCollection();
    expect(deleteExpired).toHaveBeenCalledTimes(2);
    expect(entries).toHaveBeenCalledTimes(2);
  });
});
