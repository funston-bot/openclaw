// Codex tests cover conversation turn collector plugin behavior.
import { MAX_TIMER_TIMEOUT_MS } from "openclaw/plugin-sdk/number-runtime";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCodexConversationTurnCollector } from "./conversation-turn-collector.js";

describe("codex conversation turn collector", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("collects streamed assistant deltas for the active turn", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    const completion = collector.wait({ timeoutMs: 1_000 });

    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", turnId: "turn-1", itemId: "item-1", delta: "hello " },
    });
    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", turnId: "turn-1", itemId: "item-1", delta: "world" },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: { threadId: "thread-1", turn: { id: "turn-1", status: "completed", items: [] } },
    });

    await expect(completion).resolves.toEqual({ replyText: "hello world" });
  });

  it("buffers pre-start notifications and replays only the selected turn", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");

    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          id: "turn-stale",
          status: "completed",
          items: [{ type: "agentMessage", id: "wrong", text: "stale answer" }],
        },
      },
    });
    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", turnId: "turn-1", itemId: "right", delta: "fresh " },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          id: "turn-1",
          status: "completed",
          items: [{ type: "agentMessage", id: "right", text: "fresh answer" }],
        },
      },
    });

    collector.setTurnId("turn-1");

    await expect(collector.wait({ timeoutMs: 1_000 })).resolves.toEqual({
      replyText: "fresh answer",
    });
  });

  it("retains the terminal outcome after a full pre-start assistant stream", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    for (let index = 0; index < 101; index += 1) {
      collector.handleNotification({
        method: "item/agentMessage/delta",
        params: { threadId: "thread-1", turnId: "turn-1", itemId: "answer", delta: "." },
      });
    }
    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          id: "turn-1",
          status: "completed",
          items: [{ type: "agentMessage", id: "answer", text: "authoritative answer" }],
        },
      },
    });
    collector.setTurnId("turn-1");

    await expect(collector.wait({ timeoutMs: 100 })).resolves.toEqual({
      replyText: "authoritative answer",
    });
  });

  it("does not let completed commentary replace or impersonate a final answer", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    collector.handleNotification({
      method: "item/completed",
      params: {
        threadId: "thread-1",
        turnId: "turn-1",
        item: { type: "agentMessage", id: "answer", text: "real answer", phase: "final_answer" },
      },
    });
    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", turnId: "turn-1", itemId: "progress", delta: "progress" },
    });
    collector.handleNotification({
      method: "item/completed",
      params: {
        threadId: "thread-1",
        turnId: "turn-1",
        item: { type: "agentMessage", id: "progress", text: "progress", phase: "commentary" },
      },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          id: "turn-1",
          status: "completed",
          items: [{ type: "agentMessage", id: "late", text: "late progress", phase: "commentary" }],
        },
      },
    });

    await expect(collector.wait({ timeoutMs: 100 })).resolves.toEqual({ replyText: "real answer" });
  });

  it("retains commentary completion after the pre-start notification buffer fills", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    for (let index = 0; index < 100; index += 1) {
      collector.handleNotification({
        method: "item/agentMessage/delta",
        params: { threadId: "thread-1", turnId: "turn-1", itemId: "progress", delta: "." },
      });
    }
    collector.handleNotification({
      method: "item/completed",
      params: {
        threadId: "thread-1",
        turnId: "turn-1",
        item: { type: "agentMessage", id: "progress", text: "progress", phase: "commentary" },
      },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: { threadId: "thread-1", turn: { id: "turn-1", status: "completed", items: [] } },
    });
    collector.setTurnId("turn-1");

    await expect(collector.wait({ timeoutMs: 100 })).resolves.toEqual({ replyText: "" });
  });

  it("does not let unrelated tool completion evict the final assistant message", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    for (let index = 0; index < 100; index += 1) {
      collector.handleNotification({
        method: "item/agentMessage/delta",
        params: { threadId: "thread-1", turnId: "turn-1", itemId: "answer", delta: "." },
      });
      collector.handleNotification({
        method: "item/completed",
        params: {
          threadId: "thread-1",
          turnId: "turn-1",
          item: { type: "commandExecution", id: `command-${index}` },
        },
      });
    }
    collector.handleNotification({
      method: "item/completed",
      params: {
        threadId: "thread-1",
        turnId: "turn-1",
        item: { type: "agentMessage", id: "answer", text: "authoritative answer" },
      },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: { threadId: "thread-1", turn: { id: "turn-1", status: "completed", items: [] } },
    });
    collector.setTurnId("turn-1");

    await expect(collector.wait({ timeoutMs: 100 })).resolves.toEqual({
      replyText: "authoritative answer",
    });
  });

  it.each([
    { status: "interrupted", expected: "codex app-server bound turn was interrupted" },
    {
      status: "inProgress",
      expected: "codex app-server turn completed without a valid terminal status",
    },
  ])(
    "rejects a $status terminal instead of returning partial output",
    async ({ status, expected }) => {
      const collector = createCodexConversationTurnCollector("thread-1");
      collector.setTurnId("turn-1");
      collector.handleNotification({
        method: "item/agentMessage/delta",
        params: { threadId: "thread-1", turnId: "turn-1", itemId: "answer", delta: "partial" },
      });
      collector.handleNotification({
        method: "turn/completed",
        params: { threadId: "thread-1", turn: { id: "turn-1", status, error: null, items: [] } },
      });

      await expect(collector.wait({ timeoutMs: 100 })).rejects.toThrow(expected);
    },
  );

  it("uses completed agent message items when deltas are absent", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    const completion = collector.wait({ timeoutMs: 1_000 });

    collector.handleNotification({
      method: "item/completed",
      params: {
        threadId: "thread-1",
        turnId: "turn-1",
        item: { type: "agentMessage", id: "item-1", text: "final answer" },
      },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: { threadId: "thread-1", turn: { id: "turn-1", status: "completed", items: [] } },
    });

    await expect(completion).resolves.toEqual({ replyText: "final answer" });
  });

  it("ignores notifications for other threads or turns", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    const completion = collector.wait({ timeoutMs: 1_000 });

    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-2", turnId: "turn-1", itemId: "wrong", delta: "wrong" },
    });
    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", turnId: "turn-2", itemId: "wrong", delta: "wrong" },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          id: "turn-1",
          status: "completed",
          items: [{ type: "agentMessage", id: "item-1", text: "right" }],
        },
      },
    });

    await expect(completion).resolves.toEqual({ replyText: "right" });
  });

  it("ignores unscoped deltas once the active turn is known", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    const completion = collector.wait({ timeoutMs: 1_000 });

    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", itemId: "wrong", delta: "wrong" },
    });
    collector.handleNotification({
      method: "item/agentMessage/delta",
      params: { threadId: "thread-1", turnId: "turn-1", itemId: "right", delta: "right" },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: { threadId: "thread-1", turn: { id: "turn-1", status: "completed", items: [] } },
    });

    await expect(completion).resolves.toEqual({ replyText: "right" });
  });

  it("does not complete from unscoped turn completion once the active turn is known", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    const completion = collector.wait({ timeoutMs: 1_000 });

    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          status: "completed",
          items: [{ type: "agentMessage", id: "wrong", text: "wrong" }],
        },
      },
    });
    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: {
          id: "turn-1",
          status: "completed",
          items: [{ type: "agentMessage", id: "right", text: "right" }],
        },
      },
    });

    await expect(completion).resolves.toEqual({ replyText: "right" });
  });

  it("rejects failed turns with the app-server error message", async () => {
    const collector = createCodexConversationTurnCollector("thread-1");
    collector.setTurnId("turn-1");
    const completion = collector.wait({ timeoutMs: 1_000 });

    collector.handleNotification({
      method: "turn/completed",
      params: {
        threadId: "thread-1",
        turn: { id: "turn-1", status: "failed", error: { message: "model exploded" }, items: [] },
      },
    });

    await expect(completion).rejects.toThrow("model exploded");
  });

  it("times out when the app-server never completes the turn", async () => {
    vi.useFakeTimers();
    try {
      const collector = createCodexConversationTurnCollector("thread-1");
      const completion = collector.wait({ timeoutMs: 100 });
      const assertion = expect(completion).rejects.toThrow("codex app-server bound turn timed out");
      await vi.advanceTimersByTimeAsync(100);
      await assertion;
    } finally {
      vi.restoreAllMocks();
      vi.useRealTimers();
    }
  });

  it("clamps oversized turn wait timers", async () => {
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
    try {
      const collector = createCodexConversationTurnCollector("thread-1");
      collector.setTurnId("turn-1");
      const completion = collector.wait({ timeoutMs: MAX_TIMER_TIMEOUT_MS + 1 });

      expect(setTimeoutSpy).toHaveBeenCalledWith(expect.any(Function), MAX_TIMER_TIMEOUT_MS);
      collector.handleNotification({
        method: "turn/completed",
        params: { threadId: "thread-1", turn: { id: "turn-1", status: "completed", items: [] } },
      });

      await expect(completion).resolves.toEqual({ replyText: "" });
    } finally {
      vi.restoreAllMocks();
      vi.clearAllTimers();
      vi.useRealTimers();
    }
  });
});
