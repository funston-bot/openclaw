// Codex plugin module implements conversation turn collector behavior.
import { resolveTimerTimeoutMs } from "openclaw/plugin-sdk/number-runtime";
import { asOptionalRecord as readRecord } from "openclaw/plugin-sdk/string-coerce-runtime";
import { isAssistantCommentaryCompletionNotification } from "./app-server/attempt-notifications.js";
import {
  isCodexNotificationForTurn,
  readCodexNotificationThreadId,
  readCodexNotificationTurnId,
} from "./app-server/notification-correlation.js";
import {
  isJsonObject,
  type CodexServerNotification,
  type JsonObject,
} from "./app-server/protocol.js";

const MAX_PENDING_NOTIFICATIONS_PER_TURN = 100;

export function createCodexConversationTurnCollector(threadId: string) {
  let turnId: string | undefined;
  let completed = false;
  let failedError: string | undefined;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const assistantTextByItem = new Map<string, string>();
  const pendingNotificationsByTurnId = new Map<string, CodexServerNotification[]>();
  let resolveCompletion: ((value: { replyText: string }) => void) | undefined;
  let rejectCompletion: ((error: Error) => void) | undefined;

  const collectReplyText = (): string => {
    const texts = [...assistantTextByItem.values()].map((text) => text.trim()).filter(Boolean);
    return texts.at(-1) ?? "";
  };
  const clearWaitState = () => {
    if (timeout) {
      clearTimeout(timeout);
      timeout = undefined;
    }
    resolveCompletion = undefined;
    rejectCompletion = undefined;
  };
  const finish = () => {
    if (completed) {
      return;
    }
    completed = true;
    if (failedError) {
      rejectCompletion?.(new Error(failedError));
    } else {
      resolveCompletion?.({ replyText: collectReplyText() });
    }
    clearWaitState();
  };

  const handleNotification = (notification: CodexServerNotification) => {
    const params = isJsonObject(notification.params) ? notification.params : undefined;
    if (!params || readCodexNotificationThreadId(params) !== threadId) {
      return;
    }
    if (!turnId) {
      const pendingTurnId = readCodexNotificationTurnId(params);
      if (pendingTurnId) {
        const pending = pendingNotificationsByTurnId.get(pendingTurnId) ?? [];
        if (pending.length === MAX_PENDING_NOTIFICATIONS_PER_TURN) {
          const terminal = notification.method === "turn/completed";
          if (!terminal && !isAssistantMessageCompletion(notification)) {
            return;
          }
          // Preserve item phase as well as turn status: losing commentary completion
          // would make its retained progress deltas impersonate a final answer.
          let expiredNotification = pending.findIndex(
            (item) => item.method !== "turn/completed" && !isAssistantMessageCompletion(item),
          );
          if (expiredNotification < 0 && terminal) {
            expiredNotification = pending.findIndex(isAssistantMessageCompletion);
          }
          if (expiredNotification < 0) {
            return;
          }
          pending.splice(expiredNotification, 1);
        }
        pending.push(notification);
        pendingNotificationsByTurnId.set(pendingTurnId, pending);
      }
      return;
    }
    if (!isCodexNotificationForTurn(params, threadId, turnId)) {
      return;
    }
    if (notification.method === "item/agentMessage/delta") {
      const itemId = readString(params, "itemId") ?? "assistant";
      const delta = readTextString(params, "delta");
      if (!delta) {
        return;
      }
      assistantTextByItem.set(itemId, `${assistantTextByItem.get(itemId) ?? ""}${delta}`);
      return;
    }
    if (notification.method === "item/completed") {
      const item = isJsonObject(params.item) ? params.item : undefined;
      if (item?.type === "agentMessage") {
        const itemId = readString(item, "id") ?? readString(params, "itemId") ?? "assistant";
        assistantTextByItem.delete(itemId);
        if (isAssistantCommentaryCompletionNotification(notification)) {
          return;
        }
        const text = readTextString(item, "text");
        if (text?.trim()) {
          assistantTextByItem.set(itemId, text);
        }
      }
      return;
    }
    if (notification.method === "turn/completed") {
      const turn = isJsonObject(params.turn) ? params.turn : undefined;
      const status = readString(turn, "status");
      if (status === "failed") {
        failedError =
          readString(readRecord(turn?.error), "message") ?? "codex app-server turn failed";
      } else if (status === "interrupted") {
        failedError = "codex app-server bound turn was interrupted";
      } else if (status !== "completed") {
        failedError = "codex app-server turn completed without a valid terminal status";
      }
      const items = Array.isArray(turn?.items) ? turn.items : [];
      for (const item of items) {
        if (!isJsonObject(item) || item.type !== "agentMessage" || item.phase === "commentary") {
          continue;
        }
        const itemId = readString(item, "id") ?? `assistant-${assistantTextByItem.size + 1}`;
        const text = readTextString(item, "text");
        assistantTextByItem.delete(itemId);
        if (text?.trim()) {
          assistantTextByItem.set(itemId, text);
        }
      }
      finish();
    }
  };

  return {
    setTurnId(nextTurnId: string) {
      turnId = nextTurnId;
      const pending = pendingNotificationsByTurnId.get(nextTurnId) ?? [];
      pendingNotificationsByTurnId.clear();
      for (const notification of pending) {
        handleNotification(notification);
      }
    },
    handleNotification,
    wait(params: { timeoutMs: number }): Promise<{ replyText: string }> {
      if (completed) {
        return failedError
          ? Promise.reject(new Error(failedError))
          : Promise.resolve({ replyText: collectReplyText() });
      }
      return new Promise<{ replyText: string }>((resolve, reject) => {
        resolveCompletion = resolve;
        rejectCompletion = reject;
        timeout = setTimeout(
          () => {
            completed = true;
            reject(new Error("codex app-server bound turn timed out"));
            clearWaitState();
          },
          resolveTimerTimeoutMs(params.timeoutMs, 100, 100),
        );
        timeout.unref?.();
      });
    },
  };
}

function isAssistantMessageCompletion(notification: CodexServerNotification) {
  if (notification.method !== "item/completed" || !isJsonObject(notification.params)) {
    return false;
  }
  return isJsonObject(notification.params.item) && notification.params.item.type === "agentMessage";
}

function readString(record: Record<string, unknown> | JsonObject | undefined, key: string) {
  const value = record?.[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readTextString(record: Record<string, unknown> | JsonObject | undefined, key: string) {
  const value = record?.[key];
  return typeof value === "string" && value.length > 0 ? value : undefined;
}
