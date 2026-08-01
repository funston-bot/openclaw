// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RealtimeTalkTransportContext } from "./realtime-talk-shared.ts";

const transportMock = vi.hoisted(() => ({
  context: undefined as RealtimeTalkTransportContext | undefined,
  start: vi.fn(async () => undefined),
  stop: vi.fn(),
}));

vi.mock("./realtime-talk-gateway-relay.ts", () => ({
  GatewayRelayRealtimeTalkTransport: vi.fn(),
}));
vi.mock("./realtime-talk-google-live.ts", () => ({
  GoogleLiveRealtimeTalkTransport: vi.fn(),
}));
vi.mock("./realtime-talk-webrtc.ts", () => ({
  WebRtcSdpRealtimeTalkTransport: vi.fn(function (
    _session: unknown,
    context: RealtimeTalkTransportContext,
  ) {
    transportMock.context = context;
    return { start: transportMock.start, stop: transportMock.stop };
  }),
}));

import { RealtimeTalkSession } from "./realtime-talk.ts";

describe("RealtimeTalkSession transcript queue", () => {
  beforeEach(() => {
    transportMock.context = undefined;
    transportMock.start.mockClear();
    transportMock.stop.mockClear();
  });

  it("stops once when stalled transcript persistence exceeds its bounded queue", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let releaseFirstTranscript!: () => void;
    const firstTranscriptPending = new Promise<void>((resolve) => {
      releaseFirstTranscript = resolve;
    });
    const transcriptEntryIds: string[] = [];
    let firstAttempt = true;
    try {
      const request = vi.fn(
        async (method: string, params?: { entryId?: string; text?: string }) => {
          if (method === "talk.client.create") {
            return {
              provider: "openai",
              transport: "webrtc",
              voiceSessionId: "voice-overflow",
              clientSecret: "secret",
            };
          }
          if (method === "talk.client.transcript") {
            transcriptEntryIds.push(String(params?.entryId));
            if (params?.entryId === "1") {
              if (firstAttempt) {
                firstAttempt = false;
                await firstTranscriptPending;
              }
              throw new Error("still unavailable");
            }
          }
          return { ok: true };
        },
      );
      const onStatus = vi.fn();
      const session = new RealtimeTalkSession({ request } as never, "agent:main:main", {
        onStatus,
      });
      await session.start();
      const onTranscript = transportMock.context?.callbacks.onTranscript;
      if (!onTranscript) {
        throw new Error("expected realtime transcript callback");
      }

      for (let index = 0; index < 10_000; index += 1) {
        onTranscript({
          role: index % 2 === 0 ? "user" : "assistant",
          text: `  ${"x".repeat(9_000)}  `,
          final: true,
        });
      }

      expect(transcriptEntryIds).toEqual(["1"]);
      expect(transportMock.stop).toHaveBeenCalledOnce();
      expect(onStatus.mock.calls.filter(([status]) => status === "error")).toHaveLength(1);
      expect(warn).toHaveBeenCalledOnce();
      expect(request.mock.calls.filter(([method]) => method === "talk.client.close")).toHaveLength(
        0,
      );

      releaseFirstTranscript();
      await vi.advanceTimersByTimeAsync(2_500);
      await vi.runAllTimersAsync();

      expect(transcriptEntryIds).toEqual([
        "1",
        "1",
        "1",
        ...Array.from({ length: 40 }, (_, index) => String(index + 2)),
      ]);
      expect(
        request.mock.calls
          .filter(([method]) => method === "talk.client.transcript")
          .every(([, params]) => String(params?.text).length === 8_000),
      ).toBe(true);
      expect(request.mock.calls.filter(([method]) => method === "talk.client.close")).toHaveLength(
        1,
      );
      expect(onStatus.mock.calls.filter(([status]) => status === "error")).toHaveLength(1);
      expect(warn).toHaveBeenCalledTimes(2);

      onTranscript({ role: "user", text: "too late", final: true });
      session.stop();
      await Promise.resolve();
      expect(transcriptEntryIds).toHaveLength(43);
      expect(transportMock.stop).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });
});
