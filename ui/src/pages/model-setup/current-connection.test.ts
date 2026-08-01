/* @vitest-environment jsdom */

import { nothing, render } from "lit";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SystemAgentSetupDetectResult } from "../../api/types.ts";
import { i18n } from "../../i18n/index.ts";
import { renderCurrentConnection } from "./current-connection.ts";

const prepareOptions: NonNullable<SystemAgentSetupDetectResult["prepareOptions"]> = [
  { id: "ollama", brandId: "ollama", label: "Ollama" },
  { id: "llama-cpp", brandId: "llama-cpp", label: "llama.cpp" },
  { id: "lmstudio", brandId: "lmstudio", label: "LM Studio" },
];

function mount(result: SystemAgentSetupDetectResult) {
  const container = document.createElement("div");
  document.body.append(container);
  const onStartPrepare = vi.fn();
  const onVerify = vi.fn();
  render(
    renderCurrentConnection({
      result,
      verify: {
        phase: "failed",
        status: "unavailable",
        error: "connect ECONNREFUSED",
      },
      canPrepare: true,
      canVerify: true,
      actionsDisabled: false,
      onStartPrepare,
      onVerify,
    }),
    container,
  );
  return { container, onStartPrepare, onVerify };
}

function text(container: Element): string {
  return container.textContent?.replace(/\s+/gu, " ").trim() ?? "";
}

describe("renderCurrentConnection", () => {
  beforeEach(async () => {
    await i18n.setLocale("en");
  });

  afterEach(() => {
    for (const container of document.body.querySelectorAll("div")) {
      render(nothing, container);
    }
    document.body.replaceChildren();
  });

  it.each([
    {
      brandId: "ollama",
      detail: "qwen3:8b at http://127.0.0.1:11434",
      kind: "provider-auto:ollama",
      modelRef: "ollama/qwen3:8b",
      optionId: "ollama",
    },
    {
      brandId: "llama-cpp",
      detail: "gemma-4-e4b-it-q4_k_m (downloaded)",
      kind: "provider-auto:llama-cpp",
      modelRef: "llama-cpp/gemma-4-e4b-it-q4_k_m",
      optionId: "llama-cpp",
    },
    {
      brandId: "lmstudio",
      detail: "qwen3-8b-instruct at http://localhost:1234/v1",
      kind: "provider-auto:lmstudio",
      modelRef: "lmstudio/qwen3-8b-instruct",
      optionId: "lmstudio",
    },
  ] as const)("recovers a failed $brandId connection in place", (fixture) => {
    const result: SystemAgentSetupDetectResult = {
      candidates: [
        {
          kind: fixture.kind,
          brandId: fixture.brandId,
          label: fixture.brandId,
          detail: fixture.detail,
          modelRef: fixture.modelRef,
          recommended: false,
          credentials: true,
        },
      ],
      manualProviders: [],
      prepareOptions,
      workspace: "/tmp/workspace",
      configuredModel: fixture.modelRef,
      setupComplete: true,
    };
    const { container, onStartPrepare, onVerify } = mount(result);

    expect(text(container)).toContain(fixture.detail);
    expect(text(container)).toContain("Needs attention");
    expect(text(container)).toContain(
      "Make sure the provider service is running and reachable, then retry.",
    );
    const buttons = [...container.querySelectorAll<HTMLButtonElement>("button")];
    expect(buttons.map((button) => button.textContent?.trim())).toEqual([
      "Change connection",
      "Retry verification",
    ]);

    buttons[0]?.click();
    expect(onStartPrepare).toHaveBeenCalledWith(expect.objectContaining({ id: fixture.optionId }));
    buttons[1]?.click();
    expect(onVerify).toHaveBeenCalledOnce();
  });
});
