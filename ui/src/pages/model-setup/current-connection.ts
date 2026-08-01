import { html, nothing, type TemplateResult } from "lit";
import type { SystemAgentSetupDetectResult } from "../../api/types.ts";
import { icons } from "../../components/icons.ts";
import { providerIdFromModelRef, renderProviderBrandIcon } from "../../components/provider-icon.ts";
import { t } from "../../i18n/index.ts";
import type { ModelSetupPrepareOption } from "./prepare-options.ts";
import type { ModelSetupVerifyState } from "./state.ts";

type Candidate = SystemAgentSetupDetectResult["candidates"][number];

export function failureLabel(status: string): string {
  const labels: Record<string, string> = {
    auth: t("modelSetup.failure.auth"),
    rate_limit: t("modelSetup.failure.rateLimit"),
    billing: t("modelSetup.failure.billing"),
    timeout: t("modelSetup.failure.timeout"),
    format: t("modelSetup.failure.format"),
    unavailable: t("modelSetup.failure.unavailable"),
    unknown: t("modelSetup.failure.unknown"),
  };
  return labels[status] ?? labels.unknown!;
}

function failureGuidance(status: string): string {
  const guidance: Record<string, string> = {
    auth: t("modelSetup.failureGuidance.auth"),
    rate_limit: t("modelSetup.failureGuidance.rateLimit"),
    billing: t("modelSetup.failureGuidance.billing"),
    timeout: t("modelSetup.failureGuidance.unavailable"),
    format: t("modelSetup.failureGuidance.format"),
    unavailable: t("modelSetup.failureGuidance.unavailable"),
    unknown: t("modelSetup.failureGuidance.unknown"),
  };
  return guidance[status] ?? guidance.unknown!;
}

function normalizedProviderKey(value: string | undefined): string {
  return (
    value
      ?.trim()
      .toLowerCase()
      .replace(/[^a-z0-9]/gu, "") ?? ""
  );
}

export function findPrepareOptionForModel(
  result: SystemAgentSetupDetectResult,
  modelRef: string,
  candidate?: Candidate,
): ModelSetupPrepareOption | undefined {
  const providerKeys = new Set(
    [candidate?.brandId, modelRef.split("/", 1)[0]]
      .map(normalizedProviderKey)
      .filter((value) => value.length > 0),
  );
  return (result.prepareOptions ?? []).find((option) =>
    providerKeys.has(normalizedProviderKey(option.brandId ?? option.id)),
  );
}

export function renderModelSetupFailure(status: string, error: string): TemplateResult {
  return html`
    <div class="model-setup__failure" role="alert">
      <span class="model-setup__failure-icon" aria-hidden="true">${icons.alertTriangle}</span>
      <div>
        <strong>${failureLabel(status)}</strong>
        <p>${error}</p>
        <p class="muted">${failureGuidance(status)}</p>
      </div>
    </div>
  `;
}

function currentConnectionStatus(verify: ModelSetupVerifyState): {
  label: string;
  variant: "neutral" | "checking" | "success" | "danger";
} {
  switch (verify.phase) {
    case "checking":
      return { label: t("modelSetup.verify.status.checking"), variant: "checking" };
    case "ok":
      return { label: t("modelSetup.verify.status.connected"), variant: "success" };
    case "failed":
      return { label: t("modelSetup.verify.status.needsAttention"), variant: "danger" };
    default:
      return { label: t("modelSetup.verify.status.configured"), variant: "neutral" };
  }
}

export function renderCurrentConnection(props: {
  result: SystemAgentSetupDetectResult;
  verify: ModelSetupVerifyState;
  canPrepare: boolean;
  canVerify: boolean;
  actionsDisabled: boolean;
  onStartPrepare: (option: ModelSetupPrepareOption) => void;
  onVerify: () => void;
}): TemplateResult {
  const modelRef = props.result.configuredModel!;
  // A successful verify reports the model that actually answered; prefer it over
  // the detect-time snapshot so concurrent config changes cannot mislabel the result.
  const displayRef = props.verify.phase === "ok" ? props.verify.modelRef : modelRef;
  const providerId = providerIdFromModelRef(displayRef);
  const candidate = props.result.candidates.find((entry) => entry.modelRef === modelRef);
  const detail =
    candidate?.kind === "existing-model" || !candidate?.detail.trim() ? null : candidate.detail;
  const prepareOption = findPrepareOptionForModel(props.result, modelRef, candidate);
  const status = currentConnectionStatus(props.verify);
  return html`
    <section class="settings-section model-setup__current" data-verify-phase=${props.verify.phase}>
      <div class="settings-section__header">
        <h2>${t("modelSetup.verify.title")}</h2>
      </div>
      <div class="model-setup__row">
        <div class="model-setup__current-content">
          <div class="model-setup__provider-copy">
            ${providerId
              ? renderProviderBrandIcon(providerId, { className: "model-setup__icon" })
              : nothing}
            <div class="model-setup__current-copy">
              <div class="model-setup__current-title">
                <strong>${displayRef}</strong>
                <span class="model-setup__chip model-setup__chip--${status.variant}">
                  ${status.label}
                </span>
              </div>
              ${detail ? html`<div class="muted">${detail}</div>` : nothing}
              ${props.verify.phase === "checking"
                ? html`<div class="model-setup__testing" role="status">
                    ${t("modelSetup.verify.checking", { modelRef })}
                  </div>`
                : props.verify.phase === "ok"
                  ? html`<div class="model-setup__verified" role="status">
                      ${props.verify.latencyMs === undefined
                        ? t("modelSetup.verify.answered")
                        : t("modelSetup.verify.answeredIn", {
                            latencyMs: String(props.verify.latencyMs),
                          })}
                    </div>`
                  : nothing}
            </div>
          </div>
          ${props.verify.phase === "failed"
            ? renderModelSetupFailure(props.verify.status, props.verify.error)
            : nothing}
        </div>
        <div class="model-setup__row-actions model-setup__current-actions">
          ${prepareOption && props.canPrepare
            ? html`<button
                type="button"
                class="btn"
                ?disabled=${props.actionsDisabled}
                @click=${() => props.onStartPrepare(prepareOption)}
              >
                ${icons.settings}
                <span>${t("modelSetup.verify.changeConnection")}</span>
              </button>`
            : nothing}
          ${props.canVerify
            ? html`<button
                type="button"
                class=${`btn ${props.verify.phase === "failed" ? "primary" : ""}`}
                ?disabled=${props.actionsDisabled}
                @click=${props.onVerify}
              >
                ${props.verify.phase === "failed" ? icons.refresh : nothing}
                <span>
                  ${props.verify.phase === "failed"
                    ? t("modelSetup.verify.retry")
                    : t("modelSetup.verify.button")}
                </span>
              </button>`
            : nothing}
        </div>
      </div>
    </section>
  `;
}
