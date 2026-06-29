import { CheckCircle2, CircleDashed, Loader2, XCircle } from "lucide-react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import type {
  RuntimeProgress,
  RuntimeProgressKind,
  RuntimeProgressPhase,
} from "../types";

interface RuntimeProgressPanelProps {
  progress: RuntimeProgress;
  className?: string;
}

type ProgressStepId = "submitted" | "building" | "preparing_run" | "registering" | "completed";
type ProgressStepState = "waiting" | "running" | "completed" | "failed";

const CREATE_STEPS: ProgressStepId[] = ["submitted", "building", "registering", "completed"];
const RUN_STEPS: ProgressStepId[] = ["submitted", "preparing_run", "completed"];
const REQUEST_STEPS: ProgressStepId[] = ["submitted", "building", "completed"];

export function RuntimeProgressPanel({ progress, className }: RuntimeProgressPanelProps) {
  const { t } = useTranslation();
  const steps = progressSteps(progress.kind);
  const activeIndex = progressIndex(progress.phase, steps);

  return (
    <div className={cn("rounded-[14px] border border-border/70 bg-muted/10 p-4", className)}>
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="text-sm font-semibold">{progressTitle(progress.kind, t)}</div>
          <div className="mt-1 truncate text-xs leading-5 text-muted-foreground">
            {progressDetail(progress, t)}
          </div>
        </div>
        <ProgressBadge phase={progress.phase} />
      </div>

      <div className="mt-4 flex flex-col gap-2">
        {steps.map((step, index) => (
          <ProgressRow
            key={step}
            label={progressStepLabel(progress.kind, step, t)}
            state={stepState(progress.phase, index, activeIndex, steps.length)}
          />
        ))}
      </div>
    </div>
  );
}

function ProgressRow({ label, state }: { label: string; state: ProgressStepState }) {
  return (
    <div className="grid min-w-0 grid-cols-[1.75rem_minmax(0,1fr)] gap-2 rounded-[9px] border border-border/60 bg-background px-2.5 py-2">
      <span className="flex size-6 items-center justify-center rounded-[7px] bg-muted/55 text-muted-foreground">
        <ProgressIcon state={state} />
      </span>
      <span
        className={cn(
          "min-w-0 self-center truncate text-sm font-medium",
          state === "waiting" && "text-muted-foreground",
          state === "failed" && "text-destructive",
        )}
      >
        {label}
      </span>
    </div>
  );
}

function ProgressBadge({ phase }: { phase: RuntimeProgressPhase }) {
  const { t } = useTranslation();
  if (phase === "completed") {
    return (
      <span className="shrink-0 whitespace-nowrap rounded-full bg-online-solid/10 px-2 py-0.5 text-xs font-medium text-online-solid">
        {t("progress.completed")}
      </span>
    );
  }
  if (phase === "failed") {
    return <span className="shrink-0 whitespace-nowrap rounded-full bg-destructive/10 px-2 py-0.5 text-xs font-medium text-destructive">{t("progress.failed")}</span>;
  }
  return <span className="shrink-0 whitespace-nowrap rounded-full bg-brand-500/10 px-2 py-0.5 text-xs font-medium text-brand-500">{t("progress.running")}</span>;
}

function ProgressIcon({ state }: { state: ProgressStepState }) {
  if (state === "completed") return <CheckCircle2 className="size-4 shrink-0 text-online-solid" />;
  if (state === "failed") return <XCircle className="size-4 shrink-0 text-destructive" />;
  if (state === "running") return <Loader2 className="size-4 shrink-0 animate-spin text-brand-500" />;
  return <CircleDashed className="size-4 shrink-0 text-muted-foreground" />;
}

function stepState(
  phase: RuntimeProgressPhase,
  index: number,
  activeIndex: number,
  total: number,
): ProgressStepState {
  if (phase === "completed") return "completed";
  if (phase === "failed") return index === total - 1 ? "failed" : "completed";
  if (index < activeIndex) return "completed";
  if (index === activeIndex) return "running";
  return "waiting";
}

function progressIndex(phase: RuntimeProgressPhase, steps: ProgressStepId[]) {
  if (phase === "failed") return steps.length - 1;
  const index = steps.indexOf(phase);
  return index >= 0 ? index : 0;
}

function progressSteps(kind: RuntimeProgressKind) {
  if (kind === "workflow_run") return RUN_STEPS;
  if (kind === "workflow_create") return CREATE_STEPS;
  return REQUEST_STEPS;
}

function progressTitle(kind: RuntimeProgressKind, t: ReturnType<typeof useTranslation>["t"]) {
  if (kind === "workflow_request") return t("progress.requestTitle");
  return kind === "workflow_run" ? t("progress.runTitle") : t("progress.createTitle");
}

function progressSubtitle(
  kind: RuntimeProgressKind,
  phase: RuntimeProgressPhase,
  t: ReturnType<typeof useTranslation>["t"],
) {
  if (kind === "workflow_request") return t("progress.requestSubtitle");
  if (kind === "workflow_run") return t("progress.runSubtitle");
  if (phase === "submitted") return t("progress.createSubmitted");
  if (phase === "registering") return t("progress.createRegistering");
  return t("progress.createSubtitle");
}

function progressDetail(
  progress: RuntimeProgress,
  t: ReturnType<typeof useTranslation>["t"],
) {
  const detail = progress.detail ?? "";
  if (/planning workflow request/i.test(detail)) {
    return progress.kind === "workflow_create"
      ? t("progress.createSubtitle")
      : t("progress.requestSubtitle");
  }
  if (/register|publish/i.test(detail)) {
    return t("progress.createRegistering");
  }
  return detail || progressSubtitle(progress.kind, progress.phase, t);
}

function progressStepLabel(
  kind: RuntimeProgressKind,
  step: ProgressStepId,
  t: ReturnType<typeof useTranslation>["t"],
) {
  if (kind === "workflow_request" && step === "building") return t("progress.steps.resolving");
  return t(`progress.steps.${step}`);
}
