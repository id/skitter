import { CheckCircle2, CircleDashed, Loader2, XCircle } from "lucide-react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { displayName } from "../agent-utils";
import type { AgentEntry, ChatSession, WorkflowTraceStep } from "../types";
import { mergedWorkflowSteps } from "../workflow-model";
import { AgentGlyph } from "./agent-glyph";
import { WorkflowGraphViewer } from "./workflow-graph-viewer";

interface WorkflowTraceProps {
  activeChat: ChatSession | null;
  agents: AgentEntry[];
  workflows?: AgentEntry[];
  className?: string;
}

export function WorkflowTrace({
  activeChat,
  agents,
  workflows = [],
  className,
}: WorkflowTraceProps) {
  const { t } = useTranslation();
  const isProcessing = Boolean(activeChat?.streaming);
  const isTaskAgentChat = Boolean(activeChat?.targetId && activeChat.targetId !== "skitter");
  const steps = mergedWorkflowSteps(activeChat, workflows);
  const hasRuntimeTrace = Boolean(activeChat?.workflow || steps.length || isProcessing);

  if (!hasRuntimeTrace) return null;

  const agentsById = new Map(agents.map((agent) => [agent.id, agent]));
  const state = traceState(activeChat, steps);

  if (isTaskAgentChat) {
    return (
      <SceneTrace
        activeChat={activeChat}
        agentsById={agentsById}
        className={className}
        state={state}
        steps={steps}
        t={t}
      />
    );
  }

  return (
    <div className={cn("rounded-[12px] border border-border/70 bg-background p-3", className)}>
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0 flex-1 truncate text-xs leading-5 text-muted-foreground">
          {runtimeTraceLabel(activeChat, steps, workflows, t)}
        </div>
        <TraceStateBadge state={state} t={t} />
      </div>

      {steps.length > 0 ? (
        <WorkflowGraphViewer
          activeChat={activeChat}
          agents={agents}
          workflows={workflows}
          className="mt-3 h-64"
          expandable
          expandTitle={traceFlowTitle(activeChat, workflows, t)}
          expandDescription={runtimeTraceLabel(activeChat, steps, workflows, t)}
        />
      ) : null}

      {steps.length > 0 ? <div className="my-3 h-px w-full shrink-0 bg-border" /> : null}

      <RuntimeTrace
        agentsById={agentsById}
        steps={steps}
        t={t}
      />
    </div>
  );
}

function SceneTrace({
  activeChat,
  agentsById,
  className,
  state,
  steps,
  t,
}: {
  activeChat: ChatSession | null;
  agentsById: Map<string, AgentEntry>;
  className?: string;
  state: string;
  steps: WorkflowTraceStep[];
  t: TFunction;
}) {
  return (
    <div className={cn("rounded-[12px] border border-border/70 bg-muted/10 p-3", className)}>
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0">
          <div className="text-sm font-semibold">{t("trace.a2aSteps")}</div>
          <div className="mt-0.5 truncate text-xs text-muted-foreground">
            {runtimeTraceLabel(activeChat, steps, [], t)}
          </div>
        </div>
        <TraceStateBadge state={state} t={t} />
      </div>

      <div className="mt-3 flex flex-col gap-2">
        {steps.length > 0 ? (
          steps.map((step, index) => (
            <SceneStepRow
              key={step.id}
              agent={agentsById.get(step.agentId)}
              description={step.description || step.id}
              index={index + 1}
              name={displayTraceAgentName(step, agentsById)}
              result={step.error || step.result}
              state={step.state}
              t={t}
            />
          ))
        ) : (
          <PreparingTrace message={t("trace.scenePlanning")} />
        )}
      </div>
    </div>
  );
}

function SceneStepRow({
  agent,
  description,
  index,
  name,
  result,
  state,
  t,
}: {
  agent?: AgentEntry;
  description: string;
  index: number;
  name: string;
  result?: string;
  state: string;
  t: TFunction;
}) {
  return (
    <div className="grid min-w-0 grid-cols-[1.75rem_minmax(0,1fr)] gap-2 rounded-[9px] border border-border/60 bg-background px-2.5 py-2">
      <span className="flex size-6 items-center justify-center rounded-[7px] bg-muted/55 text-muted-foreground">
        <StateIcon state={state} />
      </span>
      <div className="min-w-0">
        <div className="flex min-w-0 items-center gap-2">
          <span className="text-xs font-medium text-muted-foreground">{index}</span>
          <span className="flex size-5 shrink-0 items-center justify-center rounded-[6px] bg-muted/40 text-muted-foreground">
            {agent ? <AgentGlyph agent={agent} className="size-3" /> : <CircleDashed className="size-3" />}
          </span>
          <span className="min-w-0 flex-1 truncate text-sm font-semibold">{name}</span>
        </div>
        <div className="mt-1 line-clamp-2 text-xs leading-5 text-muted-foreground">
          {traceDescription(description, t)}
        </div>
        {result ? (
          <div className="mt-1 line-clamp-2 text-xs leading-5 text-muted-foreground">
            {compactSnippet(result, 140)}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function RuntimeTrace({
  agentsById,
  steps,
  t,
}: {
  agentsById: Map<string, AgentEntry>;
  steps: WorkflowTraceStep[];
  t: TFunction;
}) {
  return (
    <div className="flex flex-col gap-3">
      {steps.length > 0 ? (
        <div className="flex flex-col gap-3">
          {steps.map((step, index) => (
            <TraceStepRow
              key={step.id}
              isLast={index === steps.length - 1}
              name={displayTraceAgentName(step, agentsById)}
              agent={agentsById.get(step.agentId)}
              description={step.description || step.id}
              state={step.state}
              result={step.result}
              error={step.error}
              t={t}
            />
          ))}
        </div>
      ) : (
        <PreparingTrace message={t("trace.skitterPreparing")} />
      )}
    </div>
  );
}

function TraceStepRow({
  isLast,
  name,
  agent,
  description,
  state,
  result,
  error,
  t,
}: {
  isLast: boolean;
  name: string;
  agent?: AgentEntry;
  description: string;
  state: string;
  result?: string;
  error?: string;
  t: TFunction;
}) {
  const resultSnippet = error || compactSnippet(result, 140);
  const stepTitle = traceDescription(description, t);

  return (
    <div className="grid min-w-0 grid-cols-[1.75rem_minmax(0,1fr)] gap-2.5">
      <div className="relative flex justify-center">
        {!isLast ? <span className="absolute bottom-[-0.75rem] top-7 w-px bg-border" /> : null}
        <span
          className={cn(
            "z-10 flex size-7 items-center justify-center rounded-full border bg-background",
            state === "completed" && "border-online-solid/25 text-online-solid",
            state === "failed" && "border-destructive/25 text-destructive",
            state === "running" && "border-brand-500/25 text-brand-500",
            state === "waiting" && "border-border text-muted-foreground",
          )}
        >
          <StateIcon state={state} />
        </span>
      </div>

      <div className="min-w-0 pb-4">
        <div className="flex min-w-0 items-start justify-between gap-2.5">
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm font-semibold leading-5">{stepTitle}</div>
            <div className="mt-1 flex min-w-0 items-center gap-2 text-xs leading-5 text-muted-foreground">
              <span className="flex shrink-0 text-muted-foreground">
                {agent ? <AgentGlyph agent={agent} className="size-4" /> : <CircleDashed className="size-4" />}
              </span>
              <span className="min-w-0 flex-1 truncate font-medium text-foreground">{name}</span>
            </div>
          </div>
          {state !== "completed" ? <TraceStateBadge state={state} t={t} /> : null}
        </div>

        {resultSnippet ? (
          <div
            className={cn(
              "mt-1.5 line-clamp-2 text-xs leading-5",
              error ? "text-destructive" : "text-muted-foreground",
            )}
          >
            {resultSnippet}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function PreparingTrace({ message }: { message: string }) {
  return (
    <div
      aria-label={message}
      className="rounded-[10px] border border-border/60 bg-background px-4 py-4"
      role="img"
    >
      <div aria-hidden="true" className="grid grid-cols-[2.25rem_minmax(0,1fr)] gap-3">
        <div className="flex justify-center">
          <span className="flex size-8 items-center justify-center rounded-[9px] border border-brand-500/25 bg-brand-500/10">
            <Loader2 className="size-4 animate-spin text-brand-500" />
          </span>
        </div>
        <div className="space-y-2 rounded-[10px] bg-muted/25 p-3">
          <div className="h-3 w-2/3 rounded bg-muted-foreground/15" />
          <div className="h-3 w-1/2 rounded bg-muted-foreground/10" />
        </div>
      </div>
      <div aria-hidden="true" className="mt-3 grid grid-cols-[2.25rem_minmax(0,1fr)] gap-3 opacity-55">
        <div className="flex justify-center">
          <span className="size-8 rounded-[9px] bg-muted/55" />
        </div>
        <div className="space-y-2 rounded-[10px] bg-muted/20 p-3">
          <div className="h-3 w-3/5 rounded bg-muted-foreground/10" />
          <div className="h-3 w-1/2 rounded bg-muted-foreground/10" />
        </div>
      </div>
    </div>
  );
}

function TraceStateBadge({ state, t }: { state: string; t: TFunction }) {
  const badgeClassName = "shrink-0 whitespace-nowrap";
  if (state === "completed") {
    return (
      <Badge variant="success" className={badgeClassName}>
        {t("trace.completed")}
      </Badge>
    );
  }
  if (state === "failed") {
    return (
      <Badge variant="outline" className={cn("border-destructive/25 text-destructive", badgeClassName)}>
        {t("trace.failed")}
      </Badge>
    );
  }
  if (state === "waiting") {
    return (
      <Badge variant="muted" className={badgeClassName}>
        {t("trace.waiting")}
      </Badge>
    );
  }
  if (state === "running") {
    return (
      <Badge variant="secondary" className={badgeClassName}>
        {t("trace.running")}
      </Badge>
    );
  }
  return (
    <Badge variant="secondary" className={badgeClassName}>
      {state}
    </Badge>
  );
}

function StateIcon({ state }: { state: string }) {
  if (state === "completed") return <CheckCircle2 className="size-4 shrink-0 text-online-solid" />;
  if (state === "failed") return <XCircle className="size-4 shrink-0 text-destructive" />;
  if (state === "waiting") return <CircleDashed className="size-4 shrink-0 text-muted-foreground" />;
  return <Loader2 className="size-4 shrink-0 animate-spin text-brand-500" />;
}

function runtimeTraceLabel(
  activeChat: ChatSession | null,
  steps: WorkflowTraceStep[],
  workflows: AgentEntry[],
  t: TFunction,
) {
  if (activeChat?.targetId && activeChat.targetId !== "skitter") {
    const completed = steps.filter((step) => step.state === "completed").length;
    const total = steps.length;
    if (total > 0) return t("trace.a2aStepsCompleted", { completed, total });
    return t("trace.preparingA2A");
  }

  const workflow = activeChat?.workflow;
  if (!workflow?.appId) return t("trace.preparingCallFlow");
  const completed = steps.filter((step) => step.state === "completed").length;
  const total = steps.length;
  const description = workflows.find((item) => item.id === workflow.appId)?.card.description;
  if (total > 0) {
    const progress = t("trace.stepsCompleted", { completed, total });
    return description ? `${progress} · ${description}` : progress;
  }
  return description || t("trace.preparingCallFlow");
}

function traceFlowTitle(
  activeChat: ChatSession | null,
  workflows: AgentEntry[],
  t: TFunction,
) {
  const app = workflows.find((item) => item.id === activeChat?.workflow?.appId);
  return (app ? displayName(app) : "") || t("flow.title");
}

function traceState(activeChat: ChatSession | null, steps: WorkflowTraceStep[]) {
  if (activeChat?.workflow?.state) return activeChat.workflow.state;
  if (activeChat?.streaming) return "running";
  if (steps.some((step) => step.state === "failed")) return "failed";
  if (steps.length > 0) return "completed";
  return "running";
}

function displayTraceAgentName(step: WorkflowTraceStep, agentsById: Map<string, AgentEntry>) {
  const agent = agentsById.get(step.agentId);
  return agent ? displayName(agent) : step.agentName || step.agentId || step.id;
}

function traceDescription(value: string, t: TFunction) {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (/interpret user request/i.test(normalized)) return t("trace.planTask");
  return compactSnippet(normalized, 90) ?? normalized;
}

function compactSnippet(value?: string, limit = 260) {
  if (!value) return undefined;
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) return undefined;
  return normalized.length > limit ? `${normalized.slice(0, limit - 3)}...` : normalized;
}
