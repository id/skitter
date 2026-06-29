import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import type { AgentEntry, ChatSession } from "../types";
import { chatTracePanels } from "../workflow-model";
import { RuntimeProgressPanel } from "./runtime-progress";
import { WorkflowTrace } from "./workflow-trace";

interface WorkflowPreviewProps {
  traceAgents: AgentEntry[];
  workflows: AgentEntry[];
  activeView: "workflows" | "scenes";
  activeChat: ChatSession | null;
  className?: string;
}

export function WorkflowPreview({
  traceAgents,
  workflows,
  activeView,
  activeChat,
  className,
}: WorkflowPreviewProps) {
  const { t } = useTranslation();
  const { showTrace: hasTrace, showProgress: hasProgress } = chatTracePanels(activeChat);
  const isTaskAgentView = activeView === "scenes";
  const title = hasProgress ? t("preview.progress") : t("preview.tracePanel");

  return (
    <div
      className={cn(
        "flex max-h-[calc(100vh-5.25rem)] min-h-0 flex-col overflow-hidden rounded-[10px] border border-border/70 bg-background text-card-foreground shadow-[0_14px_34px_rgb(15_23_42_/_0.09)]",
        className,
      )}
    >
      <div className="flex h-10 shrink-0 items-center border-b border-border/60 px-3">
        <h3 className="min-w-0 truncate text-sm font-semibold leading-none tracking-tight">
          {title}
        </h3>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-3 pb-5">
        {hasTrace ? (
          <WorkflowTrace
            activeChat={activeChat}
            agents={traceAgents}
            workflows={workflows}
            className="border-0 bg-transparent p-0"
          />
        ) : hasProgress && activeChat?.runtimeProgress ? (
          <RuntimeProgressPanel
            progress={activeChat.runtimeProgress}
            className="border-0 bg-transparent p-0"
          />
        ) : (
          <div className="flex min-h-16 items-center justify-center rounded-[8px] border border-dashed border-border px-4 text-center text-xs text-muted-foreground">
            {isTaskAgentView ? t("preview.a2aTraceEmpty") : t("preview.workflowTraceEmpty")}
          </div>
        )}
      </div>
    </div>
  );
}
