import { useMemo } from "react";
import { Loader2, Send } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { displayName } from "../agent-utils";
import type { AgentEntry, ChatSession, ConnectionState } from "../types";
import { RunOutput } from "./run-output";
import { WorkflowPreview } from "./workflow-preview";

interface OrchestrationPanelProps {
  skitterAgent: AgentEntry;
  activeView: "workflows" | "scenes";
  callableAgents: AgentEntry[];
  appAgents: AgentEntry[];
  taskAgents: AgentEntry[];
  activeChat: ChatSession | null;
  chatInput: string;
  setChatInput: (value: string) => void;
  sendChat: (text: string) => void;
  connection: ConnectionState;
}

export function OrchestrationPanel({
  skitterAgent,
  activeView,
  callableAgents,
  appAgents,
  taskAgents,
  activeChat,
  chatInput,
  setChatInput,
  sendChat,
  connection,
}: OrchestrationPanelProps) {
  const { t } = useTranslation();
  const isTaskAgentView = activeView === "scenes";
  const canSend = Boolean(
    chatInput.trim() &&
      skitterAgent.id &&
      !activeChat?.streaming &&
      (connection === "connected" || skitterAgent.isMock),
  );
  const contextTitle = isTaskAgentView ? displayName(skitterAgent) : "";
  const traceAgents = useMemo(
    () => [...callableAgents, ...taskAgents],
    [callableAgents, taskAgents],
  );

  return (
    <section className="h-full min-h-0 overflow-hidden bg-background">
      <div className="grid h-full min-h-0 grid-cols-1 gap-4 overflow-hidden bg-background px-5 pb-5 pt-2 xl:grid-cols-[minmax(0,1fr)_340px]">
        <div className="mx-auto flex min-h-0 w-full max-w-[980px] flex-col overflow-visible">
          {contextTitle ? (
            <div className="flex h-10 shrink-0 items-center justify-between gap-3 px-1">
              <div className="flex min-w-0 items-baseline">
                <div className="truncate text-sm font-semibold tracking-tight">{contextTitle}</div>
              </div>
            </div>
          ) : null}
          <div className="flex min-h-0 flex-1">
            <RunOutput
              activeChat={activeChat}
              traceAgents={traceAgents}
              workflows={appAgents}
              activeView={activeView}
            />
          </div>
          <PromptComposer
            chatInput={chatInput}
            setChatInput={setChatInput}
            canSend={canSend}
            streaming={Boolean(activeChat?.streaming)}
            sendChat={sendChat}
            placeholder={isTaskAgentView ? t("chat.scenePlaceholder") : t("chat.workflowPlaceholder")}
            sendLabel={t("chat.send")}
          />
        </div>
        <WorkflowPreview
          traceAgents={traceAgents}
          workflows={appAgents}
          activeView={activeView}
          activeChat={activeChat}
          className="hidden w-full self-start xl:mt-3 xl:flex"
        />
      </div>
    </section>
  );
}

interface PromptComposerProps {
  chatInput: string;
  setChatInput: (value: string) => void;
  canSend: boolean;
  streaming: boolean;
  sendChat: (text: string) => void;
  placeholder: string;
  sendLabel: string;
}

function PromptComposer({
  chatInput,
  setChatInput,
  canSend,
  streaming,
  sendChat,
  placeholder,
  sendLabel,
}: PromptComposerProps) {
  function submit() {
    sendChat(chatInput);
    setChatInput("");
  }

  return (
    <div className="shrink-0 bg-background pb-3 pt-6">
      <div className="flex min-w-0 items-center gap-2 rounded-[13px] border border-border/70 bg-background p-1.5 shadow-[0_12px_32px_rgb(15_23_42_/_0.1)] transition-[border-color,box-shadow] focus-within:border-ring/35 focus-within:shadow-[0_0_0_3px_rgb(94_78_255_/_0.08),0_16px_36px_rgb(15_23_42_/_0.12)]">
        <Textarea
          value={chatInput}
          onChange={(event) => setChatInput(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey && canSend) {
              event.preventDefault();
              submit();
            }
          }}
          placeholder={placeholder}
          rows={1}
          className="h-10 max-h-28 min-h-10 min-w-0 flex-1 resize-none overflow-y-auto border-0 bg-transparent px-2.5 py-2 text-sm leading-6 shadow-none focus-visible:ring-0"
        />
        <Button
          disabled={!canSend}
          size="icon-sm"
          className="shrink-0 shadow-none"
          aria-label={sendLabel}
          onClick={submit}
        >
          {streaming ? <Loader2 className="size-4 animate-spin" /> : <Send className="size-4" />}
        </Button>
      </div>
    </div>
  );
}
