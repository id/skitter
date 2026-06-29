import { ScrollArea } from "@/components/ui/scroll-area";
import type { AgentEntry, ChatSession } from "../types";
import { chatTracePanels } from "../workflow-model";
import { ChatBubble } from "./chat-bubble";
import { RuntimeProgressPanel } from "./runtime-progress";
import { WorkflowTrace } from "./workflow-trace";

export function RunOutput({
  activeChat,
  traceAgents,
  workflows,
  activeView,
}: {
  activeChat: ChatSession | null;
  traceAgents: AgentEntry[];
  workflows: AgentEntry[];
  activeView: "workflows" | "scenes";
}) {
  const { showTrace, showProgress } = chatTracePanels(activeChat);
  const showMessages = Boolean(activeChat?.messages.length);

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
      <ScrollArea className="min-h-0 flex-1">
        <div className="flex w-full flex-col gap-3 px-2 py-3">
          {showTrace || showProgress || showMessages ? (
            <>
              {showTrace ? (
                <WorkflowTrace
                  activeChat={activeChat}
                  agents={traceAgents}
                  workflows={workflows}
                  className="xl:hidden"
                />
              ) : null}
              {showProgress && activeChat?.runtimeProgress ? (
                <RuntimeProgressPanel progress={activeChat.runtimeProgress} className="xl:hidden" />
              ) : null}
              {activeChat?.messages.map((message) => (
                <ChatBubble key={message.id} message={message} activeView={activeView} />
              ))}
            </>
          ) : null}
        </div>
      </ScrollArea>
    </div>
  );
}
