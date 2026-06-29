import { Loader2 } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../types";

export function ChatBubble({
  message,
  activeView,
}: {
  message: ChatMessage;
  activeView: "workflows" | "scenes";
}) {
  const { t } = useTranslation();
  const user = message.role === "user";

  return (
    <div className={cn("flex", user ? "justify-end" : "justify-start")}>
      <div
        className={cn(
          "w-fit max-w-[78%] rounded-[14px] px-3.5 py-2 text-sm leading-6 xl:max-w-[760px]",
          user
            ? "rounded-br-[6px] bg-primary text-primary-foreground shadow-[0_8px_18px_rgb(15_23_42_/_0.12)]"
            : "rounded-bl-[6px] bg-muted/45 text-foreground",
        )}
      >
        {message.streaming ? (
          <div className="flex items-center gap-2 text-muted-foreground">
            <Loader2 className="size-4 shrink-0 animate-spin text-brand-500" />
            <span>{message.text || streamingLabel(message.state, activeView, t)}</span>
          </div>
        ) : (
          <div className="break-words [&_a]:underline [&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_li]:ml-4 [&_ol]:my-2 [&_p:first-child]:mt-0 [&_p:last-child]:mb-0 [&_p]:my-1 [&_pre]:my-2 [&_pre]:overflow-auto [&_pre]:rounded-[8px] [&_pre]:bg-muted [&_pre]:p-3 [&_table]:my-2 [&_table]:w-full [&_table]:border-collapse [&_table]:text-left [&_td]:border [&_td]:border-border [&_td]:px-2 [&_td]:py-1 [&_th]:border [&_th]:border-border [&_th]:bg-muted [&_th]:px-2 [&_th]:py-1 [&_ul]:my-2">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.text || ""}</ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  );
}

function streamingLabel(
  state: string | undefined,
  activeView: "workflows" | "scenes",
  t: ReturnType<typeof useTranslation>["t"],
) {
  if (activeView === "scenes") {
    if (state === "TASK_STATE_SUBMITTED" || state === "submitted") return t("chat.sceneStarting");
    if (state === "TASK_STATE_WORKING") return t("chat.sceneWorking");
    return t("chat.sceneFallback");
  }

  if (state === "TASK_STATE_SUBMITTED" || state === "submitted") return t("chat.skitterStarting");
  if (state === "TASK_STATE_WORKING") return t("chat.skitterWorkflow");
  return t("chat.skitterFallback");
}
