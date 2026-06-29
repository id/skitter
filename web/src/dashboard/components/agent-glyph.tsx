import { Cpu, Fan, Layers3, LayoutList, LockKeyhole } from "lucide-react";
import type { AgentEntry } from "../types";

export function AgentGlyph({
  agent,
  className,
}: {
  agent: AgentEntry | null;
  className?: string;
}) {
  const text = `${agent?.id ?? ""} ${agent?.card.name ?? ""}`.toLowerCase();

  if (agent?.isApp) return <Layers3 className={className} />;
  if (text.includes("lock")) return <LockKeyhole className={className} />;
  if (text.includes("fan")) return <Fan className={className} />;
  if (text.includes("skitter")) return <LayoutList className={className} />;
  return <Cpu className={className} />;
}
