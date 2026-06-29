import type { AgentEntry } from "./types";

export function displayName(agent: AgentEntry | null) {
  return agent?.card.name ?? agent?.id ?? "";
}
