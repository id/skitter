import { displayName } from "./agent-utils";
import type { AgentEntry, ChatSession, WorkflowTaskDefinition, WorkflowTraceStep } from "./types";

const APP_EXTENSION_URI = "urn:skitter:app";

/** Which of the trace / progress panels a chat should show in the output column. */
export function chatTracePanels(activeChat: ChatSession | null) {
  const showTrace = Boolean(activeChat?.workflow || activeChat?.traceSteps.length);
  const showProgress = Boolean(activeChat?.runtimeProgress && !showTrace);
  return { showTrace, showProgress };
}

export interface WorkflowDefinition {
  workflowId: string;
  workflowName: string;
  description: string;
  tasks: WorkflowTaskDefinition[];
}

export function workflowDefinitionFromApp(
  app: AgentEntry | null | undefined,
  agents: Map<string, AgentEntry> = new Map(),
): WorkflowDefinition | null {
  if (!app) return null;

  const tasks = workflowTasksFromApp(app, agents);
  if (!tasks.length) return null;

  return {
    workflowId: app.id,
    workflowName: displayName(app),
    description: app.card.description ?? "",
    tasks,
  };
}

export function workflowTasksFromApp(
  app: AgentEntry,
  agents: Map<string, AgentEntry> = new Map(),
): WorkflowTaskDefinition[] {
  const tasks = app.card.capabilities?.extensions
    ?.find((extension) => extension.uri === APP_EXTENSION_URI)
    ?.params?.tasks;

  if (!Array.isArray(tasks)) return [];

  return tasks
    .filter((task): task is Record<string, unknown> => Boolean(task) && typeof task === "object")
    .map((task) => workflowTaskFromMetadata(task, agents))
    .filter((task) => task.id && task.agentId);
}

export function bindableWorkflowTasks(
  app: AgentEntry,
  agents: Map<string, AgentEntry>,
): WorkflowTaskDefinition[] {
  return workflowTasksFromApp(app, agents).filter((task) => {
    const agent = agents.get(task.agentId);
    return agent && !agent.isApp && !agent.isTaskAgent;
  });
}

function plannedWorkflowSteps(
  activeChat: ChatSession | null,
  workflows: AgentEntry[],
): WorkflowTraceStep[] {
  const workflow = workflows.find((item) => item.id === activeChat?.workflow?.appId);
  const definition = workflowDefinitionFromApp(workflow);
  if (!definition) return [];

  return definition.tasks.map((task) => ({
    id: task.id,
    sessionId: activeChat?.workflow?.sessionId ?? "",
    agentId: task.agentId,
    agentName: task.agentName,
    description: task.description || task.id,
    state: "waiting",
  }));
}

export function mergedWorkflowSteps(
  activeChat: ChatSession | null,
  workflows: AgentEntry[],
) {
  const actualSteps = [...(activeChat?.traceSteps ?? [])].sort(compareTraceSteps);
  const plannedSteps =
    activeChat?.targetId && activeChat.targetId !== "skitter"
      ? []
      : plannedWorkflowSteps(activeChat, workflows);

  return plannedSteps.length ? mergeTraceSteps(plannedSteps, actualSteps) : actualSteps;
}

function workflowTaskFromMetadata(
  task: Record<string, unknown>,
  agents: Map<string, AgentEntry>,
): WorkflowTaskDefinition {
  const agentId = String(task.agent ?? task.agent_id ?? "").trim();
  const agent = agents.get(agentId);
  const agentName = String(task.agentName ?? task.agent_name ?? "").trim();

  return {
    id: String(task.id ?? "").trim(),
    agentId,
    agentName: agentName || (agent ? displayName(agent) : undefined),
    description: String(task.description ?? "").trim(),
    needs: stringList(task.needs),
    hasExplicitNeeds: Array.isArray(task.needs),
    terminal: Boolean(task.terminal),
  };
}

function mergeTraceSteps(
  plannedSteps: WorkflowTraceStep[],
  actualSteps: WorkflowTraceStep[],
) {
  const usedActualIds = new Set<string>();

  const merged = plannedSteps.map((plannedStep) => {
    const actual =
      actualSteps.find((step) => !usedActualIds.has(step.id) && step.id === plannedStep.id) ??
      actualSteps.find((step) => !usedActualIds.has(step.id) && step.agentId === plannedStep.agentId);

    if (!actual) return plannedStep;

    usedActualIds.add(actual.id);
    return {
      ...plannedStep,
      ...actual,
      id: plannedStep.id,
      agentId: plannedStep.agentId,
      agentName: plannedStep.agentName,
      description: actual.description || plannedStep.description,
    };
  });

  const appendedActual = actualSteps.filter((step) => !usedActualIds.has(step.id));
  return [...merged, ...appendedActual];
}

function stringList(value: unknown) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item).trim()).filter(Boolean);
}

function compareTraceSteps(a: WorkflowTraceStep, b: WorkflowTraceStep) {
  if (!a.timestamp || !b.timestamp) return 0;
  return a.timestamp.localeCompare(b.timestamp);
}
