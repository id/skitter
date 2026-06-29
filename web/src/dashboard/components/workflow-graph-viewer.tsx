import {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import { CheckCircle2, CircleDashed, Expand, Flag, Loader2, XCircle } from "lucide-react";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";
import { productIdFromAgentId } from "../device-binding";
import type {
  AgentEntry,
  ChatSession,
  WorkflowState,
  WorkflowTaskDefinition,
  WorkflowTraceStep,
} from "../types";
import {
  type WorkflowDefinition,
  mergedWorkflowSteps,
  workflowDefinitionFromApp,
} from "../workflow-model";
import { AgentGlyph } from "./agent-glyph";

type FlowState = WorkflowTraceStep["state"];

interface FlowTask {
  id: string;
  agentId: string;
  agentName?: string;
  description: string;
  needs: string[];
  hasExplicitNeeds: boolean;
  terminal: boolean;
  state: FlowState;
  result?: string;
  error?: string;
}

interface WorkflowGraphViewerProps {
  activeChat?: ChatSession | null;
  agents: AgentEntry[];
  workflows?: AgentEntry[];
  workflow?: AgentEntry | null;
  className?: string;
  expandable?: boolean;
  expandTitle?: string;
  expandDescription?: string;
}

interface FlowNodeData extends Record<string, unknown> {
  task: FlowTask;
  agent?: AgentEntry;
  index: number;
  isStart: boolean;
}

type FlowNode = Node<FlowNodeData, "workflowTask">;

const nodeTypes = {
  workflowTask: WorkflowTaskNode,
};

export function WorkflowGraphViewer({
  activeChat,
  agents,
  workflows = [],
  workflow,
  className,
  expandable,
  expandTitle,
  expandDescription,
}: WorkflowGraphViewerProps) {
  const { t } = useTranslation();
  const agentsById = useMemo(() => new Map(agents.map((agent) => [agent.id, agent])), [agents]);
  const workflowFromChat = workflows.find((item) => item.id === activeChat?.workflow?.appId);
  const selectedWorkflow = workflowFromChat ?? workflow;
  const definition = useMemo(
    () => workflowDefinitionFromApp(selectedWorkflow, agentsById),
    [agentsById, selectedWorkflow],
  );
  const tasks = useMemo(
    () => graphTasks(definition, activeChat, workflows),
    [activeChat, definition, workflows],
  );
  const graph = useMemo(() => buildFlowGraph(tasks, agentsById), [agentsById, tasks]);

  // Changes only on real data changes (structure / task state), never on drag, so it
  // can drive a controlled re-seed without fighting manual node positions.
  const graphKey = useMemo(
    () => tasks.map((task) => `${task.id}:${task.state}:${task.terminal ? 1 : 0}`).join("|"),
    [tasks],
  );

  const showMiniMap = tasks.length > 6;

  if (!tasks.length) {
    return (
      <div
        className={cn(
          "flex h-56 min-h-0 items-center justify-center rounded-xl border border-dashed border-border bg-muted/15 px-4 text-center text-xs text-muted-foreground",
          className,
        )}
      >
        {t("flow.empty")}
      </div>
    );
  }

  const canvas = (
    <div
      className={cn(
        "workflow-flow relative h-64 min-h-0 overflow-hidden rounded-xl border border-border/70 bg-muted/15",
        className,
      )}
      aria-label={t("flow.title")}
    >
      <FlowCanvas graphKey={graphKey} nodes={graph.nodes} edges={graph.edges} showMiniMap={showMiniMap} />
      {expandable ? (
        <DialogTrigger asChild>
          <button
            type="button"
            className="absolute right-2 top-2 z-10 inline-flex items-center gap-1.5 rounded-lg border border-border/70 bg-background/85 px-2.5 py-1.5 text-xs font-medium text-muted-foreground shadow-sm backdrop-blur transition-colors hover:text-foreground"
            aria-label={t("flow.expand")}
          >
            <Expand className="size-3.5" />
            <span>{t("flow.expand")}</span>
          </button>
        </DialogTrigger>
      ) : null}
    </div>
  );

  if (!expandable) return canvas;

  return (
    <Dialog>
      {canvas}
      <DialogContent className="inset-0 left-0 top-0 h-screen w-screen max-w-none translate-x-0 translate-y-0 grid-rows-[auto_minmax(0,1fr)] gap-0 rounded-none border-0 p-0">
        <DialogHeader className="border-b border-border/70 px-6 py-4 pr-14">
          <DialogTitle className="min-w-0 truncate">{expandTitle || t("flow.title")}</DialogTitle>
          {expandDescription ? (
            <DialogDescription className="line-clamp-2 max-w-4xl">{expandDescription}</DialogDescription>
          ) : null}
        </DialogHeader>
        <div className="min-h-0 min-w-0">
          <WorkflowGraphViewer
            activeChat={activeChat}
            agents={agents}
            workflows={workflows}
            workflow={workflow}
            className="h-full rounded-none border-0"
          />
        </div>
      </DialogContent>
    </Dialog>
  );
}

function FlowCanvas({
  graphKey,
  nodes: seedNodes,
  edges: seedEdges,
  showMiniMap,
}: {
  graphKey: string;
  nodes: FlowNode[];
  edges: Edge[];
  showMiniMap: boolean;
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState(seedNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(seedEdges);

  // Reset to the freshly computed graph whenever the workflow data changes. Guarded by
  // graphKey (which never changes on drag) so manual node positions are preserved.
  const [seededKey, setSeededKey] = useState(graphKey);
  if (seededKey !== graphKey) {
    setSeededKey(graphKey);
    setNodes(seedNodes);
    setEdges(seedEdges);
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.18, maxZoom: 1.4 }}
      minZoom={0.1}
      maxZoom={1.8}
      panOnScroll
      proOptions={{ hideAttribution: true }}
      nodesDraggable
      nodesConnectable={false}
      edgesReconnectable={false}
      edgesFocusable={false}
      selectNodesOnDrag={false}
    >
      <Background variant={BackgroundVariant.Dots} gap={22} size={1.4} />
      <Controls showInteractive={false} position="bottom-right" />
      {showMiniMap ? (
        <MiniMap
          pannable
          zoomable
          position="bottom-left"
          nodeColor={miniMapNodeColor}
          nodeStrokeWidth={0}
          nodeBorderRadius={6}
        />
      ) : null}
    </ReactFlow>
  );
}

function WorkflowTaskNode({ data }: NodeProps<FlowNode>) {
  const { t } = useTranslation();
  const { task, agent, index, isStart } = data;
  const title = task.description || task.id;
  const agentName = agentLabel(agent, task.agentName, task.agentId);
  const resolved = Boolean(agent?.card.name || task.agentName);
  const stepLabel = isStart ? t("flow.start") : t("flow.step", { index: index + 1 });

  return (
    <div
      className={cn(
        "workflow-node group w-[300px] overflow-hidden rounded-2xl border border-border bg-card text-card-foreground",
        "shadow-[0_4px_14px_-4px_rgb(15_23_42_/_0.12),0_2px_6px_-3px_rgb(15_23_42_/_0.10)]",
        "transition-shadow duration-200 hover:shadow-[0_14px_32px_-12px_rgb(15_23_42_/_0.30)]",
        task.state === "running" && "ring-2 ring-brand-500/35",
        task.state === "failed" && "ring-2 ring-destructive/30",
      )}
    >
      <Handle type="target" position={Position.Left} />

      <div className="flex items-center justify-between gap-2 border-b border-border/60 px-4 py-2.5">
        <span className="inline-flex min-w-0 items-center gap-1.5 text-xs font-semibold text-foreground">
          {isStart ? <Flag className="size-3 shrink-0 text-sky-500" /> : null}
          <span className="truncate uppercase tracking-wide text-muted-foreground">{stepLabel}</span>
        </span>
        <StatusBadge state={task.state} />
      </div>

      <div className="px-4 py-3">
        <div className="line-clamp-3 text-[13px] font-medium leading-5 text-foreground">{title}</div>
        <div className="mt-3 flex min-w-0 items-center gap-2">
          <span className="flex size-6 shrink-0 items-center justify-center rounded-lg bg-muted/60 text-muted-foreground">
            {agent ? <AgentGlyph agent={agent} className="size-3.5" /> : <CircleDashed className="size-3.5" />}
          </span>
          <span
            className={cn(
              "min-w-0 flex-1 truncate text-xs text-muted-foreground",
              !resolved && "font-mono text-[11px]",
            )}
          >
            {agentName}
          </span>
          {task.terminal ? (
            <span className="inline-flex shrink-0 items-center rounded-full bg-amber-500/12 px-2 py-0.5 text-[10px] font-semibold uppercase leading-none tracking-wider text-amber-600 dark:text-amber-300">
              {t("flow.terminal")}
            </span>
          ) : null}
        </div>
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  );
}

// Resolve a readable agent label, never a raw URN id. Prefer the registry card name,
// then the workflow-provided name, then the decoded product id from a URN agent id.
function agentLabel(agent: AgentEntry | undefined, fallbackName: string | undefined, agentId: string) {
  const name = agent?.card.name?.trim() || fallbackName?.trim();
  if (name) return name;
  if (agentId.includes(".p.")) return productIdFromAgentId(agentId) || agentId;
  return agentId;
}

function StatusBadge({ state }: { state: FlowState }) {
  return (
    <span
      className={cn(
        "flex size-6 shrink-0 items-center justify-center rounded-full border",
        statusBadgeClass(state),
      )}
    >
      <StateIcon state={state} />
    </span>
  );
}

function graphTasks(
  definition: WorkflowDefinition | null,
  activeChat: ChatSession | null | undefined,
  workflows: AgentEntry[],
) {
  const steps = mergedWorkflowSteps(activeChat ?? null, workflows);
  if (definition) return mergeDefinitionWithTrace(definition.tasks, steps);
  return traceTasks(steps);
}

function mergeDefinitionWithTrace(
  tasks: WorkflowTaskDefinition[],
  steps: WorkflowTraceStep[],
): FlowTask[] {
  const stepsById = new Map(steps.map((step) => [step.id, step]));
  const usedStepIds = new Set<string>();

  const merged = tasks.map((task) => {
    const step = stepsById.get(task.id) ?? steps.find((item) => !usedStepIds.has(item.id) && item.agentId === task.agentId);
    if (step) usedStepIds.add(step.id);

    return {
      ...task,
      agentName: task.agentName || step?.agentName,
      description: step?.description || task.description || task.id,
      state: step?.state ?? "waiting",
      result: step?.result,
      error: step?.error,
    };
  });

  // Chain runtime-only steps after the last planned task so they connect into
  // the graph instead of floating as orphaned "Start" nodes.
  let previousId = merged.length ? merged[merged.length - 1].id : "";
  const appended = steps
    .filter((step) => !usedStepIds.has(step.id))
    .map((step) => {
      const task = traceStepTask(step, previousId);
      previousId = task.id;
      return task;
    });

  return [...merged, ...appended];
}

function traceTasks(steps: WorkflowTraceStep[]) {
  return steps.map((step, index) => traceStepTask(step, steps[index - 1]?.id));
}

function traceStepTask(step: WorkflowTraceStep, previousId = ""): FlowTask {
  return {
    id: step.id,
    agentId: step.agentId,
    agentName: step.agentName,
    description: step.description || step.id,
    needs: previousId ? [previousId] : [],
    hasExplicitNeeds: false,
    terminal: false,
    state: step.state,
    result: step.result,
    error: step.error,
  };
}

function buildFlowGraph(
  tasks: FlowTask[],
  agentsById: Map<string, AgentEntry>,
): { nodes: FlowNode[]; edges: Edge[] } {
  const layout = taskLayout(tasks);
  const edges = graphEdges(tasks);
  const incomingTaskIds = new Set(edges.map((edge) => edge.target));
  const nodes: FlowNode[] = tasks.map((task, index) => ({
    id: task.id,
    type: "workflowTask",
    position: layout.get(task.id) ?? { x: 0, y: 0 },
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    data: {
      task,
      agent: agentsById.get(task.agentId),
      index,
      isStart: !incomingTaskIds.has(task.id),
    },
  }));

  return { nodes, edges };
}

function graphEdges(tasks: FlowTask[]): Edge[] {
  const taskIds = new Set(tasks.map((task) => task.id));
  const hasExplicitNeeds = tasks.some((task) => task.hasExplicitNeeds);

  if (!hasExplicitNeeds) {
    return tasks.slice(1).map((task, index) => workflowEdge(tasks[index].id, task.id, task.state));
  }

  return tasks.flatMap((task) =>
    task.needs
      .filter((need) => taskIds.has(need))
      .map((need) => workflowEdge(need, task.id, task.state)),
  );
}

function workflowEdge(source: string, target: string, targetState: FlowState): Edge {
  const settled = targetState === "completed" || targetState === "failed";

  return {
    id: edgeId(source, target),
    source,
    target,
    type: "default",
    animated: !settled,
    style: {
      stroke: workflowEdgeStroke(targetState),
      strokeWidth: 1.5,
    },
  };
}

function edgeId(source: string, target: string) {
  return `edge:${source.length}:${source}:${target.length}:${target}`;
}

function taskLayout(tasks: FlowTask[]) {
  const layers = taskLayers(tasks);
  const grouped = new Map<number, FlowTask[]>();
  for (const task of tasks) {
    const layer = layers.get(task.id) ?? 0;
    grouped.set(layer, [...(grouped.get(layer) ?? []), task]);
  }

  const positions = new Map<string, { x: number; y: number }>();
  const xGap = 380;
  const yGap = 188;

  for (const [layer, layerTasks] of grouped) {
    const totalHeight = (layerTasks.length - 1) * yGap;
    layerTasks.forEach((task, row) => {
      positions.set(task.id, {
        x: layer * xGap,
        y: row * yGap - totalHeight / 2,
      });
    });
  }

  return positions;
}

function taskLayers(tasks: FlowTask[]) {
  const taskIds = new Set(tasks.map((task) => task.id));
  const hasExplicitNeeds = tasks.some((task) => task.hasExplicitNeeds);
  if (!hasExplicitNeeds) return new Map(tasks.map((task, index) => [task.id, index]));

  const layers = new Map<string, number>();
  const pending = new Set(tasks.map((task) => task.id));
  const taskById = new Map(tasks.map((task) => [task.id, task]));

  for (let pass = 0; pass < tasks.length && pending.size; pass += 1) {
    for (const id of [...pending]) {
      const task = taskById.get(id);
      if (!task) continue;

      const upstream = task.needs.filter((need) => taskIds.has(need));
      if (upstream.some((need) => !layers.has(need))) continue;

      const layer = upstream.length
        ? Math.max(...upstream.map((need) => layers.get(need) ?? 0)) + 1
        : 0;
      layers.set(id, layer);
      pending.delete(id);
    }
  }

  for (const id of pending) {
    layers.set(id, layers.size);
  }

  return layers;
}

function StateIcon({ state }: { state: WorkflowState | FlowState }) {
  if (state === "completed") return <CheckCircle2 className="size-3.5 shrink-0" />;
  if (state === "failed") return <XCircle className="size-3.5 shrink-0" />;
  if (state === "waiting" || state === "idle") return <CircleDashed className="size-3.5 shrink-0" />;
  return <Loader2 className="size-3.5 shrink-0 animate-spin" />;
}

function workflowEdgeStroke(state: FlowState) {
  if (state === "failed") return "var(--destructive)";
  if (state === "running") return "var(--brand-500)";
  if (state === "completed") return "var(--online-solid)";
  return "color-mix(in srgb, var(--muted-foreground) 50%, transparent)";
}

function statusBadgeClass(state: WorkflowState | FlowState) {
  if (state === "completed") return "border-online-solid/25 bg-online-bg text-online-solid";
  if (state === "failed") return "border-destructive/25 bg-destructive/10 text-destructive";
  if (state === "running") return "border-brand-500/25 bg-brand-500/10 text-brand-500";
  return "border-border bg-muted/50 text-muted-foreground";
}

function miniMapNodeColor(node: Node) {
  const state = (node.data as FlowNodeData | undefined)?.task?.state;
  if (!state) return "var(--muted-foreground)";
  if (state === "failed") return "var(--destructive)";
  if (state === "running") return "var(--brand-500)";
  if (state === "completed") return "var(--online-solid)";
  return "color-mix(in srgb, var(--muted-foreground) 40%, transparent)";
}
