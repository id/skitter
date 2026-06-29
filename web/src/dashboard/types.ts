export type ConnectionState = "disconnected" | "connecting" | "connected" | "error";
export type ReplyRole = "user" | "agent";
export type DashboardLanguage = "en" | "zh";
export type ThemeMode = "system" | "light" | "dark";
export type ResolvedTheme = "light" | "dark";

export interface DashboardConfig {
  brokerUrl: string;
  org: string;
  unit: string;
  username: string;
  password: string;
}

export interface AgentCard {
  name?: string;
  description?: string;
  version?: string;
  capabilities?: {
    extensions?: Array<{ uri?: string; description?: string; params?: Record<string, unknown> }>;
  };
}

export interface AgentEntry {
  id: string;
  card: AgentCard;
  status: "online" | "offline" | "unknown";
  isApp: boolean;
  isTaskAgent: boolean;
  lastSeenAt?: number;
  isMock?: boolean;
}

export interface WorkflowTaskDefinition {
  id: string;
  agentId: string;
  description: string;
  agentName?: string;
  needs: string[];
  hasExplicitNeeds: boolean;
  terminal: boolean;
}

export interface DeviceOption {
  deviceId: string;
  name: string;
  online?: boolean;
  state?: Record<string, unknown>;
}

export interface DeviceBindingRequest {
  appId: string;
  workflowName: string;
  prompt: string;
  tasks: WorkflowTaskDefinition[];
}

export interface StaticWorkflowBindingStep {
  taskId: string;
  agentId: string;
  agentName: string;
  description: string;
  devices: DeviceOption[];
  selectedDeviceId: string;
  loading: boolean;
  error?: string;
}

export interface StaticWorkflowBinding {
  workflowId: string;
  workflowName: string;
  steps: StaticWorkflowBindingStep[];
}

export interface AgentDeviceBinding {
  agentId: string;
  agentName: string;
  devices: DeviceOption[];
  selectedDeviceId: string;
  loading: boolean;
  error?: string;
}

export interface ChatMessage {
  id: string;
  role: ReplyRole;
  text: string;
  streaming?: boolean;
  state?: string;
}

export type WorkflowState = "idle" | "running" | "completed" | "failed";

export type RuntimeProgressKind = "workflow_request" | "workflow_create" | "workflow_run";
export type RuntimeProgressPhase =
  | "submitted"
  | "building"
  | "preparing_run"
  | "registering"
  | "completed"
  | "failed";

export interface RuntimeProgress {
  kind: RuntimeProgressKind;
  phase: RuntimeProgressPhase;
  detail?: string;
  startedAt: string;
  completedAt?: string;
}

export interface WorkflowTraceStep {
  id: string;
  sessionId: string;
  agentId: string;
  agentName?: string;
  description: string;
  state: "waiting" | "running" | "completed" | "failed";
  result?: string;
  error?: string;
  timestamp?: string;
}

export interface WorkflowSession {
  sessionId: string;
  appId: string;
  contextId: string;
  state: WorkflowState;
  startedAt?: string;
  completedAt?: string;
}

export interface ChatSession {
  id: string;
  targetId: string;
  targetName: string;
  contextId: string;
  requestTaskId?: string;
  runtimeProgress?: RuntimeProgress;
  workflow?: WorkflowSession;
  traceSteps: WorkflowTraceStep[];
  messages: ChatMessage[];
  streaming: boolean;
}
