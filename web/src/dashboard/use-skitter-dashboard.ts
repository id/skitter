import { Buffer } from "buffer";
import type { IClientOptions, IPublishPacket, MqttClient } from "mqtt";
import mqtt from "mqtt";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  buildA2ARequest,
  decodeCorrelation,
  discoveryTopic,
  eventTopic,
  extractPartsText,
  getUserProperty,
  isAppCard,
  isTaskAgentCard,
  parseMaybeJson,
  parseStoredConfig,
  replyTopic,
  requestTopic,
  terminalStates,
  topicPrefix,
  uuid,
} from "./a2a";
import { displayName } from "./agent-utils";
import {
  bindingPrompt,
  fetchAgentDevices,
  parseDeviceBindingRequest,
} from "./device-binding";
import type {
  AgentCard,
  AgentDeviceBinding,
  AgentEntry,
  ChatMessage,
  ChatSession,
  ConnectionState,
  DashboardConfig,
  DashboardLanguage,
  DeviceBindingRequest,
  DeviceOption,
  ResolvedTheme,
  RuntimeProgress,
  RuntimeProgressKind,
  RuntimeProgressPhase,
  StaticWorkflowBinding,
  ThemeMode,
  WorkflowState,
  WorkflowTraceStep,
} from "./types";
import { bindableWorkflowTasks } from "./workflow-model";

interface PendingChat {
  type: "chat";
  sessionId: string;
  agentMessageId: string;
  taskId: string;
  artifact: string;
  timer: number;
}

interface PendingRuntime {
  type: "runtime";
  artifact: string;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: number;
}

type PendingRequest = PendingChat | PendingRuntime;

interface ConfirmedDeviceBinding {
  description: string;
  agentName: string;
  device: DeviceOption;
}

interface BindingRetryTarget {
  sessionId: string;
  agentMessageId: string;
}

interface MockTaskAgentDraft {
  name: string;
  description: string;
}

type DashboardView = "workflows" | "scenes";
const CHAT_REQUEST_TIMEOUT_MS = 120000;
const LANGUAGE_STORAGE_KEY = "skitter.dashboard.language";
const THEME_STORAGE_KEY = "skitter.dashboard.theme";
const MOCK_TASK_AGENTS_KEY = "skitter.dashboard.mockTaskAgents.v2";

function bindingStorageKey(config: DashboardConfig) {
  return `skitter.dashboard.workflowBindings.${config.org}.${config.unit}`;
}

function agentBindingStorageKey(config: DashboardConfig) {
  return `skitter.dashboard.agentDeviceBindings.${config.org}.${config.unit}`;
}

function getSystemTheme(): ResolvedTheme {
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function getStoredThemeMode(): ThemeMode {
  const stored = localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === "system" || stored === "light" || stored === "dark") return stored;
  return "system";
}

function browserLanguage(): DashboardLanguage {
  return navigator.language.toLowerCase().startsWith("zh") ? "zh" : "en";
}

function getStoredLanguage(): DashboardLanguage {
  const stored = localStorage.getItem(LANGUAGE_STORAGE_KEY);
  if (stored === "en" || stored === "zh") return stored;

  try {
    const raw = localStorage.getItem("skitter.dashboard.config");
    const legacyConfig = raw ? (JSON.parse(raw) as { language?: unknown }) : {};
    if (legacyConfig.language === "en" || legacyConfig.language === "zh") {
      return legacyConfig.language;
    }
  } catch {
    return browserLanguage();
  }

  return browserLanguage();
}

function loadSelectedDeviceIds(config: DashboardConfig) {
  try {
    const raw = localStorage.getItem(bindingStorageKey(config));
    if (!raw) return new Map<string, Record<string, string>>();
    const parsed = JSON.parse(raw) as Record<string, Record<string, string>>;
    return new Map(Object.entries(parsed));
  } catch {
    return new Map<string, Record<string, string>>();
  }
}

function saveSelectedDeviceIds(config: DashboardConfig, bindings: Map<string, Record<string, string>>) {
  localStorage.setItem(bindingStorageKey(config), JSON.stringify(Object.fromEntries(bindings)));
}

function loadSelectedAgentDeviceIds(config: DashboardConfig) {
  try {
    const raw = localStorage.getItem(agentBindingStorageKey(config));
    if (!raw) return new Map<string, string>();
    const parsed = JSON.parse(raw) as Record<string, string>;
    return new Map(Object.entries(parsed));
  } catch {
    return new Map<string, string>();
  }
}

function saveSelectedAgentDeviceIds(config: DashboardConfig, bindings: Map<string, string>) {
  localStorage.setItem(agentBindingStorageKey(config), JSON.stringify(Object.fromEntries(bindings)));
}

function loadMockTaskAgents(): AgentEntry[] {
  try {
    const raw = localStorage.getItem(MOCK_TASK_AGENTS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as AgentEntry[];
    return parsed.filter((agent) => agent?.id && agent?.card?.name);
  } catch {
    return [];
  }
}

function saveMockTaskAgents(agents: AgentEntry[]) {
  localStorage.setItem(MOCK_TASK_AGENTS_KEY, JSON.stringify(agents));
}

function slugifyMockAgentName(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function uniqueMockAgentId(base: string, taken: Set<string>) {
  const prefix = base || "task-agent";
  let candidate = prefix;
  let index = 2;
  while (taken.has(candidate)) {
    candidate = `${prefix}-${index}`;
    index += 1;
  }
  return candidate;
}

function buildMockTaskAgent(id: string, draft: MockTaskAgentDraft): AgentEntry {
  return {
    id,
    card: {
      name: draft.name,
      description: draft.description,
      capabilities: {
        extensions: [
          {
            uri: "urn:skitter:task-agent",
            description: "Dashboard-created task agent preview",
            params: { kind: "mock-task-agent" },
          },
        ],
      },
    },
    status: "online",
    isApp: false,
    isTaskAgent: true,
    isMock: true,
  };
}

function confirmedBindings(binding: StaticWorkflowBinding | undefined): ConfirmedDeviceBinding[] | null {
  if (!binding || binding.steps.length === 0) return null;
  const selected = binding.steps.map((step) => {
    const device = step.devices.find((item) => item.deviceId === step.selectedDeviceId);
    if (!device) return null;
    return {
      description: step.description,
      agentName: step.agentName,
      device,
    };
  });
  if (selected.some((item) => !item)) return null;
  return selected as ConfirmedDeviceBinding[];
}

function resultError(result: unknown) {
  if (!result || typeof result !== "object" || !("error" in result)) return "";
  const error = (result as { error?: unknown }).error;
  return typeof error === "string" ? error : "";
}

function selectedDeviceForAgent(
  agentId: string,
  workflowBindings: Map<string, StaticWorkflowBinding>,
) {
  for (const binding of workflowBindings.values()) {
    const step = binding.steps.find((item) => item.agentId === agentId && item.selectedDeviceId);
    if (step) return step.selectedDeviceId;
  }
  return "";
}

function initialRuntimeProgress(kind: RuntimeProgressKind): RuntimeProgress {
  return {
    kind,
    phase: "submitted",
    startedAt: new Date().toISOString(),
  };
}

function progressKindFromText(text: string): RuntimeProgressKind | undefined {
  if (/preparing to run/i.test(text)) return "workflow_run";
  if (/created workflow/i.test(text)) return "workflow_create";
  if (/registering workflow/i.test(text)) return "workflow_create";
  return undefined;
}

function progressPhaseFromStatus(
  state: string,
  text: string,
): RuntimeProgressPhase | undefined {
  if (state === "TASK_STATE_COMPLETED") return "completed";
  if (terminalStates.has(state)) return "failed";
  if (/registering workflow/i.test(text)) return "registering";
  if (/preparing to run/i.test(text)) return "preparing_run";
  if (/planning workflow request/i.test(text)) return "building";
  if (state === "TASK_STATE_SUBMITTED" || state === "submitted") return "submitted";
  return undefined;
}

function matchingDeviceId(devices: DeviceOption[], ...deviceIds: Array<string | undefined>) {
  for (const deviceId of deviceIds) {
    if (deviceId && devices.some((device) => device.deviceId === deviceId)) return deviceId;
  }
  return "";
}

function appBasePath() {
  const base = import.meta.env.BASE_URL.replace(/\/+$/, "");
  return base === "" || base === "/" ? "" : base;
}

function stripAppBase(pathname: string) {
  const base = appBasePath();
  if (!base) return pathname;
  if (pathname === base) return "/";
  if (pathname.startsWith(`${base}/`)) return pathname.slice(base.length) || "/";
  return pathname;
}

function withAppBase(pathname: string) {
  return `${appBasePath()}${pathname}`;
}

function readViewFromUrl(): DashboardView {
  if (typeof window === "undefined") return "workflows";
  const path = stripAppBase(window.location.pathname).replace(/\/+$/, "");
  if (path === "/scenes" || path.startsWith("/scenes/")) return "scenes";
  return "workflows";
}

function readSceneFromUrl() {
  if (typeof window === "undefined") return "";
  const match = stripAppBase(window.location.pathname).match(/^\/scenes\/([^/]+)$/);
  return match?.[1] ? decodeURIComponent(match[1]) : "";
}

function writeViewToUrl(
  view: DashboardView,
  targetId = "",
  replace = false,
) {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  const path =
    view === "workflows"
      ? "/workflows"
      : targetId
        ? `/scenes/${encodeURIComponent(targetId)}`
        : "/scenes";
  url.pathname = withAppBase(path);
  url.search = "";

  const next = `${url.pathname}${url.search}${url.hash}`;
  const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (next === current) return;

  if (replace) window.history.replaceState(null, "", next);
  else window.history.pushState(null, "", next);
}

export function useSkitterDashboard() {
  const { i18n, t } = useTranslation();
  const clientId = useMemo(() => `dash-${uuid().slice(0, 8)}`, []);
  const clientRef = useRef<MqttClient | null>(null);
  const pendingRef = useRef<Map<string, PendingRequest>>(new Map());
  const selectedDeviceIdsRef = useRef<Map<string, Record<string, string>>>(new Map());
  const selectedAgentDeviceIdsRef = useRef<Map<string, string>>(new Map());
  const workflowBindingsRef = useRef<Map<string, StaticWorkflowBinding>>(new Map());
  const loadedAgentDevicesRef = useRef<Set<string>>(new Set());
  const bindingRetryRef = useRef<BindingRetryTarget | null>(null);

  const [config, setConfig] = useState(() => parseStoredConfig());
  const [draftConfig, setDraftConfig] = useState(() => parseStoredConfig());
  const [language, setLanguage] = useState<DashboardLanguage>(getStoredLanguage);
  const [theme, setTheme] = useState<ThemeMode>(getStoredThemeMode);
  const [systemTheme, setSystemTheme] = useState<ResolvedTheme>(getSystemTheme);
  const [connection, setConnection] = useState<ConnectionState>("disconnected");
  const [connectionError, setConnectionError] = useState("");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [agents, setAgents] = useState<Map<string, AgentEntry>>(new Map());
  const [mockTaskAgents, setMockTaskAgents] = useState<AgentEntry[]>(loadMockTaskAgents);
  const [chatSessions, setChatSessions] = useState<Map<string, ChatSession>>(new Map());
  const [activeChatId, setActiveChatId] = useState("");
  const [activeView, setActiveView] = useState<DashboardView>(() => readViewFromUrl());
  const [activeSceneId, setActiveSceneId] = useState(() => readSceneFromUrl());
  const [chatInput, setChatInput] = useState("");
  const [agentDeviceBindings, setAgentDeviceBindings] = useState<Map<string, AgentDeviceBinding>>(
    new Map(),
  );
  const [queuedBindingRequest, setQueuedBindingRequest] = useState<DeviceBindingRequest | null>(
    null,
  );

  const agentList = useMemo(
    () => {
      const merged = new Map(agents);
      for (const agent of mockTaskAgents) {
        if (!merged.has(agent.id)) merged.set(agent.id, agent);
      }

      return [...merged.values()].sort((a, b) => {
        if (a.isApp !== b.isApp) return a.isApp ? -1 : 1;
        if (a.id === "skitter") return -1;
        if (b.id === "skitter") return 1;
        if (a.isMock !== b.isMock) return a.isMock ? 1 : -1;
        return displayName(a).localeCompare(displayName(b));
      });
    },
    [agents, mockTaskAgents],
  );

  const skitterAgent = useMemo<AgentEntry>(
    () =>
      agents.get("skitter") ?? {
        id: "skitter",
        card: {
          name: "Skitter",
          description: t("fallback.skitterDescription"),
        },
        status: connection === "connected" ? "online" : "unknown",
        isApp: false,
        isTaskAgent: false,
      },
    [agents, connection, t],
  );

  const appAgents = useMemo(() => agentList.filter((agent) => agent.isApp), [agentList]);
  const taskAgents = useMemo(
    () => agentList.filter((agent) => agent.isTaskAgent && agent.status === "online"),
    [agentList],
  );

  const callableAgents = useMemo(
    () =>
      agentList.filter(
        (agent) => !agent.isApp && !agent.isTaskAgent && agent.id !== "skitter",
      ),
    [agentList],
  );

  const selectedAgent = useMemo<AgentEntry>(() => {
    if (activeView === "workflows") return skitterAgent;
    if (!activeSceneId) {
      return {
        id: "",
        card: {
          name: t("fallback.scenesName"),
          description: t("fallback.scenesDescription"),
        },
        status: "unknown",
        isApp: false,
        isTaskAgent: true,
      };
    }

    return (
      agents.get(activeSceneId) ??
      mockTaskAgents.find((agent) => agent.id === activeSceneId) ?? {
        id: activeSceneId,
        card: {
          name: activeSceneId,
          description: t("fallback.waitingSceneDiscovery"),
        },
        status: "unknown",
        isApp: false,
        isTaskAgent: true,
      }
    );
  }, [activeSceneId, activeView, agents, mockTaskAgents, skitterAgent, t]);

  const activeChat = activeChatId ? chatSessions.get(activeChatId) ?? null : null;
  const resolvedTheme: ResolvedTheme = theme === "system" ? systemTheme : theme;

  useEffect(() => {
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    const handleThemeChange = () => {
      setSystemTheme(mediaQuery.matches ? "dark" : "light");
    };

    mediaQuery.addEventListener("change", handleThemeChange);
    return () => mediaQuery.removeEventListener("change", handleThemeChange);
  }, []);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", resolvedTheme === "dark");
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [resolvedTheme, theme]);

  useEffect(() => {
    localStorage.setItem(LANGUAGE_STORAGE_KEY, language);
    if (i18n.language !== language) void i18n.changeLanguage(language);
  }, [i18n, language]);

  useEffect(() => {
    if (activeView !== "scenes") return;
    const nextTaskAgent =
      taskAgents.find((agent) => agent.id === activeSceneId) ?? taskAgents[0];
    if (!nextTaskAgent) {
      setActiveSceneId("");
      setActiveChatId("");
      return;
    }
    if (nextTaskAgent.id === activeSceneId) return;

    setActiveSceneId(nextTaskAgent.id);
    const session = [...chatSessions.values()].find(
      (item) => item.targetId === nextTaskAgent.id,
    );
    setActiveChatId(session?.id ?? "");
  }, [activeSceneId, activeView, chatSessions, taskAgents]);

  useEffect(() => {
    writeViewToUrl(activeView, activeSceneId, true);
  }, [activeSceneId, activeView]);

  useEffect(() => {
    function handlePopState() {
      const view = readViewFromUrl();
      const sceneId = readSceneFromUrl();
      const targetId = view === "workflows" ? "skitter" : sceneId;
      setActiveView(view);
      setActiveSceneId(sceneId);
      const existing = [...chatSessions.values()].find((session) => session.targetId === targetId);
      setActiveChatId(existing?.id ?? "");
    }

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, [chatSessions]);

  useEffect(() => {
    selectedDeviceIdsRef.current = loadSelectedDeviceIds(config);
    selectedAgentDeviceIdsRef.current = loadSelectedAgentDeviceIds(config);
    workflowBindingsRef.current = new Map();
    loadedAgentDevicesRef.current.clear();
    setAgentDeviceBindings(new Map());
  }, [config]);

  const updateChatMessage = useCallback(
    (sessionId: string, messageId: string, updater: (message: ChatMessage) => ChatMessage) => {
      setChatSessions((prev) => {
        const session = prev.get(sessionId);
        if (!session) return prev;

        const next = new Map(prev);
        next.set(sessionId, {
          ...session,
          messages: session.messages.map((message) =>
            message.id === messageId ? updater(message) : message,
          ),
        });
        return next;
      });
    },
    [],
  );

  const finishChat = useCallback((sessionId: string, messageId: string, failed: boolean) => {
    setChatSessions((prev) => {
      const session = prev.get(sessionId);
      if (!session) return prev;

      const next = new Map(prev);
      next.set(sessionId, {
        ...session,
        streaming: false,
        messages: session.messages.map((message) =>
          message.id === messageId
            ? { ...message, streaming: false, state: failed ? "failed" : "completed" }
            : message,
        ),
      });
      return next;
    });
  }, []);

  const updateRuntimeProgress = useCallback(
    (
      sessionId: string,
      updater: (progress: RuntimeProgress) => RuntimeProgress,
    ) => {
      setChatSessions((prev) => {
        const session = prev.get(sessionId);
        if (!session?.runtimeProgress) return prev;

        const next = new Map(prev);
        next.set(sessionId, {
          ...session,
          runtimeProgress: updater(session.runtimeProgress),
        });
        return next;
      });
    },
    [],
  );

  const handleEvent = useCallback((payloadText: string) => {
    let payload: {
      event?: string;
      session_id?: string;
      task_id?: string;
      timestamp?: string;
      data?: {
        app_id?: string;
        context_id?: string;
        request_task_id?: string;
        node_id?: string;
        agent?: string;
        description?: string;
        result?: string;
        error?: string;
      };
    };

    try {
      payload = JSON.parse(payloadText);
    } catch {
      return;
    }

    const requestTaskId = payload.data?.request_task_id;
    if (!requestTaskId) return;

    setChatSessions((prev) => {
      const session = [...prev.values()].find((item) => item.requestTaskId === requestTaskId);
      if (!session) return prev;

      const next = new Map(prev);
      const event = payload.event ?? "";
      const workflowState: WorkflowState =
        event === "session_completed"
          ? "completed"
          : event === "session_failed"
            ? "failed"
            : "running";
      const workflow =
        payload.session_id || payload.data?.app_id
          ? {
              sessionId: payload.session_id ?? session.workflow?.sessionId ?? "",
              appId: payload.data?.app_id ?? session.workflow?.appId ?? "",
              contextId: payload.data?.context_id ?? session.workflow?.contextId ?? session.contextId,
              state: workflowState,
              startedAt: session.workflow?.startedAt ?? payload.timestamp,
              completedAt:
                event === "session_completed" || event === "session_failed"
                  ? payload.timestamp
                  : session.workflow?.completedAt,
            }
          : session.workflow;

      let traceSteps = session.traceSteps;
      if (event === "task_started" || event === "task_completed" || event === "task_failed") {
        const stepId = payload.task_id || payload.data?.node_id || "task";
        const state =
          event === "task_completed" ? "completed" : event === "task_failed" ? "failed" : "running";
        const nextStep: WorkflowTraceStep = {
          id: stepId,
          sessionId: payload.session_id ?? "",
          agentId: payload.data?.agent ?? "",
          description: payload.data?.description ?? stepId,
          state,
          result: payload.data?.result,
          error: payload.data?.error,
          timestamp: payload.timestamp,
        };

        const existing = traceSteps.findIndex((step) => step.id === stepId);
        traceSteps =
          existing >= 0
            ? traceSteps.map((step, index) => (index === existing ? { ...step, ...nextStep } : step))
            : [...traceSteps, nextStep];
      }

      next.set(session.id, {
        ...session,
        runtimeProgress: workflow ? undefined : session.runtimeProgress,
        workflow,
        traceSteps,
      });
      return next;
    });
  }, []);

  const handleReply = useCallback(
    (payloadText: string, packet: IPublishPacket) => {
      const correlation = decodeCorrelation(packet);
      if (!correlation) return;

      const pending = pendingRef.current.get(correlation);
      if (!pending) return;

      let payload: {
        error?: { message?: string };
        result?: {
          statusUpdate?: {
            status?: { state?: string; message?: { parts?: Array<{ text?: string }> } };
            metadata?: {
              type?: string;
              agent?: string;
              description?: string;
              result?: string;
              error?: string;
            };
          };
          artifactUpdate?: {
            append?: boolean;
            artifact?: { parts?: Array<{ text?: string }> };
          };
        };
      };

      try {
        payload = JSON.parse(payloadText);
      } catch {
        return;
      }

      if (payload.error) {
        const message = payload.error.message ?? t("status.a2aFailed");
        if (pending.type === "chat") {
          window.clearTimeout(pending.timer);
          updateRuntimeProgress(pending.sessionId, (progress) => ({
            ...progress,
            phase: "failed",
            detail: message,
            completedAt: new Date().toISOString(),
          }));
          updateChatMessage(pending.sessionId, pending.agentMessageId, (item) => ({
            ...item,
            text: message,
            streaming: false,
            state: "failed",
          }));
          finishChat(pending.sessionId, pending.agentMessageId, true);
        } else {
          window.clearTimeout(pending.timer);
          pending.reject(new Error(message));
        }
        pendingRef.current.delete(correlation);
        return;
      }

      const result = payload.result;
      if (!result) return;

      if (result.artifactUpdate) {
        const text = extractPartsText(result.artifactUpdate.artifact?.parts);
        pending.artifact = result.artifactUpdate.append ? `${pending.artifact}${text}` : text;
      }

      if (result.statusUpdate) {
        const state = result.statusUpdate.status?.state ?? "";
        const text = extractPartsText(result.statusUpdate.status?.message?.parts);

        if (pending.type === "chat") {
          const phase = progressPhaseFromStatus(state, text);
          if (phase) {
            updateRuntimeProgress(pending.sessionId, (progress) => ({
              ...progress,
              kind: progressKindFromText(text) ?? progress.kind,
              phase,
              detail: text || progress.detail,
              completedAt: terminalStates.has(state)
                ? new Date().toISOString()
                : progress.completedAt,
            }));
          }

          const metadata = result.statusUpdate.metadata;
          if (metadata?.type === "task_step" && metadata.agent) {
            const stepId = `${metadata.agent}-${Date.now()}`;
            const nextStep: WorkflowTraceStep = {
              id: stepId,
              sessionId: "",
              agentId: metadata.agent,
              description: metadata.description || text || metadata.agent,
              state: metadata.error ? "failed" : "completed",
              result: metadata.result,
              error: metadata.error,
              timestamp: new Date().toISOString(),
            };

            setChatSessions((prev) => {
              const session = prev.get(pending.sessionId);
              if (!session) return prev;
              const next = new Map(prev);
              next.set(pending.sessionId, {
                ...session,
                traceSteps: [...session.traceSteps, nextStep],
              });
              return next;
            });
          }

          if (terminalStates.has(state) && state === "TASK_STATE_COMPLETED") {
            const bindingRequest = parseDeviceBindingRequest(pending.artifact);
            if (bindingRequest) {
              bindingRetryRef.current = {
                sessionId: pending.sessionId,
                agentMessageId: pending.agentMessageId,
              };
              setQueuedBindingRequest(bindingRequest);
              const hasSavedBindings = confirmedBindings(
                workflowBindingsRef.current.get(bindingRequest.appId),
              );
              updateChatMessage(pending.sessionId, pending.agentMessageId, (item) => ({
                ...item,
                text: hasSavedBindings
                  ? t("status.runningWorkflow", { name: bindingRequest.workflowName })
                  : t("status.bindDevices", { name: bindingRequest.workflowName }),
                streaming: false,
                state,
              }));
              updateRuntimeProgress(pending.sessionId, (progress) => ({
                ...progress,
                kind: "workflow_create",
                phase: "completed",
                detail: hasSavedBindings
                  ? t("status.runningWorkflow", { name: bindingRequest.workflowName })
                  : t("status.bindDevices", { name: bindingRequest.workflowName }),
                completedAt: new Date().toISOString(),
              }));
              finishChat(pending.sessionId, pending.agentMessageId, false);
              pendingRef.current.delete(correlation);
              return;
            }
          }

          const isTerminal = terminalStates.has(state);
          const finalText = pending.artifact || text;
          updateChatMessage(pending.sessionId, pending.agentMessageId, (item) => ({
            ...item,
            text: isTerminal ? finalText || item.text : text || item.text,
            streaming: !isTerminal,
            state,
          }));
          if (isTerminal) {
            updateRuntimeProgress(pending.sessionId, (progress) => ({
              ...progress,
              kind: progressKindFromText(finalText || text) ?? progress.kind,
              phase: state === "TASK_STATE_COMPLETED" ? "completed" : "failed",
              detail: finalText || text || progress.detail,
              completedAt: new Date().toISOString(),
            }));
            window.clearTimeout(pending.timer);
            finishChat(pending.sessionId, pending.agentMessageId, state !== "TASK_STATE_COMPLETED");
            pendingRef.current.delete(correlation);
          }
        } else if (terminalStates.has(state)) {
          window.clearTimeout(pending.timer);
          if (state === "TASK_STATE_COMPLETED") {
            pending.resolve(parseMaybeJson(pending.artifact));
          } else {
            pending.reject(new Error(text || state));
          }
          pendingRef.current.delete(correlation);
        }
      }
    },
    [finishChat, t, updateChatMessage, updateRuntimeProgress],
  );

  const handleMessage = useCallback(
    (topic: string, payload: Buffer, packet: IPublishPacket) => {
      const payloadText = payload.toString("utf8");
      const discoveryPrefix = `${topicPrefix()}/discovery/${config.org}/${config.unit}/`;

      if (topic.startsWith(discoveryPrefix)) {
        const id = topic.slice(discoveryPrefix.length);
        if (!payloadText) {
          setAgents((prev) => {
            const next = new Map(prev);
            next.delete(id);
            return next;
          });
          return;
        }

        try {
          const card = JSON.parse(payloadText) as AgentCard;
          const status = getUserProperty(packet, "a2a-status");
          const lastSeenAt = Date.now();
          setAgents((prev) => {
            const next = new Map(prev);
            next.set(id, {
              id,
              card,
              status: status === "offline" ? "offline" : status === "online" ? "online" : "unknown",
              isApp: isAppCard(card),
              isTaskAgent: isTaskAgentCard(card),
              lastSeenAt,
            });
            return next;
          });
        } catch {
          return;
        }
      }

      if (topic.startsWith(`${topicPrefix()}/event/${config.org}/${config.unit}/`)) {
        handleEvent(payloadText);
      }

      if (topic.startsWith(`${topicPrefix()}/reply/${config.org}/${config.unit}/${clientId}/`)) {
        handleReply(payloadText, packet);
      }
    },
    [clientId, config.org, config.unit, handleEvent, handleReply],
  );

  // Route messages through a ref so that callback churn (e.g. the i18n `t`
  // function changing on language switch) never recreates `connect` and tears
  // down the live MQTT connection. Only broker/topic config changes reconnect.
  const handleMessageRef = useRef(handleMessage);
  useEffect(() => {
    handleMessageRef.current = handleMessage;
  }, [handleMessage]);

  const connect = useCallback(() => {
    if (clientRef.current) {
      clientRef.current.end(true);
      clientRef.current = null;
    }

    loadedAgentDevicesRef.current.clear();
    setAgentDeviceBindings(new Map());
    setConnection("connecting");
    setConnectionError("");

    const options: IClientOptions = {
      // Transport binding requires MQTT Client ID as {org_id}/{unit_id}/{agent_id}.
      clientId: `${config.org}/${config.unit}/${clientId}`,
      protocolVersion: 5,
      clean: true,
      reconnectPeriod: 2500,
      connectTimeout: 5000,
    };
    if (config.username) options.username = config.username;
    if (config.password) options.password = config.password;

    const client = mqtt.connect(config.brokerUrl, options);
    clientRef.current = client;

    client.on("connect", () => {
      setConnection("connected");
      const replyBase = replyTopic(config, clientId);
      client.subscribe(discoveryTopic(config), { qos: 1 });
      client.subscribe(eventTopic(config), { qos: 1 });
      client.subscribe(replyBase, { qos: 1 });
      client.subscribe(`${replyBase}/#`, { qos: 1 });
    });
    client.on("close", () => setConnection((current) => (current === "error" ? "error" : "disconnected")));
    client.on("error", (error) => {
      setConnection("error");
      setConnectionError(error.message);
    });
    client.on("message", (topic, payload, packet) =>
      handleMessageRef.current(topic, payload, packet),
    );
  }, [clientId, config]);

  useEffect(() => {
    connect();
    const pendingRequests = pendingRef.current;

    return () => {
      clientRef.current?.end(true);
      clientRef.current = null;
      pendingRequests.forEach((pending) => {
        window.clearTimeout(pending.timer);
      });
      pendingRequests.clear();
    };
  }, [connect]);

  const publishA2A = useCallback(
    (
      targetId: string,
      text: string,
      contextId: string,
      pending: PendingRequest,
      taskId = uuid(),
    ) => {
      const client = clientRef.current;
      if (!client || connection !== "connected") {
        throw new Error(t("status.mqttDisconnected"));
      }

      const requestId = `dash-${uuid().slice(0, 8)}`;
      pendingRef.current.set(requestId, pending);
      client.publish(
        requestTopic(config, targetId),
        buildA2ARequest(text, requestId, contextId, "dashboard", taskId),
        {
          qos: 1,
          properties: {
            responseTopic: `${replyTopic(config, clientId)}/${requestId}`,
            correlationData: Buffer.from(requestId),
          },
        },
      );
      return { requestId, taskId };
    },
    [clientId, config, connection, t],
  );

  const runtimeQuery = useCallback(
    (text: string) =>
      new Promise<unknown>((resolve, reject) => {
        let correlation = "";
        const timer = window.setTimeout(() => {
          if (correlation) pendingRef.current.delete(correlation);
          reject(new Error(t("status.coordinatorTimeout")));
        }, 20000);

        try {
          const request = publishA2A("skitter", text, uuid(), {
            type: "runtime",
            artifact: "",
            resolve,
            reject,
            timer,
          });
          correlation = request.requestId;
        } catch (error) {
          window.clearTimeout(timer);
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      }),
    [publishA2A, t],
  );

  useEffect(() => {
    if (connection !== "connected") return;

    setAgentDeviceBindings((prev) => {
      const next = new Map<string, AgentDeviceBinding>();
      for (const agent of callableAgents) {
        const existing = prev.get(agent.id);
        next.set(agent.id, {
          agentId: agent.id,
          agentName: displayName(agent),
          devices: existing?.devices ?? [],
          selectedDeviceId:
            existing?.selectedDeviceId ||
            selectedAgentDeviceIdsRef.current.get(agent.id) ||
            selectedDeviceForAgent(agent.id, workflowBindingsRef.current),
          loading: existing?.loading ?? true,
          error: existing?.error,
        });
      }
      return next;
    });

    for (const agent of callableAgents) {
      if (loadedAgentDevicesRef.current.has(agent.id)) continue;
      loadedAgentDevicesRef.current.add(agent.id);

      void fetchAgentDevices(agent.id)
        .then((devices) => {
          setAgentDeviceBindings((prev) => {
            const binding = prev.get(agent.id);
            if (!binding) return prev;
            const next = new Map(prev);
            next.set(agent.id, {
              ...binding,
              devices,
              selectedDeviceId: matchingDeviceId(devices, binding.selectedDeviceId),
              loading: false,
              error: devices.length ? undefined : t("status.noDevices"),
            });
            return next;
          });
        })
        .catch((error) => {
          setAgentDeviceBindings((prev) => {
            const binding = prev.get(agent.id);
            if (!binding) return prev;
            const next = new Map(prev);
            next.set(agent.id, {
              ...binding,
              loading: false,
              error: error instanceof Error ? error.message : String(error),
            });
            return next;
          });
        });
    }
  }, [callableAgents, connection, t]);

  useEffect(() => {
    if (connection !== "connected") return;

    const next = new Map<string, StaticWorkflowBinding>();
    for (const app of appAgents) {
      const tasks = bindableWorkflowTasks(app, agents);
      if (!tasks.length) continue;

      const existing = workflowBindingsRef.current.get(app.id);
      const selected = selectedDeviceIdsRef.current.get(app.id) ?? {};
      next.set(app.id, {
        workflowId: app.id,
        workflowName: displayName(app),
        steps: tasks.map((task) => {
          const current = existing?.steps.find((step) => step.taskId === task.id);
          const agent = agents.get(task.agentId);
          const agentBinding = agentDeviceBindings.get(task.agentId);
          const devices = agentBinding?.devices ?? current?.devices ?? [];
          return {
            taskId: task.id,
            agentId: task.agentId,
            agentName: task.agentName || agentBinding?.agentName || (agent ? displayName(agent) : task.agentId),
            description: task.description,
            devices,
            selectedDeviceId: matchingDeviceId(
              devices,
              current?.selectedDeviceId,
              selected[task.id],
              agentBinding?.selectedDeviceId,
              selectedAgentDeviceIdsRef.current.get(task.agentId),
            ),
            loading: agentBinding?.loading ?? current?.loading ?? true,
            error: agentBinding?.error ?? current?.error,
          };
        }),
      });
    }
    workflowBindingsRef.current = next;
  }, [agentDeviceBindings, agents, appAgents, connection]);

  const sendChatMessage = useCallback(
    (wireText: string, visibleText = wireText) => {
      const trimmed = wireText.trim();
      const visible = visibleText.trim();
      if (!trimmed || !visible) return;
      const targetId = selectedAgent.id || (activeView === "workflows" ? "skitter" : "");
      if (!targetId) return;
      const targetName = displayName(selectedAgent) || targetId;
      const existing = [...chatSessions.values()].find((session) => session.targetId === targetId);
      const sessionId = existing?.id ?? uuid();
      const contextId = existing?.contextId ?? uuid();
      const agentMessageId = uuid();
      const taskId = uuid();

      setChatSessions((prev) => {
        const next = new Map(prev);
        const current =
          next.get(sessionId) ??
          ({
            id: sessionId,
            targetId,
            targetName,
            contextId,
            traceSteps: [],
            messages: [],
            streaming: false,
          } satisfies ChatSession);
        next.set(sessionId, {
          ...current,
          requestTaskId: taskId,
          runtimeProgress: targetId === "skitter" ? initialRuntimeProgress("workflow_request") : undefined,
          workflow: undefined,
          traceSteps: [],
          streaming: true,
          messages: [
            ...current.messages,
            { id: uuid(), role: "user", text: visible },
            { id: agentMessageId, role: "agent", text: "", streaming: true, state: "submitted" },
          ],
        });
        return next;
      });
      setActiveChatId(sessionId);

      if (selectedAgent.isMock) {
        window.setTimeout(() => {
          setChatSessions((prev) => {
            const session = prev.get(sessionId);
            if (!session) return prev;

            const next = new Map(prev);
            next.set(sessionId, {
              ...session,
              streaming: false,
              traceSteps: [
                {
                  id: `${taskId}-plan`,
                  sessionId,
                  agentId: targetId,
                  description: t("mockTask.stepPlan"),
                  state: "completed",
                  result: t("mockTask.stepPlanResult"),
                  timestamp: new Date().toISOString(),
                },
                {
                  id: `${taskId}-devices`,
                  sessionId,
                  agentId: targetId,
                  description: t("mockTask.stepDevices"),
                  state: "completed",
                  result: t("mockTask.stepDevicesResult"),
                  timestamp: new Date().toISOString(),
                },
              ],
              messages: session.messages.map((message) =>
                message.id === agentMessageId
                  ? {
                      ...message,
                      text: t("mockTask.mockReply", { name: targetName }),
                      streaming: false,
                      state: "completed",
                    }
                  : message,
              ),
            });
            return next;
          });
        }, 450);
        return;
      }

      let correlation = "";
      const timer = window.setTimeout(() => {
        if (correlation) pendingRef.current.delete(correlation);
        updateChatMessage(sessionId, agentMessageId, (message) => ({
          ...message,
          text: t("status.a2aTimeout"),
          streaming: false,
          state: "failed",
        }));
        finishChat(sessionId, agentMessageId, true);
      }, CHAT_REQUEST_TIMEOUT_MS);

      try {
        const request = publishA2A(
          targetId,
          trimmed,
          contextId,
          {
            type: "chat",
            sessionId,
            agentMessageId,
            taskId,
            artifact: "",
            timer,
          },
          taskId,
        );
        correlation = request.requestId;
      } catch (error) {
        window.clearTimeout(timer);
        updateChatMessage(sessionId, agentMessageId, (message) => ({
          ...message,
          text: error instanceof Error ? error.message : String(error),
          streaming: false,
          state: "failed",
        }));
        finishChat(sessionId, agentMessageId, true);
      }
    },
    [activeView, chatSessions, finishChat, publishA2A, selectedAgent, t, updateChatMessage],
  );

  const createMockTaskAgent = useCallback(
    (draft: MockTaskAgentDraft) => {
      const name = draft.name.trim();
      const description = draft.description.trim();
      if (!name || !description) return;

      const taken = new Set([...agents.keys(), ...mockTaskAgents.map((agent) => agent.id)]);
      const id = uniqueMockAgentId(slugifyMockAgentName(name), taken);
      const agent = buildMockTaskAgent(id, { name, description });

      setMockTaskAgents((current) => {
        const next = [agent, ...current];
        saveMockTaskAgents(next);
        return next;
      });
      setActiveView("scenes");
      setActiveSceneId(id);
      setActiveChatId("");
      writeViewToUrl("scenes", id);
    },
    [agents, mockTaskAgents],
  );

  const deleteMockTaskAgent = useCallback(
    (agentId: string) => {
      setMockTaskAgents((current) => {
        const next = current.filter((agent) => agent.id !== agentId);
        saveMockTaskAgents(next);
        return next;
      });
      setChatSessions((current) => {
        const next = new Map(current);
        for (const [sessionId, session] of next) {
          if (session.targetId === agentId) next.delete(sessionId);
        }
        return next;
      });
      if (activeSceneId === agentId) {
        setActiveSceneId("");
        setActiveChatId("");
        writeViewToUrl("scenes");
      }
    },
    [activeSceneId],
  );

  const retryChatMessage = useCallback(
    (target: BindingRetryTarget | null, wireText: string, statusText: string) => {
      if (!target) {
        sendChatMessage(wireText, statusText);
        return;
      }

      const session = chatSessions.get(target.sessionId);
      if (!session) {
        sendChatMessage(wireText, statusText);
        return;
      }

      const taskId = uuid();
      setChatSessions((prev) => {
        const current = prev.get(target.sessionId);
        if (!current) return prev;

        const next = new Map(prev);
        next.set(target.sessionId, {
          ...current,
          requestTaskId: taskId,
          runtimeProgress: initialRuntimeProgress("workflow_run"),
          workflow: undefined,
          traceSteps: [],
          streaming: true,
          messages: current.messages.map((message) =>
            message.id === target.agentMessageId
              ? { ...message, text: statusText, streaming: true, state: "submitted" }
              : message,
          ),
        });
        return next;
      });
      setActiveChatId(target.sessionId);

      let correlation = "";
      const timer = window.setTimeout(() => {
        if (correlation) pendingRef.current.delete(correlation);
        updateChatMessage(target.sessionId, target.agentMessageId, (message) => ({
          ...message,
          text: t("status.a2aTimeout"),
          streaming: false,
          state: "failed",
        }));
        finishChat(target.sessionId, target.agentMessageId, true);
      }, CHAT_REQUEST_TIMEOUT_MS);

      try {
        const request = publishA2A(
          "skitter",
          wireText,
          session.contextId,
          {
            type: "chat",
            sessionId: target.sessionId,
            agentMessageId: target.agentMessageId,
            taskId,
            artifact: "",
            timer,
          },
          taskId,
        );
        correlation = request.requestId;
      } catch (error) {
        window.clearTimeout(timer);
        updateChatMessage(target.sessionId, target.agentMessageId, (message) => ({
          ...message,
          text: error instanceof Error ? error.message : String(error),
          streaming: false,
          state: "failed",
        }));
        finishChat(target.sessionId, target.agentMessageId, true);
      }
    },
    [chatSessions, finishChat, publishA2A, sendChatMessage, t, updateChatMessage],
  );

  useEffect(() => {
    if (!queuedBindingRequest) return;
    const retryTarget = bindingRetryRef.current;
    bindingRetryRef.current = null;
    const savedBindings = confirmedBindings(workflowBindingsRef.current.get(queuedBindingRequest.appId));
    if (savedBindings?.length) {
      const wireText = bindingPrompt(
        queuedBindingRequest.prompt,
        queuedBindingRequest.workflowName,
        savedBindings,
      );
      setQueuedBindingRequest(null);
      retryChatMessage(
        retryTarget,
        wireText,
        t("status.runningWorkflow", { name: queuedBindingRequest.workflowName }),
      );
      return;
    }

    if (retryTarget) {
      updateChatMessage(retryTarget.sessionId, retryTarget.agentMessageId, (message) => ({
        ...message,
        text: t("status.bindDevices", { name: queuedBindingRequest.workflowName }),
        streaming: false,
        state: "completed",
      }));
    }
    setQueuedBindingRequest(null);
  }, [queuedBindingRequest, retryChatMessage, t, updateChatMessage]);

  const sendChat = useCallback(
    (text: string) => {
      const trimmed = text.trim();
      if (!trimmed) return;

      sendChatMessage(trimmed);
    },
    [sendChatMessage],
  );

  const updateAgentBindingDevice = useCallback(
    (agentId: string, deviceId: string) => {
      setAgentDeviceBindings((prev) => {
        const binding = prev.get(agentId);
        if (!binding) return prev;
        const next = new Map(prev);
        next.set(agentId, { ...binding, selectedDeviceId: deviceId });
        return next;
      });

      const selectedAgents = new Map(selectedAgentDeviceIdsRef.current);
      if (deviceId) selectedAgents.set(agentId, deviceId);
      else selectedAgents.delete(agentId);
      selectedAgentDeviceIdsRef.current = selectedAgents;
      saveSelectedAgentDeviceIds(config, selectedAgents);

      const next = new Map<string, StaticWorkflowBinding>();
      const selectedByWorkflow = new Map(selectedDeviceIdsRef.current);
      let changed = false;

      for (const [workflowId, binding] of workflowBindingsRef.current) {
        let workflowChanged = false;
        const selected = { ...(selectedByWorkflow.get(workflowId) ?? {}) };
        const steps = binding.steps.map((step) => {
          if (step.agentId !== agentId) return step;

          workflowChanged = true;
          changed = true;
          if (deviceId) selected[step.taskId] = deviceId;
          else delete selected[step.taskId];
          return { ...step, selectedDeviceId: deviceId };
        });

        if (workflowChanged) selectedByWorkflow.set(workflowId, selected);
        next.set(workflowId, workflowChanged ? { ...binding, steps } : binding);
      }

      if (!changed) return;
      workflowBindingsRef.current = next;
      selectedDeviceIdsRef.current = selectedByWorkflow;
      saveSelectedDeviceIds(config, selectedByWorkflow);
    },
    [config],
  );

  const deleteWorkflow = useCallback(
    async (workflowId: string) => {
      const result = await runtimeQuery(`delete app ${workflowId}`);
      const error = resultError(result);
      if (error) throw new Error(error);

      const next = new Map(workflowBindingsRef.current);
      next.delete(workflowId);
      workflowBindingsRef.current = next;

      const selected = new Map(selectedDeviceIdsRef.current);
      selected.delete(workflowId);
      selectedDeviceIdsRef.current = selected;
      saveSelectedDeviceIds(config, selected);
    },
    [config, runtimeQuery],
  );

  const selectWorkflows = useCallback(() => {
    setActiveView("workflows");
    setActiveSceneId("");
    const existing = [...chatSessions.values()].find((session) => session.targetId === "skitter");
    setActiveChatId(existing?.id ?? "");
    writeViewToUrl("workflows");
  }, [chatSessions]);

  const selectScenes = useCallback(() => {
    const taskAgentId = activeSceneId || taskAgents[0]?.id || "";
    setActiveView("scenes");
    setActiveSceneId(taskAgentId);
    const existing = [...chatSessions.values()].find(
      (session) => session.targetId === taskAgentId,
    );
    setActiveChatId(existing?.id ?? "");
    writeViewToUrl("scenes", taskAgentId);
  }, [activeSceneId, chatSessions, taskAgents]);

  const selectScene = useCallback(
    (targetId: string) => {
      setActiveView("scenes");
      setActiveSceneId(targetId);
      const existing = [...chatSessions.values()].find((session) => session.targetId === targetId);
      setActiveChatId(existing?.id ?? "");
      writeViewToUrl("scenes", targetId);
    },
    [chatSessions],
  );

  const openSettings = useCallback(
    (open: boolean) => {
      // Start every edit from the active config so a prior Cancel never leaves
      // stale unsaved edits in the draft.
      if (open) setDraftConfig(config);
      setSettingsOpen(open);
    },
    [config],
  );

  const applySettings = useCallback(() => {
    const brokerUrl = draftConfig.brokerUrl.trim();
    if (!/^wss?:\/\/.+/i.test(brokerUrl)) return; // guarded by the Save button too
    const next = { ...draftConfig, brokerUrl };
    localStorage.setItem("skitter.dashboard.config", JSON.stringify(next));
    setAgents(new Map());
    setConfig(next);
    setSettingsOpen(false);
  }, [draftConfig]);

  return {
    activeChat,
    appAgents,
    applySettings,
    agentDeviceBindings,
    callableAgents,
    chatInput,
    clientId,
    config,
    connect,
    connection,
    connectionError,
    draftConfig,
    language,
    taskAgents,
    resolvedTheme,
    selectedAgent,
    activeView,
    activeSceneId,
    sendChat,
    selectWorkflows,
    selectScenes,
    selectScene,
    setChatInput,
    setDraftConfig,
    setLanguage,
    setSettingsOpen: openSettings,
    setTheme,
    updateAgentBindingDevice,
    deleteWorkflow,
    createMockTaskAgent,
    deleteMockTaskAgent,
    settingsOpen,
    theme,
  };
}

export type SkitterDashboardController = ReturnType<typeof useSkitterDashboard>;
