import type { IPublishPacket } from "mqtt";
import type { AgentCard, DashboardConfig } from "./types";

const IP_BROKER_URL = "ws://162.14.117.182:8083/mqtt";
const LEGACY_DOMAIN_BROKER_URL = "wss://emqx-device-agent.cloud/mqtt";
const IP_HOSTNAME = "162.14.117.182";

function sameOriginBrokerUrl() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/mqtt`;
}

function shouldUseSameOriginBroker() {
  if (typeof window === "undefined") return false;
  const { hostname, protocol } = window.location;
  return (
    protocol === "https:" ||
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === IP_HOSTNAME
  );
}

function defaultBrokerUrl() {
  return shouldUseSameOriginBroker() ? sameOriginBrokerUrl() : IP_BROKER_URL;
}

export const DEFAULT_CONFIG: DashboardConfig = {
  brokerUrl: defaultBrokerUrl(),
  org: "default",
  unit: "default",
  username: "",
  password: "",
};

function normalizeBrokerUrl(value: unknown) {
  if (typeof value !== "string") return DEFAULT_CONFIG.brokerUrl;
  const brokerUrl = value.trim();
  if (!brokerUrl) return DEFAULT_CONFIG.brokerUrl;
  if (shouldUseSameOriginBroker() && brokerUrl === IP_BROKER_URL) return DEFAULT_CONFIG.brokerUrl;
  if (typeof window !== "undefined" && window.location.protocol === "https:" && brokerUrl.startsWith("ws://")) {
    return DEFAULT_CONFIG.brokerUrl;
  }
  if (brokerUrl === LEGACY_DOMAIN_BROKER_URL || brokerUrl.startsWith(`${LEGACY_DOMAIN_BROKER_URL}?`)) {
    return DEFAULT_CONFIG.brokerUrl;
  }
  return brokerUrl;
}

export const terminalStates = new Set([
  "TASK_STATE_COMPLETED",
  "TASK_STATE_FAILED",
  "TASK_STATE_CANCELED",
  "TASK_STATE_REJECTED",
  "TASK_STATE_INPUT_REQUIRED",
  "TASK_STATE_AUTH_REQUIRED",
]);

export function topicPrefix() {
  return "$a2a/v1";
}

export function discoveryTopic(config: DashboardConfig) {
  return `${topicPrefix()}/discovery/${config.org}/${config.unit}/+`;
}

export function requestTopic(config: DashboardConfig, agentId: string) {
  return `${topicPrefix()}/request/${config.org}/${config.unit}/${agentId}`;
}

export function replyTopic(config: DashboardConfig, clientId: string) {
  // clientId is the requester's own agent_id segment; callers append a
  // per-request reply_suffix → $a2a/v1/reply/{org}/{unit}/{agent_id}/{suffix}.
  return `${topicPrefix()}/reply/${config.org}/${config.unit}/${clientId}`;
}

export function eventTopic(config: DashboardConfig) {
  return `${topicPrefix()}/event/${config.org}/${config.unit}/+`;
}

export function uuid() {
  const cryptoApi = globalThis.crypto;
  if (typeof cryptoApi?.randomUUID === "function") return cryptoApi.randomUUID();

  if (typeof cryptoApi?.getRandomValues === "function") {
    const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
    return [
      hex.slice(0, 4).join(""),
      hex.slice(4, 6).join(""),
      hex.slice(6, 8).join(""),
      hex.slice(8, 10).join(""),
      hex.slice(10, 16).join(""),
    ].join("-");
  }

  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (char) => {
    const random = Math.floor(Math.random() * 16);
    const value = char === "x" ? random : (random & 0x3) | 0x8;
    return value.toString(16);
  });
}

export function parseStoredConfig(): DashboardConfig {
  try {
    const raw = localStorage.getItem("skitter.dashboard.config");
    const stored = raw
      ? (JSON.parse(raw) as Partial<DashboardConfig> & { language?: unknown })
      : {};
    delete stored.language;
    const config = { ...DEFAULT_CONFIG, ...stored };
    return { ...config, brokerUrl: normalizeBrokerUrl(config.brokerUrl) };
  } catch {
    return { ...DEFAULT_CONFIG };
  }
}

export function isAppCard(card: AgentCard) {
  return Boolean(
    card.capabilities?.extensions?.some(
      (extension) => extension.uri === "urn:skitter:app" && extension.params?.tasks,
    ),
  );
}

export function isTaskAgentCard(card: AgentCard) {
  return Boolean(
    card.capabilities?.extensions?.some((extension) => extension.uri === "urn:skitter:task-agent"),
  );
}

export function decodeCorrelation(packet: IPublishPacket) {
  const value = packet.properties?.correlationData;
  if (!value) return "";
  if (typeof value === "string") return value;
  if (value instanceof Uint8Array) return new TextDecoder().decode(value);
  return String(value);
}

export function getUserProperty(packet: IPublishPacket, key: string) {
  const props = packet.properties?.userProperties;
  if (!props) return "";
  if (Array.isArray(props)) {
    const found = props.find((item) => Array.isArray(item) && item[0] === key);
    return found ? String(found[1]) : "";
  }
  const value = (props as Record<string, string | string[] | undefined>)[key];
  return Array.isArray(value) ? value[0] : value ?? "";
}

export function buildA2ARequest(
  text: string,
  requestId: string,
  contextId: string,
  sender = "dashboard",
  taskId = uuid(),
) {
  return JSON.stringify({
    jsonrpc: "2.0",
    id: requestId,
    method: "SendMessage",
    params: {
      message: {
        messageId: uuid().replaceAll("-", ""),
        role: "ROLE_USER",
        parts: [{ text }],
        taskId,
        contextId,
      },
      metadata: { sender },
    },
  });
}

export function extractPartsText(parts: Array<{ text?: string }> | undefined) {
  return parts?.map((part) => part.text ?? "").join("") ?? "";
}

export function parseMaybeJson(text: string) {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}
