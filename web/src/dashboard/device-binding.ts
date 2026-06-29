import type {
  DeviceBindingRequest,
  DeviceOption,
} from "./types";

export function parseDeviceBindingRequest(value: unknown): DeviceBindingRequest | null {
  const parsed = typeof value === "string" ? parseJsonFragment(value) : value;
  if (!isRecord(parsed) || !isRecord(parsed.device_binding_required)) return null;

  const data = parsed.device_binding_required;
  const appId = String(data.app_id ?? "").trim();
  const workflowName = String(data.workflow_name ?? "").trim();
  const prompt = String(data.prompt ?? "").trim();
  const rawTasks = Array.isArray(data.tasks) ? data.tasks : [];
  const tasks = rawTasks
    .filter(isRecord)
    .map((task) => ({
      id: String(task.id ?? "").trim(),
      agentId: String(task.agent_id ?? task.agent ?? "").trim(),
      agentName: String(task.agent_name ?? "").trim() || undefined,
      description: String(task.description ?? "").trim(),
      needs: [],
      hasExplicitNeeds: false,
      terminal: false,
    }))
    .filter((task) => task.id && task.agentId);

  if (!appId || !workflowName || !prompt || tasks.length === 0) return null;
  return { appId, workflowName, prompt, tasks };
}

export function parseDeviceOptions(value: unknown): DeviceOption[] {
  const parsed = typeof value === "string" ? parseJsonFragment(value) : value;
  return rawDeviceList(parsed)
    .filter(isRecord)
    .map(parseDeviceOption)
    .filter(isDeviceOption);
}

export function productIdFromAgentId(agentId: string) {
  const productSegment = agentId.split(".p.")[1]?.split(".")[0] ?? "";
  if (!productSegment) return "";
  return decodeBase64Url(productSegment) || productSegment;
}

export async function fetchAgentDevices(agentId: string): Promise<DeviceOption[]> {
  const productId = productIdFromAgentId(agentId);
  if (!productId) throw new Error("Cannot resolve product ID");

  const response = await fetch(`/api/products/${encodeURIComponent(productId)}/devices`, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) throw new Error(`Device API returned ${response.status}`);

  return parseDeviceOptions(await response.json());
}

export function bindingPrompt(prompt: string, workflowName: string, bindings: Array<{
  description: string;
  agentName: string;
  device: DeviceOption;
}>) {
  const lines = bindings.map(
    (binding) =>
      `- ${binding.description} via ${binding.agentName}: device_id=${binding.device.deviceId}, device_name=${binding.device.name}`,
  );

  return `${prompt}

Confirmed device bindings for workflow "${workflowName}":
${lines.join("\n")}

Use exactly these device_id values for the matching workflow steps. Do not choose other devices.`;
}

function parseJsonFragment(text: string): unknown {
  let normalized = text.trim();
  if (normalized.startsWith("```")) {
    normalized = normalized.replace(/^```[a-zA-Z0-9_-]*\s*/, "").replace(/\s*```$/, "").trim();
  }

  try {
    return JSON.parse(normalized);
  } catch {
    const match = normalized.match(/(\{[\s\S]*\}|\[[\s\S]*\])/);
    if (!match) return null;
    try {
      return JSON.parse(match[1]);
    } catch {
      return null;
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function rawDeviceList(value: unknown) {
  if (Array.isArray(value)) return value;
  if (isRecord(value) && Array.isArray(value.devices)) return value.devices;
  return [];
}

function parseDeviceOption(device: Record<string, unknown>): DeviceOption | null {
  const deviceId = String(device.device_id ?? device.id ?? "").trim();
  if (!deviceId) return null;

  const option: DeviceOption = {
    deviceId,
    name: deviceName(device, deviceId),
  };
  const online = deviceOnline(device);
  if (online !== undefined) option.online = online;
  if (isRecord(device.state)) option.state = device.state;

  return option;
}

function deviceName(device: Record<string, unknown>, fallback: string) {
  return typeof device.name === "string" && device.name.trim() ? device.name.trim() : fallback;
}

function deviceOnline(device: Record<string, unknown>) {
  if (typeof device.online === "boolean") return device.online;
  if (device.status === "online") return true;
  if (device.status === "offline") return false;
  return undefined;
}

function isDeviceOption(value: DeviceOption | null): value is DeviceOption {
  return value !== null;
}

function decodeBase64Url(value: string) {
  try {
    const normalized = value.replaceAll("-", "+").replaceAll("_", "/");
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
    const binary = atob(padded);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    return new TextDecoder().decode(bytes).trim();
  } catch {
    return "";
  }
}
