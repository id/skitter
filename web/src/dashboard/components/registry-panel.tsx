import { Children, useMemo, useState } from "react";
import { Bot, LayoutList, Loader2, Plus, Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import type { ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import { displayName } from "../agent-utils";
import type { AgentDeviceBinding, AgentEntry } from "../types";
import { AgentGlyph } from "./agent-glyph";
import { WorkflowGraphViewer } from "./workflow-graph-viewer";

type AgentSortMode = "newest" | "oldest";

interface RegistryPanelProps {
  agents: AgentEntry[];
  apps: AgentEntry[];
  taskAgents: AgentEntry[];
  activeView: "workflows" | "scenes";
  activeSceneId: string;
  selectScene: (agentId: string) => void;
  agentDeviceBindings: Map<string, AgentDeviceBinding>;
  updateAgentBindingDevice: (agentId: string, deviceId: string) => void;
  deleteWorkflow: (workflowId: string) => Promise<void>;
  createMockTaskAgent: (draft: { name: string; description: string }) => void;
  deleteMockTaskAgent: (agentId: string) => void;
}

export function RegistryPanel({
  agents,
  apps,
  taskAgents,
  activeView,
  activeSceneId,
  selectScene,
  agentDeviceBindings,
  updateAgentBindingDevice,
  deleteWorkflow,
  createMockTaskAgent,
  deleteMockTaskAgent,
}: RegistryPanelProps) {
  const { t } = useTranslation();
  const [agentSortMode, setAgentSortMode] = useState<AgentSortMode>("newest");
  const sortedAgents = useMemo(
    () => sortDeviceAgents(agents, agentSortMode),
    [agents, agentSortMode],
  );

  return (
    <section className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-background">
      <ScrollArea className="skitter-registry-scroll min-h-0 w-full max-w-full flex-1">
        <div className="flex min-w-0 w-full max-w-full flex-col overflow-hidden px-4 pb-4 pt-4">
          {activeView === "workflows" ? (
            <CapabilitySection
              title={t("registry.savedWorkflows")}
              empty={t("registry.noWorkflows")}
            >
              {apps.map((app) => (
                <WorkflowRow
                  key={app.id}
                  app={app}
                  agents={agents}
                  deleteWorkflow={deleteWorkflow}
                />
              ))}
            </CapabilitySection>
          ) : (
            <CapabilitySection
              title={t("registry.taskAgents")}
              empty={t("registry.noScenes")}
              action={<CreateTaskAgentDialog agents={agents} createMockTaskAgent={createMockTaskAgent} />}
            >
              {taskAgents.map((scene) => (
                <SceneRow
                  key={scene.id}
                  scene={scene}
                  active={activeSceneId === scene.id}
                  onSelect={() => selectScene(scene.id)}
                  deleteMockTaskAgent={deleteMockTaskAgent}
                />
              ))}
            </CapabilitySection>
          )}

          <PanelDivider />

          <CapabilitySection
            title={t("registry.registeredDeviceAgents")}
            empty={t("registry.noOnlineAgents")}
            action={<AgentSortSelect value={agentSortMode} onChange={setAgentSortMode} />}
            contentClassName="gap-2"
          >
            {sortedAgents.map((agent) => (
              <AgentRow
                key={agent.id}
                agent={agent}
                binding={agentDeviceBindings.get(agent.id) ?? loadingAgentBinding(agent)}
                updateAgentBindingDevice={updateAgentBindingDevice}
              />
            ))}
          </CapabilitySection>
        </div>
      </ScrollArea>
    </section>
  );
}

function sortDeviceAgents(agents: AgentEntry[], mode: AgentSortMode) {
  return [...agents].sort((left, right) => {
    if (left.lastSeenAt && !right.lastSeenAt) return -1;
    if (!left.lastSeenAt && right.lastSeenAt) return 1;
    if (left.lastSeenAt && right.lastSeenAt && left.lastSeenAt !== right.lastSeenAt) {
      return mode === "newest"
        ? right.lastSeenAt - left.lastSeenAt
        : left.lastSeenAt - right.lastSeenAt;
    }

    const nameDiff = displayName(left).localeCompare(displayName(right), undefined, {
      sensitivity: "base",
      numeric: true,
    });
    return nameDiff;
  });
}

function AgentSortSelect({
  value,
  onChange,
}: {
  value: AgentSortMode;
  onChange: (mode: AgentSortMode) => void;
}) {
  const { t } = useTranslation();

  return (
    <Select value={value} onValueChange={(mode) => onChange(mode as AgentSortMode)}>
      <SelectTrigger
        size="sm"
        className="h-[30px] w-[7.5rem] min-w-0 px-2.5 !text-xs font-medium leading-4 [&_[data-slot=select-value]]:!text-xs [&_[data-slot=select-value]]:leading-4 [&_[data-slot=select-value]]:truncate [&_svg]:size-3.5"
        title={t("registry.sort")}
        aria-label={t("registry.sort")}
      >
        <SelectValue className="!text-xs leading-4" />
      </SelectTrigger>
      <SelectContent align="end">
        <SelectGroup>
          <SelectItem value="newest" className="!text-xs leading-4">
            {t("registry.sortNewest")}
          </SelectItem>
          <SelectItem value="oldest" className="!text-xs leading-4">
            {t("registry.sortOldest")}
          </SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}

function CapabilitySection({
  title,
  empty,
  action,
  contentClassName,
  children,
}: {
  title: string;
  empty: string;
  action?: ReactNode;
  contentClassName?: string;
  children: ReactNode;
}) {
  const hasChildren = Children.count(children) > 0;

  return (
    <div className="min-w-0 py-4">
      <div className="mb-3 flex h-8 items-center justify-between gap-2 px-2">
        <div className="min-w-0">
          <div className="min-w-0 truncate text-sm font-semibold leading-5 text-muted-foreground">
            {title}
          </div>
        </div>
        {action}
      </div>
      <div className={cn("flex min-w-0 max-w-full flex-col gap-2", contentClassName)}>
        {hasChildren ? (
          children
        ) : (
          <EmptyListState label={empty} />
        )}
      </div>
    </div>
  );
}

function EmptyListState({ label }: { label: string }) {
  return (
    <div
      className="mx-2 flex min-h-16 items-center justify-center rounded-[8px] border border-dashed border-border px-4 text-center text-sm leading-5 text-muted-foreground"
      role="status"
    >
      {label}
    </div>
  );
}

function SceneRow({
  scene,
  active,
  onSelect,
  deleteMockTaskAgent,
}: {
  scene: AgentEntry;
  active: boolean;
  onSelect: () => void;
  deleteMockTaskAgent: (agentId: string) => void;
}) {
  const { t } = useTranslation();
  const name = displayName(scene);
  const description = scene.card.description || scene.id;

  return (
    <div
      className={cn(
        "grid w-full min-w-0 max-w-full items-center gap-2 overflow-hidden rounded-[9px] transition-colors",
        scene.isMock ? "grid-cols-[minmax(0,1fr)_1.75rem] pr-2" : "grid-cols-1",
        active ? "bg-brand-500/[0.04] ring-1 ring-brand-500/10" : "hover:bg-muted/25",
      )}
      title={description}
    >
      <button
        type="button"
        onClick={onSelect}
        aria-current={active ? "true" : undefined}
        className="grid min-w-0 grid-cols-[2rem_minmax(0,1fr)] items-center gap-3 px-3 py-2.5 text-left"
      >
        <CapabilityRowContent
          icon={<Bot className="size-4" />}
          name={name}
          description={description}
        />
      </button>
      {scene.isMock ? (
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          className="size-7 shrink-0 opacity-75 hover:opacity-100"
          aria-label={t("mockTask.delete", { name })}
          onClick={() => deleteMockTaskAgent(scene.id)}
        >
          <Trash2 className="size-3.5" />
        </Button>
      ) : null}
    </div>
  );
}

function CreateTaskAgentDialog({
  agents,
  createMockTaskAgent,
}: {
  agents: AgentEntry[];
  createMockTaskAgent: (draft: { name: string; description: string }) => void;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const canCreate = Boolean(name.trim() && description.trim());

  function setDialogOpen(nextOpen: boolean) {
    setOpen(nextOpen);
    if (!nextOpen) {
      setName("");
      setDescription("");
    }
  }

  function submit() {
    if (!canCreate) return;
    const nextName = name.trim();
    createMockTaskAgent({ name: nextName, description: description.trim() });
    toast.success(t("mockTask.created", { name: nextName }));
    setDialogOpen(false);
  }

  return (
    <Dialog open={open} onOpenChange={setDialogOpen}>
      <DialogTrigger asChild>
        <Button
          variant="outline"
          size="icon-sm"
          title={t("mockTask.create")}
          aria-label={t("mockTask.create")}
        >
          <Plus className="size-4" />
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t("mockTask.title")}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          <label className="grid gap-2 text-sm font-medium">
            {t("mockTask.nameLabel")}
            <Input
              value={name}
              placeholder={t("mockTask.namePlaceholder")}
              onChange={(event) => setName(event.target.value)}
            />
          </label>
          <label className="grid gap-2 text-sm font-medium">
            {t("mockTask.descriptionLabel")}
            <Textarea
              value={description}
              placeholder={t("mockTask.descriptionPlaceholder")}
              onChange={(event) => setDescription(event.target.value)}
              rows={3}
              className="resize-none"
            />
          </label>
          <div className="border-t border-border pt-3">
            <div className="text-sm font-medium">{t("mockTask.devicesTitle")}</div>
            {agents.length ? (
              <div className="mt-3 flex flex-wrap gap-1.5">
                {agents.slice(0, 4).map((agent) => (
                  <Badge key={agent.id} variant="muted" className="max-w-full truncate">
                    {displayName(agent)}
                  </Badge>
                ))}
              </div>
            ) : (
              <div className="mt-2 text-xs text-muted-foreground">{t("mockTask.noDevices")}</div>
            )}
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setDialogOpen(false)}>
            {t("common.cancel")}
          </Button>
          <Button disabled={!canCreate} onClick={submit}>
            {t("mockTask.create")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function WorkflowRow({
  app,
  agents,
  deleteWorkflow,
}: {
  app: AgentEntry;
  agents: AgentEntry[];
  deleteWorkflow: (workflowId: string) => Promise<void>;
}) {
  const [deleting, setDeleting] = useState(false);
  const [open, setOpen] = useState(false);
  const { t } = useTranslation();
  const name = displayName(app);
  const description = app.card.description || app.id;

  async function remove() {
    if (deleting) return;

    setDeleting(true);
    try {
      await deleteWorkflow(app.id);
      toast.success(t("registry.deleted", { name }));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t("registry.failedDelete", { name }));
    } finally {
      setDeleting(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <div
        className={cn(
          "group grid w-full min-w-0 max-w-full grid-cols-[minmax(0,1fr)_1.75rem] items-center overflow-hidden rounded-[10px] border border-border/65 bg-background pr-2",
          "transition-colors hover:border-brand-500/20 hover:bg-muted/30",
        )}
        title={description}
      >
        <DialogTrigger asChild>
          <button
            type="button"
            className="grid min-w-0 grid-cols-[2rem_minmax(0,1fr)] items-center gap-3 px-3 py-2.5 text-left"
            aria-label={t("flow.open", { name })}
          >
            <CapabilityRowContent
              icon={<LayoutList className="size-4" />}
              name={name}
              description={description}
            />
          </button>
        </DialogTrigger>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          className="size-7 shrink-0 opacity-75 hover:opacity-100"
          disabled={deleting}
          onClick={remove}
          aria-label={t("registry.delete", { name })}
        >
          {deleting ? <Loader2 className="size-3.5 animate-spin" /> : <Trash2 className="size-3.5" />}
        </Button>
      </div>
      <DialogContent className="inset-0 left-0 top-0 h-screen w-screen max-w-none translate-x-0 translate-y-0 grid-rows-[auto_minmax(0,1fr)] gap-0 rounded-none border-0 p-0">
        <DialogHeader className="border-b border-border/70 px-6 py-4 pr-14">
          <DialogTitle className="min-w-0 truncate">{name}</DialogTitle>
          <DialogDescription className="line-clamp-2 max-w-4xl">{description}</DialogDescription>
        </DialogHeader>
        <div className="min-h-0 min-w-0">
          <WorkflowGraphViewer
            agents={agents}
            workflow={app}
            className="h-full rounded-none border-0"
          />
        </div>
      </DialogContent>
    </Dialog>
  );
}

function AgentRow({
  agent,
  binding,
  updateAgentBindingDevice,
}: {
  agent: AgentEntry;
  binding: AgentDeviceBinding;
  updateAgentBindingDevice: (agentId: string, deviceId: string) => void;
}) {
  const name = displayName(agent);
  const description = agent.card.description || agent.id;

  return (
    <div
      className="w-full min-w-0 max-w-full overflow-hidden rounded-[10px] border border-border/70 bg-background px-3.5 py-4 transition-colors hover:bg-muted/20"
      title={description}
    >
      <div className="flex min-w-0 items-center gap-2">
        <span className="mr-0.5 flex shrink-0 text-muted-foreground">
          <AgentGlyph agent={agent} className="size-5" />
        </span>
        <span className="min-w-0 truncate text-sm font-medium">
          {name}
        </span>
      </div>
      <div className="mt-2 min-w-0 truncate text-xs leading-5 text-muted-foreground">
        {description}
      </div>
      <div className="mt-4 block min-w-0 max-w-full overflow-hidden">
        <AgentDeviceSelect
          binding={binding}
          updateAgentBindingDevice={updateAgentBindingDevice}
        />
      </div>
    </div>
  );
}

function CapabilityRowContent({
  icon,
  name,
  description,
}: {
  icon: ReactNode;
  name: string;
  description: string;
}) {
  return (
    <>
      <span className="flex size-8 shrink-0 items-center justify-center text-muted-foreground">
        {icon}
      </span>
      <span className="block min-w-0 overflow-hidden">
        <span className="block min-w-0 truncate text-sm font-medium">{name}</span>
        <span className="block min-w-0 truncate text-xs leading-5 text-muted-foreground">
          {description}
        </span>
      </span>
    </>
  );
}

function PanelDivider() {
  return <div className="mx-2 my-4 h-px bg-border/70" />;
}

function AgentDeviceSelect({
  binding,
  updateAgentBindingDevice,
}: {
  binding: AgentDeviceBinding;
  updateAgentBindingDevice: (agentId: string, deviceId: string) => void;
}) {
  const { t } = useTranslation();
  const devices = binding.loading ? [] : binding.devices;
  const error = devices.length ? "" : binding.error;
  const placeholder = binding.loading ? t("registry.loadingDevices") : t("registry.selectDevice");

  return (
    <div className="relative min-w-0 max-w-full overflow-hidden">
      <Select
        value={binding.selectedDeviceId}
        disabled={binding.loading || devices.length === 0}
        onValueChange={(deviceId) => updateAgentBindingDevice(binding.agentId, deviceId)}
      >
        <SelectTrigger
          size="sm"
          className="min-w-0 w-full max-w-full overflow-hidden [&_[data-slot=select-value]]:min-w-0 [&_[data-slot=select-value]]:flex-1 [&_[data-slot=select-value]]:truncate"
          style={{ fontSize: 12, lineHeight: "18px" }}
          title={binding.selectedDeviceId || placeholder}
        >
          <SelectValue placeholder={placeholder} />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            {devices.map((device) => (
              <SelectItem
                key={device.deviceId}
                value={device.deviceId}
                className="max-w-full [&>span:last-child]:min-w-0 [&>span:last-child]:truncate"
                style={{ fontSize: 12, lineHeight: "18px" }}
              >
                {device.deviceId}
                {device.online === false ? ` ${t("registry.offline")}` : ""}
              </SelectItem>
            ))}
          </SelectGroup>
        </SelectContent>
      </Select>
      {binding.loading ? (
        <Loader2 className="pointer-events-none absolute right-7 top-1/2 size-3 -translate-y-1/2 animate-spin text-muted-foreground" />
      ) : null}
      {error ? <div className="truncate pt-1 text-xs text-destructive">{error}</div> : null}
    </div>
  );
}

function loadingAgentBinding(agent: AgentEntry): AgentDeviceBinding {
  return {
    agentId: agent.id,
    agentName: displayName(agent),
    devices: [],
    selectedDeviceId: "",
    loading: true,
  };
}
