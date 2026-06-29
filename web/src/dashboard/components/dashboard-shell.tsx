import { useEffect } from "react";
import { Allotment, setSashSize } from "allotment";
import type { SkitterDashboardController } from "../use-skitter-dashboard";
import { AppHeader } from "./app-header";
import { DashboardSidebar } from "./dashboard-sidebar";
import { OrchestrationPanel } from "./orchestration-panel";
import { RegistryPanel } from "./registry-panel";

const DEFAULT_SPLIT_SIZES = [30, 70];
const DASHBOARD_SPLIT_SIZES_KEY = "skitter-dashboard-allotment-sizes-v2";

function readStoredSplitSizes() {
  if (typeof window === "undefined") return DEFAULT_SPLIT_SIZES;

  try {
    const raw = window.localStorage.getItem(DASHBOARD_SPLIT_SIZES_KEY);
    if (!raw) return DEFAULT_SPLIT_SIZES;

    const value: unknown = JSON.parse(raw);
    if (
      Array.isArray(value) &&
      value.length === 2 &&
      value.every((size) => typeof size === "number" && Number.isFinite(size) && size > 0)
    ) {
      return value;
    }
  } catch {
    window.localStorage.removeItem(DASHBOARD_SPLIT_SIZES_KEY);
  }

  return DEFAULT_SPLIT_SIZES;
}

function storeSplitSizes(sizes: number[]) {
  if (sizes.length !== 2 || sizes.some((size) => !Number.isFinite(size) || size <= 0)) {
    return;
  }

  window.localStorage.setItem(DASHBOARD_SPLIT_SIZES_KEY, JSON.stringify(sizes));
}

export function DashboardShell({ dashboard }: { dashboard: SkitterDashboardController }) {
  useEffect(() => {
    setSashSize(12);
  }, []);

  return (
    <div className="flex h-screen min-h-0 w-full bg-background">
      <DashboardSidebar
        org={dashboard.config.org}
        unit={dashboard.config.unit}
        activeView={dashboard.activeView}
        selectWorkflows={dashboard.selectWorkflows}
        selectScenes={dashboard.selectScenes}
      />

      <main className="relative flex min-w-0 flex-1 flex-col overflow-hidden bg-background">
        <AppHeader
          clientId={dashboard.clientId}
          config={dashboard.config}
          activeView={dashboard.activeView}
          connection={dashboard.connection}
          connectionError={dashboard.connectionError}
          connect={dashboard.connect}
          language={dashboard.language}
          setLanguage={dashboard.setLanguage}
          theme={dashboard.theme}
          resolvedTheme={dashboard.resolvedTheme}
          setTheme={dashboard.setTheme}
          settingsOpen={dashboard.settingsOpen}
          setSettingsOpen={dashboard.setSettingsOpen}
          draftConfig={dashboard.draftConfig}
          setDraftConfig={dashboard.setDraftConfig}
          applySettings={dashboard.applySettings}
        />

        <div className="min-h-0 w-full flex-1 overflow-hidden">
          <Allotment
            className="skitter-split-view"
            defaultSizes={readStoredSplitSizes()}
            proportionalLayout
            onDragEnd={storeSplitSizes}
          >
            <Allotment.Pane preferredSize="30%" minSize={190}>
              <div className="h-full min-w-0 overflow-hidden">
                <RegistryPanel
                  agents={dashboard.callableAgents}
                  apps={dashboard.appAgents}
                  taskAgents={dashboard.taskAgents}
                  activeView={dashboard.activeView}
                  activeSceneId={dashboard.activeSceneId}
                  selectScene={dashboard.selectScene}
                  agentDeviceBindings={dashboard.agentDeviceBindings}
                  updateAgentBindingDevice={dashboard.updateAgentBindingDevice}
                  deleteWorkflow={dashboard.deleteWorkflow}
                  createMockTaskAgent={dashboard.createMockTaskAgent}
                  deleteMockTaskAgent={dashboard.deleteMockTaskAgent}
                />
              </div>
            </Allotment.Pane>
            <Allotment.Pane minSize={360}>
              <div className="h-full min-w-0 overflow-hidden">
                <OrchestrationPanel
                  skitterAgent={dashboard.selectedAgent}
                  activeView={dashboard.activeView}
                  callableAgents={dashboard.callableAgents}
                  appAgents={dashboard.appAgents}
                  taskAgents={dashboard.taskAgents}
                  activeChat={dashboard.activeChat}
                  chatInput={dashboard.chatInput}
                  setChatInput={dashboard.setChatInput}
                  sendChat={dashboard.sendChat}
                  connection={dashboard.connection}
                />
              </div>
            </Allotment.Pane>
          </Allotment>
        </div>
      </main>
    </div>
  );
}
