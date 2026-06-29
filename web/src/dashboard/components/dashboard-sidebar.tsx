import { Bot, LayoutList } from "lucide-react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";

interface DashboardSidebarProps {
  org: string;
  unit: string;
  activeView: "workflows" | "scenes";
  selectWorkflows: () => void;
  selectScenes: () => void;
}

export function DashboardSidebar({
  org,
  unit,
  activeView,
  selectWorkflows,
  selectScenes,
}: DashboardSidebarProps) {
  const { t } = useTranslation();
  const baseUrl = import.meta.env.BASE_URL.endsWith("/")
    ? import.meta.env.BASE_URL
    : `${import.meta.env.BASE_URL}/`;

  return (
    <aside className="relative flex h-full w-16 shrink-0 flex-col border-r border-border/60 bg-background text-foreground">
      <header className="flex h-16 items-center justify-center">
        <button
          type="button"
          title={`EMQX ${org}/${unit}`}
          aria-label={`EMQX ${org}/${unit}`}
          className="flex size-12 items-center justify-center rounded-none outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring"
        >
          <img src={`${baseUrl}emqx.svg`} alt="" className="size-7 object-contain" />
        </button>
      </header>

      <nav className="flex min-h-0 flex-1 flex-col items-center gap-2 overflow-auto px-2 py-3">
        <SidebarNavButton
          active={activeView === "workflows"}
          label={t("common.workflows")}
          onClick={selectWorkflows}
        >
          <LayoutList className="size-5" />
        </SidebarNavButton>
        <SidebarNavButton
          active={activeView === "scenes"}
          label={t("common.scenes")}
          onClick={selectScenes}
        >
          <Bot className="size-5" />
        </SidebarNavButton>
      </nav>
    </aside>
  );
}

function SidebarNavButton({
  active,
  children,
  label,
  onClick,
}: {
  active: boolean;
  children: ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      title={label}
      aria-label={label}
      data-active={active}
      onClick={onClick}
      className={cn(
        "flex size-12 items-center justify-center rounded-[13px] text-muted-foreground outline-none transition-colors hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-ring",
        active && "bg-brand-500/10 text-brand-500",
      )}
    >
      {children}
    </button>
  );
}
