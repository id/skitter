import { Check, Laptop, Moon, RefreshCw, SlidersHorizontal, Sun } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import type { DashboardConfig, DashboardLanguage, ResolvedTheme, ThemeMode } from "../types";
import { ConnectionBadge } from "./connection-badge";
import { SettingsDialog } from "./settings-dialog";
import type { SkitterDashboardController } from "../use-skitter-dashboard";

interface AppHeaderProps {
  clientId: string;
  config: DashboardConfig;
  activeView: "workflows" | "scenes";
  connection: SkitterDashboardController["connection"];
  connectionError: string;
  connect: () => void;
  language: SkitterDashboardController["language"];
  setLanguage: SkitterDashboardController["setLanguage"];
  theme: SkitterDashboardController["theme"];
  resolvedTheme: SkitterDashboardController["resolvedTheme"];
  setTheme: SkitterDashboardController["setTheme"];
  settingsOpen: boolean;
  setSettingsOpen: (open: boolean) => void;
  draftConfig: SkitterDashboardController["draftConfig"];
  setDraftConfig: SkitterDashboardController["setDraftConfig"];
  applySettings: () => void;
}

export function AppHeader({
  clientId,
  config,
  activeView,
  connection,
  connectionError,
  connect,
  language,
  setLanguage,
  theme,
  resolvedTheme,
  setTheme,
  settingsOpen,
  setSettingsOpen,
  draftConfig,
  setDraftConfig,
  applySettings,
}: AppHeaderProps) {
  const { t } = useTranslation();
  const title = activeView === "scenes" ? t("common.scenes") : t("common.workflows");

  return (
    <header className="border-b border-border/60 bg-background/92 backdrop-blur">
      <div className="flex h-14 w-full items-center justify-between gap-3 px-4">
        <div className="flex min-w-0 items-center gap-3">
          <div className="min-w-0">
            <div className="truncate text-lg font-semibold leading-none tracking-tight">{title}</div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Badge variant="outline" className="hidden h-7 gap-1.5 font-mono text-[11px] text-muted-foreground md:inline-flex">
            Scope {config.org}/{config.unit}
          </Badge>
          <ConnectionBadge state={connection} />
          {connectionError ? (
            <span className="max-w-64 truncate rounded-[6px] border border-destructive/20 bg-destructive/10 px-2 py-1 text-xs text-destructive">
              {connectionError}
            </span>
          ) : null}
          <Button variant="outline" size="icon-sm" title={t("header.reconnect")} onClick={connect}>
            <RefreshCw className="size-4" />
          </Button>
          <PreferencesMenu
            language={language}
            setLanguage={setLanguage}
            theme={theme}
            resolvedTheme={resolvedTheme}
            setTheme={setTheme}
          />
          <SettingsDialog
            open={settingsOpen}
            setOpen={setSettingsOpen}
            draftConfig={draftConfig}
            setDraftConfig={setDraftConfig}
            applySettings={applySettings}
            clientId={clientId}
          />
        </div>
      </div>
    </header>
  );
}

interface PreferencesMenuProps {
  language: DashboardLanguage;
  setLanguage: (language: DashboardLanguage) => void;
  theme: ThemeMode;
  resolvedTheme: ResolvedTheme;
  setTheme: (theme: ThemeMode) => void;
}

function PreferencesMenu({
  language,
  setLanguage,
  theme,
  resolvedTheme,
  setTheme,
}: PreferencesMenuProps) {
  const { t } = useTranslation();
  const themeLabel =
    theme === "system"
      ? `${t("theme.system")} (${t(`theme.${resolvedTheme}`)})`
      : t(`theme.${theme}`);

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="outline"
          size="icon-sm"
          title={`${t("preferences.trigger")}: ${t("preferences.language")} / ${themeLabel}`}
        >
          <SlidersHorizontal className="size-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-44 p-1.5">
        <DropdownMenuGroup>
          <DropdownMenuLabel className="px-2 py-1 text-[11px] font-medium text-muted-foreground">
            {t("preferences.language")}
          </DropdownMenuLabel>
          <DropdownMenuItem
            className={menuItemClass(language === "en")}
            onSelect={() => setLanguage("en")}
          >
            <span>{t("preferences.english")}</span>
            <Check className={checkClass(language === "en")} />
          </DropdownMenuItem>
          <DropdownMenuItem
            className={menuItemClass(language === "zh")}
            onSelect={() => setLanguage("zh")}
          >
            <span>{t("preferences.chinese")}</span>
            <Check className={checkClass(language === "zh")} />
          </DropdownMenuItem>
        </DropdownMenuGroup>
        <DropdownMenuSeparator className="my-1.5" />
        <DropdownMenuGroup>
          <DropdownMenuLabel className="px-2 py-1 text-[11px] font-medium text-muted-foreground">
            {t("preferences.theme")}
          </DropdownMenuLabel>
          <DropdownMenuItem
            className={menuItemClass(theme === "system")}
            onSelect={() => setTheme("system")}
          >
            <Laptop className="size-4" />
            <span>{t("theme.system")}</span>
            <Check className={checkClass(theme === "system")} />
          </DropdownMenuItem>
          <DropdownMenuItem
            className={menuItemClass(theme === "light")}
            onSelect={() => setTheme("light")}
          >
            <Sun className="size-4" />
            <span>{t("theme.light")}</span>
            <Check className={checkClass(theme === "light")} />
          </DropdownMenuItem>
          <DropdownMenuItem
            className={menuItemClass(theme === "dark")}
            onSelect={() => setTheme("dark")}
          >
            <Moon className="size-4" />
            <span>{t("theme.dark")}</span>
            <Check className={checkClass(theme === "dark")} />
          </DropdownMenuItem>
        </DropdownMenuGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function menuItemClass(active: boolean) {
  return cn("h-8 rounded-[6px] px-2 text-sm", active && "font-medium text-foreground");
}

function checkClass(active: boolean) {
  return cn("ml-auto size-4", active ? "opacity-100" : "opacity-0");
}
