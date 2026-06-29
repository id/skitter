import type { Dispatch, SetStateAction } from "react";
import { Cable } from "lucide-react";
import { useTranslation } from "react-i18next";
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
import type { DashboardConfig } from "../types";

interface SettingsDialogProps {
  open: boolean;
  setOpen: (open: boolean) => void;
  draftConfig: DashboardConfig;
  setDraftConfig: Dispatch<SetStateAction<DashboardConfig>>;
  applySettings: () => void;
  clientId: string;
}

export function SettingsDialog({
  open,
  setOpen,
  draftConfig,
  setDraftConfig,
  applySettings,
  clientId,
}: SettingsDialogProps) {
  const { t } = useTranslation();
  const brokerUrlValid = /^wss?:\/\/.+/i.test(draftConfig.brokerUrl.trim());

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="icon-sm" title={t("settings.trigger")}>
          <Cable className="size-4" />
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t("settings.title")}</DialogTitle>
          <DialogDescription>{t("settings.description")}</DialogDescription>
        </DialogHeader>
        <div className="grid gap-4">
          <label className="grid gap-2 text-sm font-medium">
            {t("settings.brokerUrl")}
            <Input
              value={draftConfig.brokerUrl}
              onChange={(event) =>
                setDraftConfig((current) => ({ ...current, brokerUrl: event.target.value }))
              }
            />
            {!brokerUrlValid && (
              <span className="text-xs font-normal text-destructive">
                {t("settings.brokerUrlInvalid")}
              </span>
            )}
          </label>
          <div className="grid grid-cols-2 gap-3">
            <label className="grid gap-2 text-sm font-medium">
              {t("settings.org")}
              <Input
                value={draftConfig.org}
                onChange={(event) => setDraftConfig((current) => ({ ...current, org: event.target.value }))}
              />
            </label>
            <label className="grid gap-2 text-sm font-medium">
              {t("settings.unit")}
              <Input
                value={draftConfig.unit}
                onChange={(event) => setDraftConfig((current) => ({ ...current, unit: event.target.value }))}
              />
            </label>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <label className="grid gap-2 text-sm font-medium">
              {t("settings.username")}
              <Input
                value={draftConfig.username}
                onChange={(event) =>
                  setDraftConfig((current) => ({ ...current, username: event.target.value }))
                }
              />
            </label>
            <label className="grid gap-2 text-sm font-medium">
              {t("settings.password")}
              <Input
                type="password"
                value={draftConfig.password}
                onChange={(event) =>
                  setDraftConfig((current) => ({ ...current, password: event.target.value }))
                }
              />
            </label>
          </div>
          <div className="rounded-[8px] border border-border bg-muted/50 p-3">
            <div className="text-sm font-medium">{t("settings.clientId")}</div>
            <div className="mt-1 font-mono text-xs text-muted-foreground">{clientId}</div>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            {t("common.cancel")}
          </Button>
          <Button onClick={applySettings} disabled={!brokerUrlValid}>
            {t("common.save")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
