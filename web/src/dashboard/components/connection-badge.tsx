import { CheckCircle2, CircleOff, Loader2, XCircle } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import type { BadgeProps } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { ConnectionState } from "../types";

type ConnectionBadgeItem = {
  labelKey: string;
  variant: NonNullable<BadgeProps["variant"]>;
  icon: LucideIcon;
};

const connectionBadges = {
  connected: { labelKey: "connection.connected", variant: "success", icon: CheckCircle2 },
  connecting: { labelKey: "connection.connecting", variant: "warning", icon: Loader2 },
  disconnected: { labelKey: "connection.disconnected", variant: "muted", icon: CircleOff },
  error: { labelKey: "connection.error", variant: "warning", icon: XCircle },
} satisfies Record<ConnectionState, ConnectionBadgeItem>;

export function ConnectionBadge({ state }: { state: ConnectionState }) {
  const { t } = useTranslation();
  const item = connectionBadges[state];
  const Icon = item.icon;

  return (
    <Badge variant={item.variant} className="h-7 gap-1.5 px-2.5">
      <Icon className={cn("size-3.5", state === "connecting" && "animate-spin")} />
      {t(item.labelKey)}
    </Badge>
  );
}
