import type * as React from "react";
import { cn } from "@/lib/utils";

const badgeBase = "inline-flex items-center rounded-[6px] border px-2 py-0.5 text-xs font-medium transition-colors";

const badgeVariants = {
  default: "border-transparent bg-primary text-primary-foreground",
  secondary: "border-transparent bg-secondary text-secondary-foreground",
  outline: "border-border text-foreground",
  success: "border-online-border bg-online-bg text-online-text",
  warning: "border-amber-500/25 bg-amber-500/10 text-amber-700 dark:text-amber-300",
  muted: "border-border bg-muted text-muted-foreground",
};

export interface BadgeProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: keyof typeof badgeVariants;
}

export function Badge({ className, variant = "default", ...props }: BadgeProps) {
  return <div className={cn(badgeBase, badgeVariants[variant], className)} {...props} />;
}
